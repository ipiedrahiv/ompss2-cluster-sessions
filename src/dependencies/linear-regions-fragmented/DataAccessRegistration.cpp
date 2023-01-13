/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <cassert>
#include <deque>
#include <iostream>
#include <mutex>
#include <algorithm>

#include "BottomMapEntry.hpp"
#include "CPUDependencyData.hpp"
#include "CommutativeScoreboard.hpp"
#include "DataAccess.hpp"
#include "DataAccessRegistration.hpp"
#include "ReductionInfo.hpp"
#include "TaskDataAccesses.hpp"
#include "executors/threads/TaskFinalization.hpp"
#include "executors/threads/ThreadManager.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "hardware/places/ComputePlace.hpp"
#include "memory/directory/Directory.hpp"
#include "scheduling/Scheduler.hpp"
#include "support/Containers.hpp"
#include "tasks/Task.hpp"

#include <ClusterManager.hpp>
#include <ExecutionWorkflow.hpp>
#include <InstrumentComputePlaceId.hpp>
#include <InstrumentDependenciesByAccess.hpp>
#include <InstrumentDependenciesByAccessLinks.hpp>
#include <InstrumentLogMessage.hpp>
#include <InstrumentReductions.hpp>
#include <InstrumentTaskId.hpp>
#include <InstrumentDependencySubsystemEntryPoints.hpp>
#include <InstrumentDebug.hpp>
#include <ObjectAllocator.hpp>

#include "cluster/ClusterUtil.hpp"

#ifdef USE_CLUSTER
#include "ClusterTaskContext.hpp"
#include <ClusterUtil.hpp>
#include "cluster/NodeNamespace.hpp"
#endif

#pragma GCC visibility push(hidden)

const char *dataAccessTypeNames[] = {
	"none", "read", "write", "readwrite", "concurrent", "commutative", "reduction", "auto"};

namespace DataAccessRegistration {

	/*
	 * Debugging function to print out the accesses and fragments for a task
	 *
	 * Prints desc, plus task name, then the accesses and fragments
	 */

#ifdef USE_CLUSTER
	__attribute__((unused))
	static void printTaskAccessesAndFragments(const char *desc, Task *task)
	{
		clusterCout << desc << task->getLabel() << "\n";

		assert(task != nullptr);
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());

		// Take lock on access structures if not already done
// #ifndef NDEBUG
// 		TaskDataAccesses::spinlock_t *lock = accessStructures._lock.isLockedByThisThread() ?
// 								nullptr : &accessStructures._lock;
// #else
		TaskDataAccesses::spinlock_t *lock = nullptr;
// #endif

		if (lock)
			lock->lock();

		/*
		 * Print all the task accesses. The task accesses correspond to the
		 * regions accessed by the task and its subtasks, and is essentially
		 * what is "visible" to the parent. The task accesses are fragmented,
		 * if necessary, because of sibling task accesses. This is done by
		 * registerTaskDataAccess. There are some circumstances when the task
		 * accesses are fragmented later, for example due to early release of
		 * dependencies.
		 */
		accessStructures._accesses.processAll(
			/* processor: called for each task access */
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *access = &(*position);
				assert(access != nullptr);
				std::cout << "access: (DataAccess *)" << access << ": "
					<< access->getAccessRegion().getStartAddress() << ":"
					<< access->getAccessRegion().getSize()
					<< " rw: " << access->readSatisfied() << access->writeSatisfied()
					<< " loc: " << access->getMemoryPlaceNodeIndex()
					<< " orig: " << access->getMemoryPlaceNodeIndex(access->getConcurrentInitialLocation())
					<< "\n";
				return true; /* always continue, don't stop here */
			}
		);

		/*
		 * Print all the task fragments. The task fragments correspond to
		 * regions accessed by child tasks, so is essentially what is "visible"
		 * to the children. A task starts with no fragments.  As child tasks
		 * are submitted, they will create fragments to cover their accesses
		 * (if not already covered by previous sibling tasks), and the (parent)
		 * task's fragments will be fragmented as necessary.
		 */
		accessStructures._accessFragments.processAll(
			/* processor: called for each task access fragment */
			[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
				DataAccess *fragment = &(*position);
				assert(fragment != nullptr);
				std::cout << "fragment: (DataAccess *)" << fragment << ": "
					<< fragment->getAccessRegion().getStartAddress() << ":"
					<< fragment->getAccessRegion().getSize()
					<< " rw: " << fragment->readSatisfied() << fragment->writeSatisfied()
					<< " loc: " << fragment->getMemoryPlaceNodeIndex()
					<< "\n";
				return true; /* always continue, don't stop here */
			}
		);

		/*
		 * Print all the taskwait fragments.
		 */
		accessStructures._taskwaitFragments.processAll(
			/* processor: called for each task access fragment */
			[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
				DataAccess *taskwaitFragment = &(*position);
				assert(taskwaitFragment != nullptr);
				std::cout << "taskwaitFragment: (DataAccess *)" << taskwaitFragment << ": "
					<< taskwaitFragment->getAccessRegion().getStartAddress() << ":"
					<< taskwaitFragment->getAccessRegion().getSize()
					<< " rw: " << taskwaitFragment->readSatisfied() << taskwaitFragment->writeSatisfied()
					<< " loc: " << taskwaitFragment->getMemoryPlaceNodeIndex()
					<< "\n";
				return true; /* always continue, don't stop here */
			}
		);

		/*
		 * Print all the bottom map entries.
		 */
		accessStructures._subaccessBottomMap.processAll(
			/* processor: called for each bottom map entry */
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);

				DataAccessLink previous = bottomMapEntry->_link;
				DataAccessRegion region = bottomMapEntry->_region;
				std::cout << "bottom map: region: " << region << " for task ";
				if (previous._task) {
					std::cout << previous._task->getLabel();
				}
				std::cout << "\n";

				/* Always continue with the rest of the bottom map */
				return true;
			}
		);

		// Release lock if not already done by the caller
		if (lock)
			lock->unlock();
	}
#endif //USE_CLUSTER

	typedef CPUDependencyData::removable_task_list_t removable_task_list_t;


	typedef CPUDependencyData::UpdateOperation UpdateOperation;


	struct DataAccessStatusEffects {
		bool _isRegistered: 1 ;
		bool _isSatisfied: 1 ;
		bool _enforcesDependency: 1 ;

		bool _hasNext: 1 ;
		bool _propagatesReadSatisfiabilityToNext: 1 ;
		bool _propagatesWriteSatisfiabilityToNext: 1 ;
		bool _propagatesConcurrentSatisfiabilityToNext: 1 ;
		bool _propagatesCommutativeSatisfiabilityToNext: 1 ;
		bool _propagatesReductionInfoToNext: 1 ;
		bool _propagatesReductionSlotSetToNext: 1 ;
		bool _releasesCommutativeRegion: 1 ;

		bool _propagatesReadSatisfiabilityToFragments: 1 ;
		bool _propagatesWriteSatisfiabilityToFragments: 1 ;
		bool _propagatesConcurrentSatisfiabilityToFragments: 1 ;
		bool _propagatesCommutativeSatisfiabilityToFragments: 1 ;
		bool _propagatesReductionInfoToFragments: 1 ;
		bool _propagatesReductionSlotSetToFragments: 1 ;

		bool _makesReductionOriginalStorageAvailable: 1 ;
		bool _combinesReductionToPrivateStorage: 1 ;
		bool _combinesReductionToOriginal: 1 ;

		bool _linksBottomMapAccessesToNextAndInhibitsPropagation: 1 ;

		bool _isRemovable: 1 ;

		bool _triggersTaskwaitWorkflow: 1 ;

		bool _triggersDataRelease: 1 ;
		bool _triggersDataLinkRead: 1 ;
		bool _triggersDataLinkWrite: 1 ;
		bool _triggersDataLinkConcurrent: 1 ;

		bool _allowNamespacePropagation : 1;

	public:
		DataAccessStatusEffects() :
			_isRegistered(false),
			_isSatisfied(false),
			_enforcesDependency(false),

			_hasNext(false),
			_propagatesReadSatisfiabilityToNext(false),
			_propagatesWriteSatisfiabilityToNext(false),
			_propagatesConcurrentSatisfiabilityToNext(false),
			_propagatesCommutativeSatisfiabilityToNext(false),
			_propagatesReductionInfoToNext(false),
			_propagatesReductionSlotSetToNext(false),
			_releasesCommutativeRegion(false),

			_propagatesReadSatisfiabilityToFragments(false),
			_propagatesWriteSatisfiabilityToFragments(false),
			_propagatesConcurrentSatisfiabilityToFragments(false),
			_propagatesCommutativeSatisfiabilityToFragments(false),
			_propagatesReductionInfoToFragments(false),
			_propagatesReductionSlotSetToFragments(false),

			_makesReductionOriginalStorageAvailable(false),
			_combinesReductionToPrivateStorage(false),
			_combinesReductionToOriginal(false),

			_linksBottomMapAccessesToNextAndInhibitsPropagation(false),

			_isRemovable(false),

			_triggersTaskwaitWorkflow(false),
			_triggersDataRelease(false),
			_triggersDataLinkRead(false),
			_triggersDataLinkWrite(false),
			_triggersDataLinkConcurrent(false),
			_allowNamespacePropagation(true)
		{
		}

		DataAccessStatusEffects(DataAccess const *access)
		{
			_isRegistered = access->isRegistered();

			_isSatisfied = access->satisfied();
			_enforcesDependency =
				!access->isWeak() && !_isSatisfied &&
				// Reduction accesses can begin as soon as they have a ReductionInfo (even without SlotSet)
				!((access->getType() == REDUCTION_ACCESS_TYPE) && (access->receivedReductionInfo() || access->allocatedReductionInfo())) && (access->getObjectType() == access_type);
			_hasNext = access->hasNext();

			// Propagation to fragments
			if (access->hasSubaccesses()) {
				_propagatesReadSatisfiabilityToFragments = access->readSatisfied();
				_propagatesWriteSatisfiabilityToFragments = access->writeSatisfied();
				_propagatesConcurrentSatisfiabilityToFragments =
						access->concurrentSatisfied() && access->satisfied();
				_propagatesCommutativeSatisfiabilityToFragments =
						access->commutativeSatisfied() && access->satisfied();
				// If an access allocates a ReductionInfo, its fragments will have the ReductionInfo
				// set as soon as they are created (being created as a copy of the parent access)
				// For this, this trigger is used to propagate to the fragments the information of
				// *having received* (not having allocated) a ReductionInfo, as this is what is actually
				// tracked in the fragment's 'receivedReductionInfo' status bit
				_propagatesReductionInfoToFragments = access->receivedReductionInfo();
				// Non-reduction accesses will propagate received ReductionSlotSet to their fragments
				// to make their status consistent with the access itself
				_propagatesReductionSlotSetToFragments = access->receivedReductionSlotSet();
			} else {
				_propagatesReadSatisfiabilityToFragments = false;
				_propagatesWriteSatisfiabilityToFragments = false;
				_propagatesConcurrentSatisfiabilityToFragments = false;
				_propagatesCommutativeSatisfiabilityToFragments = false;
				_propagatesReductionInfoToFragments = false;
				_propagatesReductionSlotSetToFragments = false;
			}

			// Propagation to next
			if (_hasNext) {

				assert(access->getObjectType() != taskwait_type);

				// This can happen now if a remote task has a successor in the namespace
				// assert(access->getObjectType() != top_level_sink_type);

				if (access->hasSubaccesses()) {
					assert(access->getObjectType() == access_type);
					_propagatesReadSatisfiabilityToNext =
						access->canPropagateReadSatisfiability() && access->readSatisfied()
						&& ((access->getType() == READ_ACCESS_TYPE) || (access->getType() == NO_ACCESS_TYPE));
					_propagatesWriteSatisfiabilityToNext = false; // Write satisfiability is propagated through the fragments
					_propagatesConcurrentSatisfiabilityToNext =
						access->canPropagateConcurrentSatisfiability() && access->concurrentSatisfied()
						&& (access->getType() == CONCURRENT_ACCESS_TYPE);
					_propagatesCommutativeSatisfiabilityToNext =
						access->canPropagateCommutativeSatisfiability() && access->commutativeSatisfied()
						&& (access->getType() == COMMUTATIVE_ACCESS_TYPE);
					_propagatesReductionInfoToNext =
						access->canPropagateReductionInfo()
						&& (access->receivedReductionInfo() || access->allocatedReductionInfo())
						// For 'write' and 'readwrite' accesses we need to propagate the ReductionInfo through fragments only,
						// in order to be able to propagate a nested reduction ReductionInfo outside
						&& ((access->getType() != WRITE_ACCESS_TYPE) && (access->getType() != READWRITE_ACCESS_TYPE)
							&& (access->getType() != AUTO_ACCESS_TYPE));
					_propagatesReductionSlotSetToNext = false; // ReductionSlotSet is propagated through the fragments
					// Occasionally data release step needs to be propagated here
				} else if (
					(access->getObjectType() == fragment_type)
					|| (access->getObjectType() == taskwait_type)
					|| (access->getObjectType() == top_level_sink_type)) {
					_propagatesReadSatisfiabilityToNext =
						access->canPropagateReadSatisfiability()
						&& access->readSatisfied();
					_propagatesWriteSatisfiabilityToNext = access->writeSatisfied() && !access->getNamespaceNextIsIn();
					_propagatesConcurrentSatisfiabilityToNext =
						access->canPropagateConcurrentSatisfiability()
						&& !access->getNamespaceNextIsIn()
						&& access->concurrentSatisfied();
					_propagatesCommutativeSatisfiabilityToNext =
						access->canPropagateCommutativeSatisfiability()
						&& !access->getNamespaceNextIsIn()
						&& access->commutativeSatisfied();
					_propagatesReductionInfoToNext =
						access->canPropagateReductionInfo()
						&& (access->receivedReductionInfo() || access->allocatedReductionInfo());
					_propagatesReductionSlotSetToNext =
						(access->getType() == REDUCTION_ACCESS_TYPE)
						&& access->complete()
						&& access->receivedReductionInfo()
						&& !access->closesReduction()
						&& (access->allocatedReductionInfo()
								|| access->receivedReductionSlotSet());
				} else {
					assert(access->getObjectType() == access_type);
					assert(!access->hasSubaccesses());

					// A regular access without subaccesses but with a next
					bool propagateAllMemory = (access->getType() == AUTO_ACCESS_TYPE) && access->getDisableEagerSend() && ClusterManager::autoOptimizeNonAccessed();
					_propagatesReadSatisfiabilityToNext =
						access->canPropagateReadSatisfiability()
						&& access->readSatisfied()
						// Note: 'satisfied' as opposed to 'readSatisfied', because otherwise read
						// satisfiability could be propagated before reductions are combined
						&& (propagateAllMemory
							|| access->isAutoReadOnly() ||
							( _isSatisfied &&
							 ((access->getType() == READ_ACCESS_TYPE) || (access->getType() == NO_ACCESS_TYPE) || access->complete())
							));
					_propagatesWriteSatisfiabilityToNext =
						access->writeSatisfied()
						&& !access->getNamespaceNextIsIn()
						&& (propagateAllMemory
							|| (access->complete()
								// Note: This is important for not propagating write
								// satisfiability before reductions are combined
								&& _isSatisfied));

					_propagatesConcurrentSatisfiabilityToNext =
						access->canPropagateConcurrentSatisfiability()
						&& access->concurrentSatisfied()
						// Note: If a reduction is to be combined, being the (reduction) access 'satisfied'
						// and 'complete' should allow it to be done before propagating this satisfiability
						&& _isSatisfied
						&& !access->getNamespaceNextIsIn()
						&& (propagateAllMemory
							|| (access->getType() == CONCURRENT_ACCESS_TYPE || access->complete()));
					_propagatesCommutativeSatisfiabilityToNext =
						access->canPropagateCommutativeSatisfiability()
						&& access->commutativeSatisfied()
						&& !access->getNamespaceNextIsIn()
						&& (propagateAllMemory
							|| ((access->getType() == COMMUTATIVE_ACCESS_TYPE) || access->complete()));
					_propagatesReductionInfoToNext =
						access->canPropagateReductionInfo()
						&& (access->receivedReductionInfo() || access->allocatedReductionInfo())
						// For 'write' and 'readwrite' accesses we need to propagate the ReductionInfo to next only when
						// complete, otherwise subaccesses can still appear
						&& (propagateAllMemory ||
							(((access->getType() != WRITE_ACCESS_TYPE) && (access->getType() != READWRITE_ACCESS_TYPE)
							&& (access->getType() != AUTO_ACCESS_TYPE))
							|| access->complete()));
					_propagatesReductionSlotSetToNext =
						(access->getType() == REDUCTION_ACCESS_TYPE)
						&& (propagateAllMemory || access->complete())
							// && !access->getNamespaceNextIsIn()
							&& !access->closesReduction()
							&& (access->allocatedReductionInfo()
								|| access->receivedReductionSlotSet());
				}
			} else {
				assert(!access->hasNext());
				_propagatesReadSatisfiabilityToNext = false;
				_propagatesWriteSatisfiabilityToNext = false;
				_propagatesConcurrentSatisfiabilityToNext = false;
				_propagatesCommutativeSatisfiabilityToNext = false;
				_propagatesReductionInfoToNext = false;
				_propagatesReductionSlotSetToNext = false;
			}

			_makesReductionOriginalStorageAvailable =
				access->getObjectType() == access_type
				&& access->allocatedReductionInfo()
				&& access->writeSatisfied();

			_combinesReductionToPrivateStorage =
				access->closesReduction()
				// If there are subaccesses, it's the last subaccess that should combine
				&& !access->hasSubaccesses()
				// Having received 'ReductionSlotSet' implies that previously inserted reduction accesses
				// (forming part of the same reduction) are completed, but access' predecessors are
				// not necessarily so
				&& (access->allocatedReductionInfo()
					|| access->receivedReductionSlotSet())
				&& access->complete();

			_combinesReductionToOriginal =
				_combinesReductionToPrivateStorage
				// Being satisfied implies all predecessors (reduction or not) have been completed
				&& _isSatisfied;

			_triggersDataRelease = false;
			if (access->getOriginator()->hasDataReleaseStep()) {
				ExecutionWorkflow::DataReleaseStep const * const releaseStep =
					access->getOriginator()->getDataReleaseStep();

				_triggersDataRelease = releaseStep->checkDataRelease(access);
			}

			_isRemovable = access->propagatedInRemoteNamespace()
				|| (access->readSatisfied()
					&& access->writeSatisfied()
					&& access->receivedReductionInfo()
					// Read as: If this (reduction) access is part of its predecessor reduction,
					// it needs to have received the 'ReductionSlotSet' before being removed
					&& ((access->getType() != REDUCTION_ACCESS_TYPE)
						|| access->allocatedReductionInfo()
						|| access->receivedReductionSlotSet())
					&& access->complete()
					&& (!access->isInBottomMap()
						|| (access->hasNext() && !access->getNamespaceNextIsIn())
						|| (access->getType() == NO_ACCESS_TYPE)
						|| (access->getObjectType() == taskwait_type)
						|| (access->getObjectType() == top_level_sink_type)));


			/* Also must have already received the namespace information from previous access */
			_isRemovable = _isRemovable && ((access->getObjectType() != access_type)
											|| (access->getValidNamespacePrevious() != VALID_NAMESPACE_UNKNOWN));

			/*
			 * If the access is a taskwait access (from createTaskwait)
			 * Note: taskwait noflush has no output location, but still uses the
			 * notification via the workflow.
			 */
			_triggersTaskwaitWorkflow = (access->getObjectType() == taskwait_type)
										&& access->readSatisfied()
										&& access->writeSatisfied();

			_triggersDataLinkRead = access->hasDataLinkStep()
									&& access->readSatisfied();

			_triggersDataLinkWrite = access->hasDataLinkStep()
									 && access->writeSatisfied();

			_triggersDataLinkConcurrent = access->hasDataLinkStep()
									      && access->concurrentSatisfied();

			_releasesCommutativeRegion =
				(access->getType() == COMMUTATIVE_ACCESS_TYPE)
				&& !access->isWeak()
				&& access->complete();

			// NOTE: Calculate inhibition from initial status
			_linksBottomMapAccessesToNextAndInhibitsPropagation =
				access->hasNext() && access->complete() && access->hasSubaccesses();

			// By default allow propagation of the namespace information to the
			// next access
			_allowNamespacePropagation = true;
		}

		void setEnforcesDependency()
		{
			assert(_enforcesDependency == false);
			_enforcesDependency = true;
		}
	};

	static void unfragmentTaskAccesses(
		Task *task,
		TaskDataAccesses &accessStructures,
		bool enforceSameNamespacePrevious
	) {

		if (accessStructures._accesses.size() <= 1) {
			return;
		}

		// assert(accessStructures._lock.isLockedByThisThread());
		DataAccess *lastAccess = nullptr;
		accessStructures._accesses.processAllWithErase(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *access = &(*position);
				assert(access != nullptr);
				assert(access->getOriginator() == task);
				assert(access->isRegistered());

				if (access->canMergeWith(lastAccess, enforceSameNamespacePrevious)) {
					DataAccessRegion newrel(
						lastAccess->getAccessRegion().getStartAddress(),
						access->getAccessRegion().getEndAddress()
					);
					lastAccess->setAccessRegion(newrel);

					if (access->getWriteID() != lastAccess->getWriteID()) {
						lastAccess->setNewWriteID();
						if (lastAccess->getLocation()->isClusterLocalMemoryPlace()) {
							WriteIDManager::registerWriteIDasLocal(lastAccess->getWriteID(), newrel);
						}
					}

					DataAccessStatusEffects initialStatus(lastAccess);
					if (initialStatus._isRemovable) {
						// enforceSameNamespacePrevious is not true during REGISTERING the task.
						// and during REGISTERING _isRemovable is always true.
						assert(!enforceSameNamespacePrevious);
					} else {
						__attribute__((unused)) const int removalBlockers
							= accessStructures._removalBlockers.fetch_sub(1) - 1;
						assert(removalBlockers > 0);
					}

					if (initialStatus._enforcesDependency) {
						// This region is reached only when we unfragment after linking
						// dependencies.
						assert(enforceSameNamespacePrevious);
						__attribute__((unused)) const bool dec = task->decreasePredecessors();
						assert(!dec);
					}
					/* true: erase the second region */
					return true;
				}
				lastAccess = access;
				/* false: do not erase this region */
				return false;
			}
		);
	}

	static void unfragmentTaskwaits(TaskDataAccesses &accessStructures)
	{
		DataAccess *lastAccess = nullptr;
		bool lastAccessNeedsNewWriteId = false;

		accessStructures._taskwaitFragments.processAllWithErase(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *access = &(*position);
				assert(access != nullptr);

				if (access->canMergeWith(lastAccess, true)) {
					/* Combine two contiguous regions into one */
					DataAccessRegion newrel(
						lastAccess->getAccessRegion().getStartAddress(),
						access->getAccessRegion().getEndAddress()
					);
					lastAccess->setAccessRegion(newrel);

					if (access->getWriteID() != lastAccess->getWriteID()) {
						// The merged taskwait can only have a single WriteID, but the
						// previous accesses had different WriteIDs. So we need to set a
						// new write ID for this taskwait. But don't do this write away,
						// because more than one taskwait (potentially lots) may get
						// merged into a single taskwait. If so, we want to generate and
						// register a single WriteID rather than doing it every time we
						// merge in a taskwait.
						lastAccessNeedsNewWriteId = true;
					}

#ifndef NDEBUG
					DataAccessStatusEffects initialStatus(lastAccess);
					assert (initialStatus._isRemovable);
#endif
					accessStructures._removalBlockers--;
					assert(accessStructures._removalBlockers > 0);
					// accessStructures._liveTaskwaitFragmentCount--;
					// assert(accessStructures._liveTaskwaitFragmentCount > 0);
					/* true: erase the second region */
					return true;
				} else {
					if (lastAccessNeedsNewWriteId) {
						lastAccess->setNewWriteID();
						if (lastAccess->getLocation()->isClusterLocalMemoryPlace()) {
							WriteIDManager::registerWriteIDasLocal(lastAccess->getWriteID(), lastAccess->getAccessRegion());
						}
					}
					lastAccessNeedsNewWriteId = false;
				}

				lastAccess = access;
				/* false: do not erase this region */
				return false;
			}
		);
		if (lastAccessNeedsNewWriteId) {
			lastAccess->setNewWriteID();
			if (lastAccess->getLocation()->isClusterLocalMemoryPlace()) {
				WriteIDManager::registerWriteIDasLocal(lastAccess->getWriteID(), lastAccess->getAccessRegion());
			}
		}
	}

	struct BottomMapUpdateOperation {
		DataAccessRegion _region;
		DataAccessType _parentAccessType;

		bool _linkBottomMapAccessesToNext;

		bool _inhibitReadSatisfiabilityPropagation;
		bool _inhibitConcurrentSatisfiabilityPropagation;
		bool _inhibitCommutativeSatisfiabilityPropagation;
		bool _inhibitReductionInfoPropagation;

		bool _setCloseReduction;
		bool _namespaceNextIsIn;

		DataAccessLink _next;

		BottomMapUpdateOperation() :
			_region(),
			_parentAccessType(NO_ACCESS_TYPE),
			_linkBottomMapAccessesToNext(false),
			_inhibitReadSatisfiabilityPropagation(false),
			_inhibitConcurrentSatisfiabilityPropagation(false),
			_inhibitCommutativeSatisfiabilityPropagation(false),
			_inhibitReductionInfoPropagation(false),
			_setCloseReduction(false),
			_namespaceNextIsIn(false),
			_next()
		{
		}

		BottomMapUpdateOperation(DataAccessRegion const &region) :
			_region(region),
			_parentAccessType(NO_ACCESS_TYPE),
			_linkBottomMapAccessesToNext(false),
			_inhibitReadSatisfiabilityPropagation(false),
			_inhibitConcurrentSatisfiabilityPropagation(false),
			_inhibitCommutativeSatisfiabilityPropagation(false),
			_inhibitReductionInfoPropagation(false),
			_setCloseReduction(false),
			_namespaceNextIsIn(false),
			_next()
		{
		}

		bool empty() const
		{
			return !_linkBottomMapAccessesToNext;
		}
	};


	// Forward declarations
	static inline void processBottomMapUpdate(
		BottomMapUpdateOperation &operation,
		TaskDataAccesses &accessStructures, Task *task,
		/* OUT */ CPUDependencyData &hpDependencyData);
	static inline void removeBottomMapTaskwaitOrTopLevelSink(
		DataAccess *access, TaskDataAccesses &accessStructures, __attribute__((unused)) Task *task,
		/* OUT */ CPUDependencyData &hpDependencyData
	);
	static inline BottomMapEntry *fragmentBottomMapEntry(
		BottomMapEntry *bottomMapEntry, DataAccessRegion region,
		TaskDataAccesses &accessStructures, bool removeIntersection = false);
	static void handleRemovableTasks(
		/* inout */ CPUDependencyData::removable_task_list_t &removableTasks);
	static void handleCompletedTaskwaits(
		CPUDependencyData &completedTaskwaits,
		__attribute__((unused)) ComputePlace *computePlace);
	static inline DataAccess *fragmentAccess(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures);
	static inline DataAccess *createAccess(
		Task *originator,
		DataAccessObjectType objectType,
		DataAccessType accessType, bool weak, DataAccessRegion region,
		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex = no_reduction_type_and_operator,
		reduction_index_t reductionIndex = -1,
		MemoryPlace const *location = nullptr,
		MemoryPlace const *outputLocation = nullptr,
		ExecutionWorkflow::DataLinkStep *dataLinkStep = nullptr,
		DataAccess::status_t status = 0, DataAccessLink next = DataAccessLink()
	);
	static void removeFromNamespaceBottomMap(CPUDependencyData &hpDependencyData);

	/*
	 * Make the changes to the data access implied by the differences between
	 * initialStatus and updatedStatus. This is called with the lock for the
	 * tasks's data accesses (accessStructures). Any changes that cannot be
	 * done while this lock is held (as they need a different lock and taking
	 * it could cause a deadlock) will be added to hpDependencyData and done
	 * later (in processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks).
	 */
	static inline void handleDataAccessStatusChanges(
		DataAccessStatusEffects const &initialStatus,
		DataAccessStatusEffects const &updatedStatus,
		DataAccess *access, TaskDataAccesses &accessStructures, Task *task,
		/* OUT */ CPUDependencyData &hpDependencyData
	) {
		/* Check lock on task's access structures already taken by caller */
		// assert(task->getDataAccesses()._lock.isLockedByThisThread());

		// Registration
		if (initialStatus._isRegistered != updatedStatus._isRegistered) {
			assert(!initialStatus._isRegistered);

			// Count the access
			if (!initialStatus._isRemovable) {
				if (accessStructures._removalBlockers == 0) {
					// The blocking count is decreased once all the accesses become removable
					task->increaseRemovalBlockingCount();
				}
				accessStructures._removalBlockers++;

				/*
				 * Count the registered taskwait fragments, so know when they
				 * have all been handled.
				 */
				if (access->getObjectType() == taskwait_type) {
					accessStructures._liveTaskwaitFragmentCount++;
				}
			}

			// (Strong) Commutative accounting
			if (!access->isWeak() && (access->getType() == COMMUTATIVE_ACCESS_TYPE)) {
				accessStructures._totalCommutativeBytes += access->getAccessRegion().getSize();
			}

			if (updatedStatus._enforcesDependency) {
				task->increasePredecessors();
			}
		}

		if (!updatedStatus._isRegistered) {
			return;
		}

		// NOTE: After this point, all actions assume the access is registered

		// Satisfiability
		if (initialStatus._isSatisfied != updatedStatus._isSatisfied) {
			assert(!initialStatus._isSatisfied);
			Instrument::dataAccessBecomesSatisfied(
				access->getInstrumentationId(),
				true,
				task->getInstrumentationTaskId());
		}

		// Link to Next
		if (initialStatus._hasNext != updatedStatus._hasNext) {
			assert(!initialStatus._hasNext);
			Instrument::linkedDataAccesses(
				access->getInstrumentationId(),
				access->getNext()._task->getInstrumentationTaskId(),
				(Instrument::access_object_type_t)access->getNext()._objectType,
				access->getAccessRegion(),
				/* direct */ true, /* unidirectional */ false);
		}

		// Dependency updates
		if (initialStatus._enforcesDependency != updatedStatus._enforcesDependency) {
			if (updatedStatus._enforcesDependency) {
				// A new access that enforces a dependency.
				// Only happens when the task is first registered, and has already been
				// counted as part of the registration status change.
				assert(!initialStatus._isRegistered && updatedStatus._isRegistered);
			} else {
				// The access no longer enforces a dependency (has become satisfied)
				if (task->decreasePredecessors()) {
					// The task becomes ready
					if (accessStructures._totalCommutativeBytes != 0UL) {
						hpDependencyData._satisfiedCommutativeOriginators.push_back(task);
					} else {
						hpDependencyData._satisfiedOriginators.push_back(task);
					}
				}
			}
		}

		// Notify reduction original storage has become available
		if (initialStatus._makesReductionOriginalStorageAvailable != updatedStatus._makesReductionOriginalStorageAvailable) {
			assert(!initialStatus._makesReductionOriginalStorageAvailable);
			assert(access->getObjectType() == access_type);

			ReductionInfo *reductionInfo = access->getReductionInfo();
			assert(reductionInfo != nullptr);

			reductionInfo->makeOriginalStorageRegionAvailable(access->getAccessRegion());
		}

		// Reduction combination to a private reduction storage
		if ((initialStatus._combinesReductionToPrivateStorage != updatedStatus._combinesReductionToPrivateStorage)
			// If we can already combine to the original region directly, we just skip this step
			&& (initialStatus._combinesReductionToOriginal == updatedStatus._combinesReductionToOriginal)) {
			assert(!initialStatus._combinesReductionToPrivateStorage);
			assert(!initialStatus._combinesReductionToOriginal);

			assert(!access->hasBeenDiscounted());

			assert(access->getType() == REDUCTION_ACCESS_TYPE);
			assert(access->allocatedReductionInfo() || (access->receivedReductionInfo() && access->receivedReductionSlotSet()));

			ReductionInfo *reductionInfo = access->getReductionInfo();
			assert(reductionInfo != nullptr);
			__attribute__((unused)) bool wasLastCombination =
				reductionInfo->combineRegion(access->getAccessRegion(), access->getReductionSlotSet(), /* canCombineToOriginalStorage */ false);
			assert(!wasLastCombination);
		}

		// Reduction combination to original region
		if (initialStatus._combinesReductionToOriginal != updatedStatus._combinesReductionToOriginal) {
			assert(!initialStatus._combinesReductionToOriginal);
			assert(updatedStatus._combinesReductionToPrivateStorage);

			assert(!access->hasBeenDiscounted());

			assert(access->getType() == REDUCTION_ACCESS_TYPE);
			assert(access->receivedReductionInfo());
			assert(access->allocatedReductionInfo() || access->receivedReductionSlotSet());

			ReductionInfo *reductionInfo = access->getReductionInfo();
			assert(reductionInfo != nullptr);
			bool wasLastCombination = reductionInfo->combineRegion(access->getAccessRegion(), access->getReductionSlotSet(), /* canCombineToOriginalStorage */ true);

			if (wasLastCombination) {
				const DataAccessRegion &originalRegion = reductionInfo->getOriginalRegion();

				ObjectAllocator<ReductionInfo>::deleteObject(reductionInfo);

				Instrument::deallocatedReductionInfo(
					access->getInstrumentationId(),
					reductionInfo,
					originalRegion);
			}
		}

		// Release of commutative region
		if (initialStatus._releasesCommutativeRegion != updatedStatus._releasesCommutativeRegion) {
			assert(!initialStatus._releasesCommutativeRegion);
			hpDependencyData._releasedCommutativeRegions.emplace_back(task, access->getAccessRegion(), access->getLocation());
		}

		// Propagation to Next
		if (access->hasNext()) {
			/*
			 * Prepare an update operation that will affect the next task.
			 */
			UpdateOperation updateOperation(access->getNext(), access->getAccessRegion());
			updateOperation._location = access->getLocation();
			updateOperation._writeID = access->getWriteID();

			if (initialStatus._propagatesReadSatisfiabilityToNext != updatedStatus._propagatesReadSatisfiabilityToNext) {
				assert(!initialStatus._propagatesReadSatisfiabilityToNext);
				updateOperation._makeReadSatisfied = true; /* make next task read satisfied */
				// assert(access->hasLocation());
			}

			// Note: do not pass namespace propagation info to taskwaits
			// (1) It would be unnecessary delayed operations and overhead.
			// (2) It may result in fragmentation of a taskwait after it
			//     become complete. In this case the taskwait may be
			//     fragmented while it is in the _completedTaskwaits list, which
			//     will fail (since only the first fragment would be in the list).
			if (updatedStatus._allowNamespacePropagation
				&& !access->getPropagatedNamespaceInfo()
				&& (access->getNext()._objectType == access_type)) {

				if (access->getType() == CONCURRENT_ACCESS_TYPE
					|| access->getType() == COMMUTATIVE_ACCESS_TYPE) {
					// Do not propagate in the namespace from a concurrent or commutative
					// access, as the synchronization is done at the offloader's side.
					updateOperation._validNamespace = VALID_NAMESPACE_NONE;
					updateOperation._namespacePredecessor = access->getOriginator()->getOffloadedTaskId();
					updateOperation._namespaceAccessType = NO_ACCESS_TYPE;
					access->setPropagatedNamespaceInfo();
				} else if (access->getObjectType() == access_type
					&& access->getType() == READ_ACCESS_TYPE
						&& access->getValidNamespacePrevious() != VALID_NAMESPACE_UNKNOWN) {
					// Read access: we allow multiple concurrent readers propagating
					// in different namespaces. To do this, pass the same namespace previous
					// to the next access, so all concurrent reads get the same namespace
					// predecessor, but only if the tasks are siblings.
					if (access->getNext()._task->getParent() == access->getOriginator()->getParent()) {
						updateOperation._validNamespace = access->getValidNamespacePrevious();
						updateOperation._namespacePredecessor = access->getNamespacePredecessor();
					} else {
						updateOperation._validNamespace = VALID_NAMESPACE_NONE;
						updateOperation._namespacePredecessor = OffloadedTaskIdManager::InvalidOffloadedTaskId;
					}
					updateOperation._namespaceAccessType = READ_ACCESS_TYPE;
					access->setPropagatedNamespaceInfo();
				} else if ((access->getObjectType() != access_type
					|| access->getType() != READ_ACCESS_TYPE)
					&& access->getValidNamespaceSelf() != VALID_NAMESPACE_UNKNOWN) {
					// Other object types or non-read accesses: propagate own namespace info
					// to allow remote namespace propagation from this access to the next.
					updateOperation._validNamespace = access->getValidNamespaceSelf();
					if (access->getObjectType() == fragment_type) {
						updateOperation._namespacePredecessor = access->getOriginator()->getOffloadedTaskIdAsParent();
					} else {
						updateOperation._namespacePredecessor = access->getOriginator()->getOffloadedTaskId();
					}
					updateOperation._namespaceAccessType = access->getType();
					access->setPropagatedNamespaceInfo();
				}
			}


			if (initialStatus._propagatesWriteSatisfiabilityToNext != updatedStatus._propagatesWriteSatisfiabilityToNext) {
				assert(!initialStatus._propagatesWriteSatisfiabilityToNext);

				/*
				 * This assertion happens occasionally. Temporarily disable it.
				 */
				// assert(!access->canPropagateReductionInfo() || updatedStatus._propagatesReductionInfoToNext);
				updateOperation._makeWriteSatisfied = true;
				updateOperation._previousIsCommutative = (access->getType() == COMMUTATIVE_ACCESS_TYPE);
			}

			if (initialStatus._propagatesConcurrentSatisfiabilityToNext != updatedStatus._propagatesConcurrentSatisfiabilityToNext) {
				assert(!initialStatus._propagatesConcurrentSatisfiabilityToNext);
				updateOperation._makeConcurrentSatisfied = true;
			}
			if (initialStatus._propagatesCommutativeSatisfiabilityToNext != updatedStatus._propagatesCommutativeSatisfiabilityToNext) {
				assert(!initialStatus._propagatesCommutativeSatisfiabilityToNext);
				updateOperation._makeCommutativeSatisfied = true;
			}

			if (initialStatus._propagatesReductionInfoToNext != updatedStatus._propagatesReductionInfoToNext) {
				assert(!initialStatus._propagatesReductionInfoToNext);
				assert((access->getType() != REDUCTION_ACCESS_TYPE) || (access->receivedReductionInfo() || access->allocatedReductionInfo()));
				updateOperation._setReductionInfo = true;
				updateOperation._reductionInfo = access->getReductionInfo();
			}

			if (initialStatus._propagatesReductionSlotSetToNext != updatedStatus._propagatesReductionSlotSetToNext) {
				assert(!initialStatus._propagatesReductionSlotSetToNext);

				// Reduction slot set computation

				assert(access->getType() == REDUCTION_ACCESS_TYPE);
				assert(access->receivedReductionInfo() || access->allocatedReductionInfo());
				assert(access->getReductionSlotSet().size() > 0);
				assert(access->isWeak() || task->isFinal() || access->getReductionSlotSet().any());

				updateOperation._reductionSlotSet = access->getReductionSlotSet();
			}

			if (!updateOperation.empty()) {
				hpDependencyData._delayedOperations.emplace_back(updateOperation);
			}
		}

		// Propagation to Fragments
		if (access->hasSubaccesses()) {
			UpdateOperation updateOperation(DataAccessLink(task, fragment_type), access->getAccessRegion());
			updateOperation._location = access->getLocation();

			if (initialStatus._propagatesReadSatisfiabilityToFragments != updatedStatus._propagatesReadSatisfiabilityToFragments) {
				assert(!initialStatus._propagatesReadSatisfiabilityToFragments);
				updateOperation._makeReadSatisfied = true;
				updateOperation._writeID = access->getWriteID();
				// assert(access->hasLocation());
			}

			if (initialStatus._propagatesWriteSatisfiabilityToFragments != updatedStatus._propagatesWriteSatisfiabilityToFragments) {
				assert(!initialStatus._propagatesWriteSatisfiabilityToFragments);
				updateOperation._makeWriteSatisfied = true;
			}

			if (initialStatus._propagatesConcurrentSatisfiabilityToFragments != updatedStatus._propagatesConcurrentSatisfiabilityToFragments) {
				assert(!initialStatus._propagatesConcurrentSatisfiabilityToFragments);
				updateOperation._makeConcurrentSatisfied = true;
			}

			if (initialStatus._propagatesCommutativeSatisfiabilityToFragments != updatedStatus._propagatesCommutativeSatisfiabilityToFragments) {
				assert(!initialStatus._propagatesCommutativeSatisfiabilityToFragments);
				updateOperation._makeCommutativeSatisfied = true;
			}

			if (initialStatus._propagatesReductionInfoToFragments != updatedStatus._propagatesReductionInfoToFragments) {
				assert(!initialStatus._propagatesReductionInfoToFragments);
				assert(!(access->getType() == REDUCTION_ACCESS_TYPE) || (access->receivedReductionInfo() || access->allocatedReductionInfo()));
				updateOperation._setReductionInfo = true;
				updateOperation._reductionInfo = access->getReductionInfo();
			}

			if (initialStatus._propagatesReductionSlotSetToFragments != updatedStatus._propagatesReductionSlotSetToFragments) {
				assert(!initialStatus._propagatesReductionSlotSetToFragments);

				assert(access->receivedReductionSlotSet() || ((access->getType() == REDUCTION_ACCESS_TYPE) && access->allocatedReductionInfo()));
				assert(access->getReductionSlotSet().size() > 0);

				updateOperation._reductionSlotSet = access->getReductionSlotSet();
			}

			if (!updateOperation.empty()) {
				hpDependencyData._delayedOperations.emplace_back(updateOperation);
			}
		}

		// Bottom Map Updates
		if (access->hasSubaccesses()) {
			if (
				initialStatus._linksBottomMapAccessesToNextAndInhibitsPropagation
				!= updatedStatus._linksBottomMapAccessesToNextAndInhibitsPropagation) {
				BottomMapUpdateOperation bottomMapUpdateOperation(access->getAccessRegion());

				bottomMapUpdateOperation._parentAccessType = access->getType();

				bottomMapUpdateOperation._linkBottomMapAccessesToNext = true;
				bottomMapUpdateOperation._next = access->getNext();
				bottomMapUpdateOperation._namespaceNextIsIn = access->getNamespaceNextIsIn();

				bottomMapUpdateOperation._inhibitReadSatisfiabilityPropagation = (access->getType() == READ_ACCESS_TYPE);
				assert(!updatedStatus._propagatesWriteSatisfiabilityToNext);
				bottomMapUpdateOperation._inhibitConcurrentSatisfiabilityPropagation = (access->getType() == CONCURRENT_ACCESS_TYPE);
				bottomMapUpdateOperation._inhibitCommutativeSatisfiabilityPropagation = (access->getType() == COMMUTATIVE_ACCESS_TYPE);
				// 'write' and 'readwrite' accesses can have a nested reduction that is combined outside the parent task itself, and thus
				// their ReductionInfo needs to be propagates through the bottom map
				// Subaccesses of an access that can't have a nested reduction which is visible outside
				// should never propagate the ReductionInfo (it is already propagated by the parent access)
				bottomMapUpdateOperation._inhibitReductionInfoPropagation =
					(access->getType() != WRITE_ACCESS_TYPE) && (access->getType() != READWRITE_ACCESS_TYPE) && (access->getType() != AUTO_ACCESS_TYPE);

				bottomMapUpdateOperation._setCloseReduction = (access->getType() != REDUCTION_ACCESS_TYPE) || access->closesReduction();

				processBottomMapUpdate(bottomMapUpdateOperation, accessStructures, task, hpDependencyData);
			}
		}

		if (initialStatus._triggersTaskwaitWorkflow != updatedStatus._triggersTaskwaitWorkflow) {
			assert(!initialStatus._triggersTaskwaitWorkflow);
			assert(access->getObjectType() == taskwait_type);
			assert(access->readSatisfied());
			assert(access->writeSatisfied());
			assert(!access->complete());
			assert(!access->hasNext());
			assert(access->isInBottomMap());

			hpDependencyData._completedTaskwaits.emplace_back(access);
		}

		// DataReleaseStep triggers
		if (initialStatus._triggersDataRelease != updatedStatus._triggersDataRelease) {
			assert(!initialStatus._triggersDataRelease);

			// This assert is not required but a limitation of the current simple implementation.
			// But in a general case it may be removed in favor of a more refined call.
			assert(access->getOriginator() == task);
			assert(task->hasDataReleaseStep());

			// We are about to perform the data release (from the original access or top-level
			// sink). So set the dataReleased flag to prevent propagation on the namespace.
			access->setDataReleased();

			// Unmap the task's access from the namespace bottom map. This can be
			// done as a delayed operation, because the dataReleased flag is now
			// set, so despite the access still being on the bottom map, the
			// namespace propagation will fail. We must, however, ensure that it is
			// removed from the bottom map (by removeFromNamespaceBottomMap) before the
			// task is deleted.
			assert(access->getOriginator()->getParent() == NodeNamespace::getNamespaceTask());
			hpDependencyData._namespaceRegionsToRemove.emplace_back(CPUDependencyData::TaskAndRegion(access->getOriginator(), access->getAccessRegion(), /* location not needed */ nullptr));

			// Don't release a concurrent access if the location hasn't changed. That happens
			// either (a) because we didn't write to it (it must have been weakconcurrent) or (b)
			// because we did write to it, but on the same node that it was on before the concurrent tasks.
			// By sending no message we will not change the location, which in case (a) will let
			// another concurrent task do the write and in case (b) gives the correct location already.
			bool dontRelease = (access->getType() == CONCURRENT_ACCESS_TYPE
								&& (access->getLocation() == access->getConcurrentInitialLocation()))
								|| (access->getType() == AUTO_ACCESS_TYPE && access->getDisableEagerSend() && ClusterManager::autoOptimizeNonAccessed());

			if (access->getLocation() != nullptr && !dontRelease) {

				// Any access that does a data release must be read satisfied, so its location must
				// be known. Unless it is a non-accessed auto
				assert(access->hasLocation());

				// This adds the access to the ClusterDataReleaseStep::_releaseInfo vector.
				// The accesses will be released latter.
				assert(access->getObjectType() == access_type || access->getObjectType() == top_level_sink_type);
				access->getOriginator()->getDataReleaseStep()->addToReleaseList(access);
			}

			if (access->getType() == COMMUTATIVE_ACCESS_TYPE) {
				// This is the last commutative task here, so delete the scoreboard entry.
				CommutativeScoreboard::_lock.lock();
				CommutativeScoreboard::endCommutative(access->getAccessRegion());
				CommutativeScoreboard::_lock.unlock();
			}
		}

		/*
		 * If task offloaded from this node to another node receives read
		 * satisfiability twice (once through the local dependency system and
		 * again from the remote namespace), then triggersDataLinkRead for this
		 * access will follow the sequence:
		 *     0    (has DataLinkStep but not read satisfied)
		 *     1    (has DataLinkStep and read satisfied locally) => linkRegion
		 *     0    (DataLinkStep unset and read satisfied again remotely) => nothing
		 */
		bool linksRead = initialStatus._triggersDataLinkRead < updatedStatus._triggersDataLinkRead;
		bool linksWrite = initialStatus._triggersDataLinkWrite < updatedStatus._triggersDataLinkWrite;
		bool linksConcurrent = initialStatus._triggersDataLinkConcurrent < updatedStatus._triggersDataLinkConcurrent;
		if (!(access->getType() == AUTO_ACCESS_TYPE && access->getDisableEagerSend() && ClusterManager::autoOptimizeNonAccessed())
			&& (linksRead || linksWrite || linksConcurrent)) {
			assert(access->hasDataLinkStep());

			ExecutionWorkflow::DataLinkStep *step = access->getDataLinkStep();

			if (access->getType() == READ_ACCESS_TYPE) {
				linksWrite = false; // never link true write satisfiability to an in access
				if (linksRead) {
					// Link pseudowrite with read satisfiability. We send it as if it
					// is write satisfiability - the remote node will treat it as write
					// satisfiability. This only works because namespace propagation
					// never happens from an in to an inout or out access.
					linksWrite = true;
				}
			} else if (access->getType() == COMMUTATIVE_ACCESS_TYPE) {
				// Commutative accesses of offloaded tasks get pseudowrite and pseudoread
				// Note: they are ready when they have commutative satisfiability. Read
				// and write satisfiability is passed when all predecessors have completed.
				assert(!access->isWeak()); // Weak commutative accesses not supported yet
				linksRead = false;
				linksWrite = false;
			} else if (access->getType() == CONCURRENT_ACCESS_TYPE) {
				// Concurrent accesses of offloaded tasks get pseudowrite and pseudoread
				// when they are concurrent satisfied.
				linksRead = linksConcurrent;
				linksWrite = linksConcurrent;
			}

			/*
			 * Send satisfiability through the workflow. For Nanos6@cluster, this will
			 * send a MessageSatisfiability to a remote node.
			 * NOTE: it is possible for access->getLocation() to be nullptr only
			 * in the rare case that write satisfiability is propagated before read
			 * satisfiability.
			 */
			if (linksRead || linksWrite) {
				step->linkRegion(access, linksRead, linksWrite, hpDependencyData._satisfiabilityMap, hpDependencyData._dataSendRegionInfoMap);
			}

			if (updatedStatus._triggersDataLinkRead && updatedStatus._triggersDataLinkWrite) {
				access->unsetDataLinkStep();
			}
		}

		// Access becomes removable
		if (initialStatus._isRemovable != updatedStatus._isRemovable) {
			assert(!initialStatus._isRemovable);

			int newRemovalBlockers;

			/*
			 * Discounted means that it is no longer blocking the removal of
			 * the task (?)
			 */
			if (access->getObjectType() != taskwait_type) {
				access->markAsDiscounted();
				newRemovalBlockers = accessStructures._removalBlockers.fetch_sub(1, std::memory_order_relaxed) - 1;
				assert(newRemovalBlockers >= 0);
			} else {
				newRemovalBlockers = accessStructures._removalBlockers;
			}

			if (access->getObjectType() == taskwait_type) {
				// Update parent data access ReductionSlotSet with information from its subaccesses
				// collected at the taskwait fragment
				// Note: This shouldn't be done for top-level sink fragments, as their presence
				// in the bottomMap just means that there is no matching access in the parent
				// (the reduction is local and not waited for)
				if (access->getType() == REDUCTION_ACCESS_TYPE) {
					assert(access->getReductionSlotSet().size() > 0);

					accessStructures._accesses.processIntersecting(
						access->getAccessRegion(),
						[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
							DataAccess *dataAccess = &(*position);
							assert(dataAccess != nullptr);
							assert(!dataAccess->hasBeenDiscounted());

							assert(dataAccess->getType() == REDUCTION_ACCESS_TYPE);
							assert(dataAccess->isWeak());

							assert(dataAccess->receivedReductionInfo() || dataAccess->allocatedReductionInfo());
							assert(access->receivedReductionInfo());
							assert(dataAccess->getReductionInfo() == access->getReductionInfo());

							assert(dataAccess->getReductionSlotSet().size() ==
								access->getReductionSlotSet().size()
							);

							dataAccess->getReductionSlotSet() |= access->getReductionSlotSet();

							return true;
						});
				}

				// The last taskwait fragment that finishes removes the blocking over the task
				int newLiveFragmentCount = (accessStructures._liveTaskwaitFragmentCount.fetch_sub(1, std::memory_order_relaxed) - 1);
				assert(newLiveFragmentCount >= 0);

				if (newLiveFragmentCount == 0) {
					if (task->decreaseBlockingCount())
						hpDependencyData._satisfiedOriginators.push_back(task);
				}
			}

			if (access->hasNext()) {
				Instrument::unlinkedDataAccesses(
					access->getInstrumentationId(),
					access->getNext()._task->getInstrumentationTaskId(),
					(Instrument::access_object_type_t)access->getNext()._objectType,
					/* direct */ true);
			} else {
				/*
				 * The access has no next access, so actually delete it.
				 */
				if ((access->getObjectType() == taskwait_type)
					|| (access->getObjectType() == top_level_sink_type))
				{
				} else if (access->getObjectType() == fragment_type) {
					// This is a fragment that was completed by unregisterLocalAccess.
					// Since it has no next access (see outer "if" condition), it
					// must be part of a fragmented access that was never accessed
					// by a subtask. It is OK to just remove.
					Instrument::removedDataAccess(access->getInstrumentationId());
					accessStructures._accessFragments.erase(access);
					ObjectAllocator<DataAccess>::deleteObject(access);
				} else {
					// This also happens if a task finishes and there are subtasks
					// on the bottom map accessing an lmalloc'ed (or dmalloc'ed) region.
					// assert(access->getOriginator()->isRemoteTask()
					// 	|| (access->getObjectType() == access_type
					// 		&& access->getType() == NO_ACCESS_TYPE));

					Instrument::removedDataAccess(access->getInstrumentationId());
					accessStructures._accesses.erase(access);
					ObjectAllocator<DataAccess>::deleteObject(access);
				}
			}

			/*
			 * This removable access is no longer blocking the removal of the
			 * task itself. Decrement the task's removal blocking count (of
			 * accesses) and, if it becomes zero, list the task as removable.
			 */
			if (newRemovalBlockers == 0) {
				if (task->decreaseRemovalBlockingCount()) {
					hpDependencyData._removableTasks.push_back(task);
				}
			}
		}
	}

	static inline void removeBottomMapTaskwaitOrTopLevelSink( DataAccess *access, TaskDataAccesses &accessStructures,
		__attribute__((unused)) Task *task,
		/* OUT */ CPUDependencyData &hpDependencyData
	) {
		(void)hpDependencyData;
		assert(access != nullptr);
		assert(task != nullptr);
		assert(access->getOriginator() == task);
		// assert(accessStructures._lock.isLockedByThisThread());
		assert((access->getObjectType() == taskwait_type) || (access->getObjectType() == top_level_sink_type));

		//! We are about to delete the taskwait fragment. Before doing so,
		//! move the location info back to the original access
		accessStructures._accesses.processIntersectingAndMissing(
			access->getAccessRegion(),
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *originalAccess = &(*position);
				assert(originalAccess != nullptr);
				// Skip accesses handled by unregisterLocallyPropagatedTaskDataAccesses.
				if (originalAccess->complete() && !originalAccess->getNamespaceNextIsIn()) {
					return true;
				}
				assert(!originalAccess->hasBeenDiscounted());

				originalAccess =
					fragmentAccess(originalAccess,
						access->getAccessRegion(),
						accessStructures);

				if (originalAccess->getType() == AUTO_ACCESS_TYPE) {
					if (!access->getDisableEagerSend()) {
						originalAccess->setAutoHasBeenAccessed();
					} else {
						// Wait or autowait at end of task. If the region was not actually
						// accessed, then record this in the original access so that we don't
						// send a second, redundant NoEagerSend.
						assert(task->hasFinished());
						originalAccess->setDisableEagerSend();
					}
				}

				const MemoryPlace *location;

				if (access->getOutputLocation() && access->getOutputLocation()->isClusterLocalMemoryPlace()) {
					// If the output location is a local memory place, then use the local
					// location. Note: for cases where a copy isn't needed (e.g. read accesses,
					// distributed memory), the output location will have been cleared by the workflow.
					location = access->getOutputLocation();
					if (!originalAccess->isWeak() && originalAccess->getType() != READ_ACCESS_TYPE) {
						// If it is a strong access that is not a read, then assign a new
						// write ID to capture any changes.
						originalAccess->setNewLocalWriteID();
					} else {
						// Note: already registered as local by the taskwait's workflow
						originalAccess->setWriteID(access->getWriteID());
					}
				} else {
					// Use the location before the taskwait
					location = access->getLocation();
					originalAccess->setWriteID(access->getWriteID());
				}

				if (originalAccess->getLocation()->isClusterLocalMemoryPlace()
					|| !location->isClusterLocalMemoryPlace()) {
					// Either the original access was already local or the new location
					// is non-local. In either case, we only need to update the location
					// and writeID of the original access.
					if (originalAccess->getType() != READ_ACCESS_TYPE) {
						originalAccess->setLocation(location);
					}
				} else {
					// Updating the location of the original access from a non-local to
					// a local location may cause read satisfiability to be propagated to
					// the next access. This is the logic in disableReadPropagationToNext
					// which reduces unnecessary data fetches that would otherwise happen
					// from the old location. Note: it is important that the fragments
					// have already been removed, since when there are fragments the
					// logic to propagate satisfiability does not take account of
					// disableReadPropagationToNext.
					DataAccessStatusEffects initialStatus(originalAccess);
					originalAccess->setLocation(location);
					DataAccessStatusEffects updatedStatus(originalAccess);
					handleDataAccessStatusChanges(
						initialStatus, updatedStatus,
						originalAccess, accessStructures, originalAccess->getOriginator(),
						hpDependencyData);
				}
				return true;
			},
			[&](DataAccessRegion missingRegion) -> bool {

				// Missing region when removing a taskwait. This happens if a
				// child task with a weak access on "all memory" allocates
				// memory and doesn't free it. In this case we need to register
				// it in the parent's accesses in order to remember its
				// location.
				DataAccess *newLocalAccess = createAccess(
					task,
					access_type,
					NO_ACCESS_TYPE,
					/* weak */ true, // keep location but do not normally fetch
					missingRegion
				);

				/* Modifications to be done after the lock is taken  */
				DataAccessStatusEffects initialStatus(newLocalAccess);
				newLocalAccess->setNewInstrumentationId(task->getInstrumentationTaskId());

				newLocalAccess->setReadSatisfied(access->getLocation());
				newLocalAccess->setWriteSatisfied();
				newLocalAccess->setConcurrentSatisfied();
				newLocalAccess->setCommutativeSatisfied();
				newLocalAccess->setReceivedReductionInfo();
				newLocalAccess->setValidNamespacePrevious(
					VALID_NAMESPACE_NONE,
					OffloadedTaskIdManager::InvalidOffloadedTaskId
				);
				newLocalAccess->setValidNamespaceSelf(VALID_NAMESPACE_NONE);
				newLocalAccess->setRegistered();
		#ifndef NDEBUG
				newLocalAccess->setReachable();
		#endif
				DataAccessStatusEffects updatedStatus(newLocalAccess);
				//! This is an exception to avoid decreasing predecessor and it
				//! is not used anywhere else.
				updatedStatus.setEnforcesDependency();

				/* Insert the new access */
				accessStructures._accesses.insert(*newLocalAccess);

				/* Handle the above data access status changes */
				handleDataAccessStatusChanges(
					initialStatus,
					updatedStatus,
					newLocalAccess,
					accessStructures,
					task,
					hpDependencyData
				);
				return true;
			});

		accessStructures._taskwaitFragments.erase(access);
		ObjectAllocator<DataAccess>::deleteObject(access);
	}


	/*
	 * Internal function to create a new data access.
	 */
	static inline DataAccess *createAccess(
		Task *originator,
		DataAccessObjectType objectType,
		DataAccessType accessType, bool weak, DataAccessRegion region,
		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex,
		reduction_index_t reductionIndex,
		MemoryPlace const *location,
		MemoryPlace const *outputLocation,
		ExecutionWorkflow::DataLinkStep *dataLinkStep,
		DataAccess::status_t status, DataAccessLink next
	) {
		// Not sure why this was previously commented as "Regular object duplication"
		DataAccess *dataAccess = ObjectAllocator<DataAccess>::newObject(
			objectType,
			accessType, weak, originator, region,
			reductionTypeAndOperatorIndex,
			reductionIndex,
			location,
			outputLocation,
			dataLinkStep,
			Instrument::data_access_id_t(),
			status, next);

		return dataAccess;
	}

	/*
	 * The upgrade rules are summarized in the below table. Only the upper triangle is shown,
	 * as the table is symmetric. The entries on the diagonal are indicated by "#".
	 *
	 *  		     NONE READ WRITE READWRITE  CONCURRENT COMMUTATIVE REDUCTION ALLMEMORY
	 *  NONE         #    R    W     RW         concurrent commutative reduction None
	 *  READ              #    RW    RW         RW         RW          Invalid   R
	 *  WRITE                  #     RW         RW         RW          Invalid   W
	 *  READWRITE                    #          RW         RW          Invalid   RW
	 *  CONCURRENT                              #          commutative Invalid   concurrent
	 *  COMMUTATIVE                                        #           Invalid   commutative
	 *  REDUCTION                                                      #         reduction
	 *  ALLMEMORY                                                                #
	 */

	static DataAccessType upgradeAccessTable[AUTO_ACCESS_TYPE+1][AUTO_ACCESS_TYPE+1] = {
		/* NO_ACCESS_TYPE */
			{ NO_ACCESS_TYPE, READ_ACCESS_TYPE, WRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, CONCURRENT_ACCESS_TYPE, COMMUTATIVE_ACCESS_TYPE, REDUCTION_ACCESS_TYPE, NO_ACCESS_TYPE },
		/* READ_ACCESS_TYPE */
			{ READ_ACCESS_TYPE, READ_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, NO_ACCESS_TYPE, READ_ACCESS_TYPE },
		/* WRITE_ACCESS_TYPE */
			{ WRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, WRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, NO_ACCESS_TYPE, WRITE_ACCESS_TYPE },
		/* READWRITE_ACCESS_TYPE */
			{ READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, NO_ACCESS_TYPE, READWRITE_ACCESS_TYPE },
		/* CONCURRENT_ACCESS_TYPE */
			{ CONCURRENT_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, COMMUTATIVE_ACCESS_TYPE, NO_ACCESS_TYPE, CONCURRENT_ACCESS_TYPE },
		/* COMMUTATIVE_ACCESS_TYPE */
			{ COMMUTATIVE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, COMMUTATIVE_ACCESS_TYPE, CONCURRENT_ACCESS_TYPE, NO_ACCESS_TYPE, COMMUTATIVE_ACCESS_TYPE },
		/* REDUCTION_ACCESS_TYPE */
			{ REDUCTION_ACCESS_TYPE, NO_ACCESS_TYPE, NO_ACCESS_TYPE, NO_ACCESS_TYPE, NO_ACCESS_TYPE, NO_ACCESS_TYPE, REDUCTION_ACCESS_TYPE, REDUCTION_ACCESS_TYPE },
		/* AUTO_ACCESS_TYPE */
			{ NO_ACCESS_TYPE, READ_ACCESS_TYPE, WRITE_ACCESS_TYPE, READWRITE_ACCESS_TYPE, CONCURRENT_ACCESS_TYPE, COMMUTATIVE_ACCESS_TYPE, REDUCTION_ACCESS_TYPE, AUTO_ACCESS_TYPE },
	};

	/*
	 * Upgrade an access: called by registerTaskDataAccess when a task's access
	 * intersects a previously-registered access of the same task.
	 */
	static inline void upgradeAccess(
		DataAccess *dataAccess,
		DataAccessType accessType,
		bool weak,
		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex
	) {
		assert(dataAccess != nullptr);
		assert(!dataAccess->hasBeenDiscounted());

		/* Only weak if both accesses are weak */
		bool newWeak = dataAccess->isWeak() && weak;

		DataAccessType newDataAccessType = accessType;
		if (accessType != dataAccess->getType()) {
			FatalErrorHandler::failIf(
				(accessType == REDUCTION_ACCESS_TYPE) || (dataAccess->getType() == REDUCTION_ACCESS_TYPE),
				"Task ",
				(dataAccess->getOriginator()->getTaskInfo()->implementations[0].task_label != nullptr ? dataAccess->getOriginator()->getTaskInfo()->implementations[0].task_label : dataAccess->getOriginator()->getTaskInfo()->implementations[0].declaration_source),
				" has non-reduction accesses that overlap a reduction");

			assert(accessType <= AUTO_ACCESS_TYPE);
			assert(dataAccess->getType() <= AUTO_ACCESS_TYPE);
			newDataAccessType = upgradeAccessTable[accessType][dataAccess->getType()];
			assert(newDataAccessType == upgradeAccessTable[dataAccess->getType()][accessType]);

		} else {
			FatalErrorHandler::failIf(
				(accessType == REDUCTION_ACCESS_TYPE)
					&& (dataAccess->getReductionTypeAndOperatorIndex() != reductionTypeAndOperatorIndex),
				"Task ",
				(dataAccess->getOriginator()->getTaskInfo()->implementations[0].task_label != nullptr ? dataAccess->getOriginator()->getTaskInfo()->implementations[0].task_label : dataAccess->getOriginator()->getTaskInfo()->implementations[0].declaration_source),
				" has two overlapping reductions over different types or with different operators");
		}

		dataAccess->upgrade(newWeak, newDataAccessType);
	}


	// NOTE: locking should be handled from the outside
	static inline DataAccess *duplicateDataAccess(
		DataAccess const &toBeDuplicated,
		__attribute__((unused)) TaskDataAccesses &accessStructures)
	{
		assert(toBeDuplicated.getOriginator() != nullptr);
		assert(!accessStructures.hasBeenDeleted());
		assert(!toBeDuplicated.hasBeenDiscounted());

		// Regular object duplication
		DataAccess *newFragment = ObjectAllocator<DataAccess>::newObject(toBeDuplicated);

		// Copy symbols
		newFragment->addToSymbols(toBeDuplicated.getSymbols()); // TODO: Consider removing the pointer from declaration and make it a reference

		newFragment->clearRegistered();

		return newFragment;
	}


#ifndef NDEBUG

	/*
	 * Debug function to check that none of the accesses is marked as
	 * reachable.  If no access is reachable then no locking is necessary on
	 * the access structures. Accesses become reachable in linkTaskAccesses,
	 * when the task's accesses are linked into the dependency system, and in
	 * createTaskwait and createTopLevelSink, which do the same for taskwaits
	 * and top-level sinks. They are then reachable until they are destroyed.
	 */
	static bool noAccessIsReachable(TaskDataAccesses &accessStructures) __attribute__((unused));

	static bool noAccessIsReachable(TaskDataAccesses &accessStructures)
	{
		assert(!accessStructures.hasBeenDeleted());
		return accessStructures._accesses.processAll(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				return !position->isReachable();
			});
	}
#endif


	static inline BottomMapEntry *fragmentBottomMapEntry(
		BottomMapEntry *bottomMapEntry, DataAccessRegion region,
		TaskDataAccesses &accessStructures, bool removeIntersection)
	{
		if (bottomMapEntry->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return bottomMapEntry;
		}

		assert(!accessStructures.hasBeenDeleted());
		// assert(accessStructures._lock.isLockedByThisThread());

		TaskDataAccesses::subaccess_bottom_map_t::iterator position =
			accessStructures._subaccessBottomMap.iterator_to(*bottomMapEntry);
		position = accessStructures._subaccessBottomMap.fragmentByIntersection(
			position, region,
			removeIntersection,
			[&](BottomMapEntry const &toBeDuplicated) -> BottomMapEntry * {
				return ObjectAllocator<BottomMapEntry>::newObject(DataAccessRegion(), toBeDuplicated._link,
					toBeDuplicated._accessType, toBeDuplicated._reductionTypeAndOperatorIndex);
			},
			[&](__attribute__((unused)) BottomMapEntry *fragment, __attribute__((unused)) BottomMapEntry *originalBottomMapEntry) {
			});

		if (!removeIntersection) {
			bottomMapEntry = &(*position);
			assert(bottomMapEntry != nullptr);
			assert(bottomMapEntry->getAccessRegion().fullyContainedIn(region));

			return bottomMapEntry;
		} else {
			return nullptr;
		}
	}


	static inline void setUpNewFragment(
		DataAccess *fragment, DataAccess *originalDataAccess,
		TaskDataAccesses &accessStructures)
	{
		if (fragment != originalDataAccess) {
			CPUDependencyData hpDependencyData;

			DataAccessStatusEffects initialStatus(fragment);
			fragment->setUpNewFragment(originalDataAccess->getInstrumentationId());
			fragment->setRegistered();
			DataAccessStatusEffects updatedStatus(fragment);
			updatedStatus._allowNamespacePropagation = false;

			handleDataAccessStatusChanges(
				initialStatus, updatedStatus,
				fragment, accessStructures, fragment->getOriginator(),
				hpDependencyData);

			/* Do not expect any delayed operations */
			assert (hpDependencyData.empty());
		}
	}


	/*
	 * fragmentAccessObject: Fragment an access if necessary to match a region.
	 *
	 * The task access structures must be either locked or not reachable.
	 *
	 */

	static inline DataAccess *fragmentAccessObject(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures)
	{
		assert(!dataAccess->hasBeenDiscounted());
		assert(dataAccess->getObjectType() == access_type);

		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return dataAccess;
		}

		TaskDataAccesses::accesses_t::iterator position =
			accessStructures._accesses.iterator_to(*dataAccess);
		position = accessStructures._accesses.fragmentByIntersection(
			position, region,
			/* removeIntersection */ false,
			/* duplicator */
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			/* postprocessor */
			[&](DataAccess *fragment, DataAccess *originalDataAccess) {
				setUpNewFragment(fragment, originalDataAccess, accessStructures);
			});

		/*
		 * Return the part of this access that is fully inside the given region
		 */
		dataAccess = &(*position);
		assert(dataAccess != nullptr);
		assert(dataAccess->getAccessRegion().fullyContainedIn(region));

		return dataAccess;
	}

	/*
	 * fragmentUnregisteredAccessObject: Fragment an unregistered access if necessary to match a region.
	 *
	 * The task access structures must not be reachable.
	 *
	 */
	static inline DataAccess *fragmentUnregisteredAccessObject(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures)
	{
		assert(!dataAccess->hasBeenDiscounted());
		assert(!dataAccess->isRegistered());
		assert(dataAccess->getObjectType() == access_type);

		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return dataAccess;
		}

		TaskDataAccesses::accesses_t::iterator position =
			accessStructures._accesses.iterator_to(*dataAccess);
		position = accessStructures._accesses.fragmentByIntersection(
			position, region,
			/* removeIntersection */ false,
			/* duplicator */
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(!toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			/* postprocessor */
			[&](DataAccess *fragment, DataAccess *originalDataAccess) {
				fragment->setUpNewFragment(originalDataAccess->getInstrumentationId());
			});

		/*
		 * Return the part of this access that is fully inside the given region
		 */
		dataAccess = &(*position);
		assert(dataAccess != nullptr);
		assert(dataAccess->getAccessRegion().fullyContainedIn(region));

		return dataAccess;
	}


	static inline DataAccess *fragmentFragmentObject(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures)
	{
		assert(!dataAccess->hasBeenDiscounted());
		assert(dataAccess->getObjectType() == fragment_type);

		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return dataAccess;
		}

		TaskDataAccesses::access_fragments_t::iterator position =
			accessStructures._accessFragments.iterator_to(*dataAccess);
		position = accessStructures._accessFragments.fragmentByIntersection(
			position, region,
			/* removeIntersection */ false,
			/* duplicator */
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			/* postprocessor */
			[&](DataAccess *fragment, DataAccess *originalDataAccess) {
				setUpNewFragment(fragment, originalDataAccess, accessStructures);
			});

		/*
		 * Return the part of this fragment that is fully inside the given region
		 */
		dataAccess = &(*position);
		assert(dataAccess != nullptr);
		assert(dataAccess->getAccessRegion().fullyContainedIn(region));

		return dataAccess;
	}


	static inline DataAccess *fragmentTaskwaitFragmentObject(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures)
	{
		assert(!dataAccess->hasBeenDiscounted());
		assert((dataAccess->getObjectType() == taskwait_type) || (dataAccess->getObjectType() == top_level_sink_type));

		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment
			return dataAccess;
		}

		TaskDataAccesses::taskwait_fragments_t::iterator position =
			accessStructures._taskwaitFragments.iterator_to(*dataAccess);
		position = accessStructures._taskwaitFragments.fragmentByIntersection(
			position, region,
			/* removeIntersection */ false,
			/* duplicator */
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			/* postprocessor */
			[&](DataAccess *fragment, DataAccess *originalDataAccess) {
				setUpNewFragment(fragment, originalDataAccess, accessStructures);
			});

		/*
		 * Return the part of this taskwait fragment that is fully inside the given region
		 */
		dataAccess = &(*position);
		assert(dataAccess != nullptr);
		assert(dataAccess->getAccessRegion().fullyContainedIn(region));

		return dataAccess;
	}


	/*
	 * fragmentAccess: Fragment a data access, fragment or taskwait as
	 * necessary to match a region.
	 *
	 * The task access structures must be either locked or not reachable.
	 *
	 * This function should be called inside one of the processors for the
	 * access structures (e.g. processAll, processIntersecting, ...). If
	 * fragmentation is necessary, then the this function will split the
	 * access/fragment/taskwait into multiple parts. The return value will be
	 * the first part; the other parts will be visited later by the iterator
	 * and processed by a subsequent call to the supplied lambda function.
	 */
	static inline DataAccess *fragmentAccess(
		DataAccess *dataAccess, DataAccessRegion const &region,
		TaskDataAccesses &accessStructures)
	{
		assert(dataAccess != nullptr);
		// assert(accessStructures._lock.isLockedByThisThread()); // Not necessary when fragmenting an access that is not reachable
		// assert(accessStructures._lock.isLockedByThisThread() || noAccessIsReachable(accessStructures));
		assert(&dataAccess->getOriginator()->getDataAccesses() == &accessStructures);
		assert(!accessStructures.hasBeenDeleted());

		// This following assert did once fail, when called indirectly from
		// unregisterTaskDataAccesses => ... => processUpdateOperation => ...
		// (but only with two runtimes on a node):
		// salloc -q debug -c 48 -n 2 -t 01:00:00
		// ./nasty.py --nodes 2 --tasks 40 --nesting 4 --seed 135
		// mcc -fsanitize=address -fno-omit-frame-pointer -ggdb -o nasty --ompss-2 nasty.c
		// mpirun -np 4 ./nasty
		// assert(!dataAccess->hasBeenDiscounted());
#ifndef NDEBUG
		if (dataAccess->hasBeenDiscounted()) {
			std::cerr << "Warning: dataAccess->hasBeenDiscounted at " << __FILE__ << ":" << __LINE__
				<< " task: " << dataAccess->getOriginator()->getLabel() << std::endl;
		}
#endif

		if (dataAccess->getAccessRegion().fullyContainedIn(region)) {
			// Nothing to fragment: this task access/fragment is fully contained inside the region
			return dataAccess;
		}

		if (dataAccess->getObjectType() == access_type) {
			return fragmentAccessObject(dataAccess, region, accessStructures);
		} else if (dataAccess->getObjectType() == fragment_type) {
			return fragmentFragmentObject(dataAccess, region, accessStructures);
		} else {
			assert((dataAccess->getObjectType() == taskwait_type) || (dataAccess->getObjectType() == top_level_sink_type));
			return fragmentTaskwaitFragmentObject(dataAccess, region, accessStructures);
		}
	}

	static inline void processAccessInfo(/* INOUT */ CPUDependencyData &hpDependencyData)
	{
		if (!hpDependencyData._accessInfoMap.empty()) {
			for (auto &it : hpDependencyData._accessInfoMap) {
				ClusterNode *node = it.first;
				std::vector<TaskOffloading::AccessInfo> &accessInfoVector = it.second;
				TaskOffloading::sendAccessInfo(node, accessInfoVector);
			}
			hpDependencyData._accessInfoMap.clear();
		}
	}


	static inline void processSatisfiedCommutativeOriginators(/* INOUT */ CPUDependencyData &hpDependencyData)
	{
		if (!hpDependencyData._satisfiedCommutativeOriginators.empty()) {
			CommutativeScoreboard::_lock.lock();
			for (Task *satisfiedCommutativeOriginator : hpDependencyData._satisfiedCommutativeOriginators) {
				assert(satisfiedCommutativeOriginator != 0);

				bool acquiredCommutativeSlots =
					CommutativeScoreboard::addAndEvaluateTask(satisfiedCommutativeOriginator, hpDependencyData);
				if (acquiredCommutativeSlots) {
					hpDependencyData._satisfiedOriginators.push_back(satisfiedCommutativeOriginator);
				}
			}
			CommutativeScoreboard::_lock.unlock();

			hpDependencyData._satisfiedCommutativeOriginators.clear();
		}
	}


	//! Process all the originators that have become ready
	static inline void processSatisfiedOriginators(
		/* INOUT */ CPUDependencyData &hpDependencyData,
		ComputePlace *computePlace,
		bool fromBusyThread)
	{
		processSatisfiedCommutativeOriginators(hpDependencyData);

		// NOTE: This is done without the lock held and may be slow since it can enter the scheduler
		for (Task *satisfiedOriginator : hpDependencyData._satisfiedOriginators) {
			assert(satisfiedOriginator != 0);

			ComputePlace *computePlaceHint = nullptr;
			if (computePlace != nullptr) {
				if (computePlace->getType() == satisfiedOriginator->getDeviceType()) {
					computePlaceHint = computePlace;
				}
			}

			ReadyTaskHint schedulingHint = SIBLING_TASK_HINT;
			if (fromBusyThread || !computePlaceHint || !computePlaceHint->isOwned()) {
				schedulingHint = BUSY_COMPUTE_PLACE_TASK_HINT;
			}

			Scheduler::addReadyTask(satisfiedOriginator, computePlaceHint, schedulingHint);
		}

		hpDependencyData._satisfiedOriginators.clear();
	}


	static void applyUpdateOperationOnAccess(
		UpdateOperation const &updateOperation,
		DataAccess *access, TaskDataAccesses &accessStructures,
		/* OUT */ CPUDependencyData &hpDependencyData)
	{
		// Fragment if necessary
		access = fragmentAccess(access, updateOperation._region, accessStructures);
		assert(access != nullptr);

		DataAccessStatusEffects initialStatus(access);

		// Calculate the valid namespace.
		if (updateOperation._validNamespace != VALID_NAMESPACE_UNKNOWN) {
			// Must not receive namespace information more than once. Propagating it
			// more than once may result in a use-after-free.
			// assert(access->getValidNamespacePrevious() == VALID_NAMESPACE_UNKNOWN);
			if (access->getType() == CONCURRENT_ACCESS_TYPE
				|| access->getType() == COMMUTATIVE_ACCESS_TYPE) {
				// Do not support namespace propagation into a concurrent or commutative access
				// This is for simplicity as both are currently synchronized on the node on which
				// the tasks are created. But it might be worth figuring out if and how namespace
				// propagation of these accesses could work. We have a similar condition to
				// disable namespace propagation out of these accesses (in the calculation
				// of updateOperation._validNamespace).
				access->setValidNamespacePrevious(
					VALID_NAMESPACE_NONE,
					OffloadedTaskIdManager::InvalidOffloadedTaskId
				);
			} else {
				// Can only propagate in to in or non-in to non-in
				// NO_ACCESS_TYPE is used by propagateSatisfiability to reproduce the namespace
				// previous on the remote node (which is OK as it was OK on the offloader node).
				if (updateOperation._namespaceAccessType == NO_ACCESS_TYPE
					// Not read access to non-read
					|| !((updateOperation._namespaceAccessType == READ_ACCESS_TYPE)
							&& (access->getType() != READ_ACCESS_TYPE))) {
					access->setValidNamespacePrevious(
						updateOperation._validNamespace,
						updateOperation._namespacePredecessor
					);
				} else {
					access->setValidNamespacePrevious(VALID_NAMESPACE_NONE, access->getOriginator()->getOffloadedTaskId());
				}
			}
		}

		// When an access is propagated in the namespace, we need to be careful in the
		// setting of read (R), write (W), and concurrent/commutative (C) satisfiability.
		// There are three cases:
		//
		//   (a) non-namespace
		//
		//          Access       For a non-namespace access, R, W and C satisfiability
		//            |          are of course propagated from the previous access.
		//            | R,W,C    The arrow shows the access's "next" field.
		//            v
		//          Access
		//
		//   (b) Namespace non-in => non-in
		//
		//          Non-in       For a namespace non-in to non-in access (e.g. inout to
		//            |          inout), R, W and C	satisfiability are propagated from the
		//            | R,W,C    previous in the remote namespace. There must be no
		//            v          satisfiability messages (which could arrive after the task
		//          Non-in       has been deleted).
		//
		//   (c) Namespace non-in => in
		//
		//          Access       For a namespace access to an in access (e.g. inout to in or
		//            |          in to in), R and C	satisfiability are propagated from the
		//            | R,C      previous in the remote namespace. This allows the "in"
		//   W        v          in access to quickly become satisfied. But the offloader does
		// ------>    in         not track which of potentially multiple remote nodes are able
		//                       to propagate in the remote namespace, so it sends R and W
		//                       satisfiability to all of them. We ignore the R satisfiability
		//                       in the message but take the W satisfiability. This ensures that
		//                       the task cannot have been deleted when the message arrives.
		if (access->getPropagateFromNamespace()) {
			// If propagated into this access in the remote namespace, then... 
			if (updateOperation._propagateSatisfiability) {
				// (1) Normally, only read accesses get read and write satisfiability (note:
				// the setting of the node namespace predecessor is a different
				// type of satisfiability operation, which is allowed). The only exception
				// is if the access is the immediate successor of an auto access of an
				// offloaded task that is actually read-only, so it sends back a message to the
				// offloader to propagate read satisfiability. But after doing so, a successor
				// gets offloaded to the same node with propagation in the namespace
				if (access->getType() != READ_ACCESS_TYPE) {
					assert(!updateOperation._makeWriteSatisfied);
				}
			} else {
				// and (2) Read accesses never get write satisfiability in the namespace (case c)
				if (access->getType() == READ_ACCESS_TYPE) {
					assert(!updateOperation._makeWriteSatisfied);
				}
			}
		}

		if ( !(access->getPropagateFromNamespace()
				&& updateOperation._propagateSatisfiability)) {

			if (updateOperation._makeReadSatisfied) {
				if (access->readSatisfied()) {
					// Actually there are other circumstances when this happens (TBD)
					// assert(access->getOriginator()->isRemoteTask());
				} else {
					bool updateLocation;
					if (access->getObjectType() == access_type
						&& access->getType() == CONCURRENT_ACCESS_TYPE) {
						// Special handling for concurrent accesses
						if (updateOperation._location != access->getConcurrentInitialLocation()) {
							// Either this is not the same as the original location, so somebody must have
							// written to it on a different node, or this is the first concurrent task, which
							// immediately gets read satisfiability. In either case update the location.
							updateLocation = true;
						} else {
							// The location is the same as the original location. Either (a) it hasn't been
							// written to by any of the tasks earlier in sequential order, or (b) it has been
							// written to, but on the same node as the original location. Don't update the
							// location because in case (a) maybe it was this task that did the write and
							// already updated the location to the correct one and in case (b) there is no
							// change to make.
							updateLocation = false;
						}
					} else {
						updateLocation = true;
					}

					if (access->isStrongLocalAccess()) {
						access->setReadSatisfied(access->getLocation());
					} else if ((access->getType() == AUTO_ACCESS_TYPE)
						&& updateOperation._location == nullptr
						&& !access->hasLocation()) {
						access->setReadSatisfied(Directory::getDirectoryMemoryPlace());
					} else if (updateLocation) {
						access->setReadSatisfied(updateOperation._location);
						access->setWriteID(updateOperation._writeID);
					} else {
						access->setReadSatisfied(access->getLocation());
					}
				}

				if ((access->getObjectType() == taskwait_type  || access->getObjectType() == top_level_sink_type)
					&& ClusterManager::autoOptimizeReadOnly()
					&& access->getType() == AUTO_ACCESS_TYPE
					&& access->getOriginator()->isRemoteTask()
					&& access->getOriginator()->hasFinished() // and not a taskwait in the middle of the task! TODO: check this also
					&& !updateOperation._makeWriteSatisfied
					&& !access->writeSatisfied()) {
						// The taskwait gets read satisfiability before write satisfiability.
						// So the *last* subtask that accessed the data was read-only. Maybe
						// an earlier subtask did modify it though. Check the write ID to
						// see if it is still the same as the original access. If so, this
						// task and its subtasks will not modified the data.
						bool modified = false;
						accessStructures._accesses.processIntersecting(
							access->getAccessRegion(),
							[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
								DataAccess *originalAccess = &(*position);
								if (originalAccess->isAutoHasBeenAccessed()
									|| originalAccess->getWriteID() != access->getWriteID()) {
									modified = true;
									return false;
								}
								return true;
							}
						);

						if (!modified) {
							// Note: [READONLY_ALLMEMORY] we cannot rely on the
							// access type being precisely right when a subtask's
							// access (perhaps an auto access) covers multiple
							// (parent) task accesses. This happens when the parent
							// task has more precise dependencies than the subtask,
							// so it is hopefully not the common case.  So there
							// may be some spurious ReadOnly hint messages that
							// will be ignored at the other end.
							ClusterNode *node = access->getOriginator()->getClusterContext()->getRemoteNode();
							hpDependencyData._accessInfoMap[node].emplace_back(
									access->getAccessRegion(),
									access->getOriginator()->getOffloadedTaskId(),
									/* noEagerSend */ false,
									/* isReadOnly */ true,
									access->getOriginator());
						}
					}
			}

			/*
			 * Write Satisfiability.
			 * NOTE: although normally an access becomes read satisified before
			 * (or at the same time as) it becomes write satisfied, it is valid
			 * for the write satisfiability to arrive first. This reordering
			 * happens for example due to the race between setting
			 * _make{Read/Write}Satisfied and calling
			 * applyUpdateOperationOnAccess as a delayed operation.
			 */

			if (updateOperation._makeWriteSatisfied) {

				if (!access->getPropagateFromNamespace() || access->getType() != READ_ACCESS_TYPE) {
					// not if it is a read access from the namespace; in this case we take (pseudo)write
					// satisfiability from the satisfiability messages
					access->setWriteSatisfied();
				}

				if (updateOperation._previousIsCommutative
					&& (access->getType() != COMMUTATIVE_ACCESS_TYPE)) {
					// This is a normal access following one or more commutative accesses to the same region.
					// Delete the scoreboard entry.
					CommutativeScoreboard::_lock.lock();
					CommutativeScoreboard::endCommutative(access->getAccessRegion());
					CommutativeScoreboard::_lock.unlock();
				}
			}

			bool propagateSatisfiabilityMakesConcurrentAndCommutative
					= updateOperation._propagateSatisfiability
					&& access->readSatisfied()
					&& access->writeSatisfied();

			// Concurrent Satisfiability
			if (updateOperation._makeConcurrentSatisfied
				|| propagateSatisfiabilityMakesConcurrentAndCommutative) {
				access->setConcurrentSatisfied();
				if (access->getType() != AUTO_ACCESS_TYPE) {
					assert(updateOperation._location);
				}
				if (access->getType() == CONCURRENT_ACCESS_TYPE) {
					access->setLocation(updateOperation._location);
					access->setWriteID(updateOperation._writeID);
					access->setConcurrentInitialLocation(updateOperation._location);
				}
			}

			// Commutative Satisfiability
			if (updateOperation._makeCommutativeSatisfied
				|| propagateSatisfiabilityMakesConcurrentAndCommutative) {
				access->setCommutativeSatisfied();
				if (access->getType() != AUTO_ACCESS_TYPE) {
					assert(updateOperation._location);
				}
				if (!access->hasLocation()) {
					access->setLocation(updateOperation._location);
					access->setWriteID(updateOperation._writeID);
				}
			}
		} else {
			// If it is propagated in the namespace, we should only
			// get write satisfiability if it is actually pseudowrite
			// satisfiability for a read-only access. In this case it
			// should still be ignored.
			if (updateOperation._makeWriteSatisfied) {
				assert(access->getType() == READ_ACCESS_TYPE);
				assert(!access->writeSatisfied());
				access->setWriteSatisfied();
			}

			// Also, commutative satisfiability for commutative accesses
			// should never be in the namespace to begin with.
			if (updateOperation._makeCommutativeSatisfied) {
				assert(access->getType() != COMMUTATIVE_ACCESS_TYPE);
			}
		}

		if (updateOperation._setPropagateFromNamespace) {
			access->setPropagateFromNamespace();
		}

		// ReductionInfo
		if (updateOperation._setReductionInfo) {
			access->setPreviousReductionInfo(updateOperation._reductionInfo);

			// ReductionInfo can be already assigned for partially overlapping reductions
			if (access->getReductionInfo() != nullptr) {
				assert(access->getType() == REDUCTION_ACCESS_TYPE);
				assert(access->allocatedReductionInfo());
			} else if ((access->getType() == REDUCTION_ACCESS_TYPE)
					   && (updateOperation._reductionInfo != nullptr)
					   && (access->getReductionTypeAndOperatorIndex() == updateOperation._reductionInfo->getTypeAndOperatorIndex())) {
				// Received compatible ReductionInfo
				access->setReductionInfo(updateOperation._reductionInfo);

				Instrument::receivedCompatibleReductionInfo(
					access->getInstrumentationId(),
					*updateOperation._reductionInfo);
			}

			if (!access->receivedReductionInfo()) {
				// It is possible now to receive the reduction information more than once, when the
				// previous task is offloaded and it has an auto access that is only read-only.
				// We get the reduction info set when our access is finalized. We get it again when
				// the previous access completes. TODO: fix this.
				access->setReceivedReductionInfo();
			}
		}

		// ReductionSlotSet
		if (updateOperation._reductionSlotSet.size() > 0) {
			assert((access->getObjectType() == access_type) || (access->getObjectType() == fragment_type) || (access->getObjectType() == taskwait_type));
			assert(access->getType() == REDUCTION_ACCESS_TYPE);
			assert(access->getReductionSlotSet().size() == updateOperation._reductionSlotSet.size());

			access->getReductionSlotSet() |= updateOperation._reductionSlotSet;
			access->setReceivedReductionSlotSet();
		}

		DataAccessStatusEffects updatedStatus(access);

		handleDataAccessStatusChanges(
			initialStatus, updatedStatus,
			access, accessStructures, updateOperation._target._task,
			hpDependencyData);
	}

	/*
	 * Process an update operation.
	 *
	 * The lock must already be taken on the target task's access structures. The
	 * target task is updateOperation._target._task.
	 */
	static void processUpdateOperation(
		UpdateOperation const &updateOperation,
		/* OUT */ CPUDependencyData &hpDependencyData)
	{
		assert(!updateOperation.empty());

		TaskDataAccesses &accessStructures = updateOperation._target._task->getDataAccesses();

		/* Check lock on access structures already taken by caller */
		// assert(accessStructures._lock.isLockedByThisThread());

		if (updateOperation._target._objectType == access_type) {
			// Update operation for accesses
			accessStructures._accesses.processIntersecting(
				updateOperation._region,
				[&](TaskDataAccesses::accesses_t::iterator accessPosition) -> bool {
					DataAccess *access = &(*accessPosition);

					applyUpdateOperationOnAccess(updateOperation, access, accessStructures, hpDependencyData);

					return true;
				});
		} else if (updateOperation._target._objectType == fragment_type) {
			// Update operation for fragments
			accessStructures._accessFragments.processIntersecting(
				updateOperation._region,
				[&](TaskDataAccesses::access_fragments_t::iterator fragmentPosition) -> bool {
					DataAccess *fragment = &(*fragmentPosition);

					applyUpdateOperationOnAccess(updateOperation, fragment, accessStructures, hpDependencyData);

					return true;
				});
		} else {
			// Update operation for taskwait Fragments
			assert((updateOperation._target._objectType == taskwait_type) || (updateOperation._target._objectType == top_level_sink_type));
			accessStructures._taskwaitFragments.processIntersecting(
				updateOperation._region,
				[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
					DataAccess *taskwaitFragment = &(*position);

					applyUpdateOperationOnAccess(updateOperation, taskwaitFragment, accessStructures, hpDependencyData);

					return true;
				});
		}
	}

	// Process all delayed operations that relate to access that are
	// not in a different offloaded task.
	static inline void processDelayedOperationsSameTask(
		/* INOUT */ CPUDependencyData &hpDependencyData,
					Task *task)
	{
		Task *lastLocked = task;
		const Task *myOffloadedTask = task->getOffloadedPredecesor();

#ifndef NDEBUG
		if (task->hasDataReleaseStep()) {
			assert(task == myOffloadedTask);
		}
#endif // NDEBUG

		// assert(task->getDataAccesses()._lock.isLockedByThisThread());

		for (auto it = hpDependencyData._delayedOperations.begin();
		     it != hpDependencyData._delayedOperations.end();) {
			UpdateOperation &delayedOperation = *it;

			const Task *targetOffloadedTask = delayedOperation._target._task->getOffloadedPredecesor();

			// targetOffloadedTask must be null when all the tasks are local.
			assert(ClusterManager::inClusterMode() || targetOffloadedTask == nullptr);

			// Process the delayed operation if there are in the same offloaded
			// task (or a descendent of the same one), OR neither of them is 
			// the descendent of an offloaded task
			assert( ((myOffloadedTask == nullptr) && (targetOffloadedTask == nullptr))
					|| ((myOffloadedTask != nullptr) && (targetOffloadedTask != nullptr)) );

			if (myOffloadedTask == targetOffloadedTask) {

				if (delayedOperation._target._task != lastLocked) {
					if (lastLocked != nullptr) {
						lastLocked->getDataAccesses()._lock.unlock();
					}
					lastLocked = delayedOperation._target._task;
					lastLocked->getDataAccesses()._lock.lock();
				}
				// Process the delayed operation
				processUpdateOperation(delayedOperation, hpDependencyData);

				// Advance the iterator
				auto oldIterator = it;
				it++;
				hpDependencyData._delayedOperations.erase(oldIterator);
			} else {
				assert(ClusterManager::inClusterMode());
				it++;
			}
		}

		// if handleDataAccessStatusChanges added some accesses to release
		// then We release all of them together
		assert(!task->hasDataReleaseStep()
			|| (task->hasDataReleaseStep() && task == myOffloadedTask));

		if (lastLocked != nullptr) {
			lastLocked->getDataAccesses()._lock.unlock();
		}

		assert(ClusterManager::inClusterMode() || hpDependencyData._delayedOperations.empty());
		processAccessInfo(hpDependencyData);
	}


	/*
	 * Process the delayed operations. These are operations that are triggered
	 * by handleDataAccessStatusChanges. A lock was already taken on a task's
	 * access structures before calling handleDataAccessStatusChanges. Since
	 * these delayed operations require taking a lock on a different task's
	 * access structures, they couldn't have been done at the time without
	 * risking a deadlock.
	 */
	static inline void processDelayedOperations(
		/* INOUT */ CPUDependencyData &hpDependencyData)
	{
		Task *lastLocked = nullptr;

		while (!hpDependencyData._delayedOperations.empty()) {
			UpdateOperation &delayedOperation = hpDependencyData._delayedOperations.front();

			assert(delayedOperation._target._task != nullptr);
			if (delayedOperation._target._task != lastLocked) {
				if (lastLocked != nullptr) {
					lastLocked->getDataAccesses()._lock.unlock();
				}
				lastLocked = delayedOperation._target._task;
				lastLocked->getDataAccesses()._lock.lock();
			}

			processUpdateOperation(delayedOperation, hpDependencyData);

			hpDependencyData._delayedOperations.pop_front();
		}

		if (lastLocked != nullptr) {
			lastLocked->getDataAccesses()._lock.unlock();
		}
		processAccessInfo(hpDependencyData);
	}


	static inline void processReleasedCommutativeRegions(
		/* INOUT */ CPUDependencyData &hpDependencyData)
	{
		if (!hpDependencyData._releasedCommutativeRegions.empty()) {
			CommutativeScoreboard::_lock.lock();
			CommutativeScoreboard::processReleasedCommutativeRegions(hpDependencyData);
			CommutativeScoreboard::_lock.unlock();
		}
	}


	void processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
		CPUDependencyData &hpDependencyData,
		ComputePlace *computePlace,
		bool fromBusyThread
	) {
		Instrument::enterProcessDelayedOperationsSatisfiedOriginatorsAndRemovableTasks();

		processReleasedCommutativeRegions(hpDependencyData);

#if NO_DEPENDENCY_DELAYED_OPERATIONS
#else
		processDelayedOperations(hpDependencyData);   // Most of the time is here
#endif

		handleCompletedTaskwaits(hpDependencyData, computePlace);

#if NO_DEPENDENCY_DELAYED_OPERATIONS
#else
		processDelayedOperations(hpDependencyData);
#endif

		processSatisfiedOriginators(hpDependencyData, computePlace, fromBusyThread);
		assert(hpDependencyData._satisfiedOriginators.empty());

#ifdef USE_CLUSTER
		TaskOffloading::sendSatisfiabilityAndDataSends(hpDependencyData._satisfiabilityMap, hpDependencyData._dataSendRegionInfoMap);
#endif // USE_CLUSTER

		handleRemovableTasks(hpDependencyData._removableTasks);

		removeFromNamespaceBottomMap(hpDependencyData);
		Instrument::exitProcessDelayedOperationsSatisfiedOriginatorsAndRemovableTasks();
	}


	/*
	 * Create an initial fragment due to an access of a newly-submitted child
	 * task that was not yet in the bottom map, but is covered by the parent
	 * task's accesses. The parent task's fragments correspond to child task
	 * accesses (after fragmentation) within the parent's accesses. This method
	 * creates the new fragment, but not the bottom map entry corresponding to
	 * it.
	 */
	static inline DataAccess *createInitialFragment(
		TaskDataAccesses::accesses_t::iterator accessPosition,
		TaskDataAccesses &accessStructures,       /* Access structures for the parent task */
		DataAccessRegion subregion                /* Subregion accessed by the new child task */
	) {
		DataAccess *dataAccess = &(*accessPosition);
		assert(dataAccess != nullptr);
		assert(!accessStructures.hasBeenDeleted());

		assert(!accessStructures._accessFragments.contains(dataAccess->getAccessRegion()));

		Instrument::data_access_id_t instrumentationId =
			Instrument::createdDataSubaccessFragment(dataAccess->getInstrumentationId());
		DataAccess *fragment = ObjectAllocator<DataAccess>::newObject(
			fragment_type,
			dataAccess->getType(),
			dataAccess->isWeak(),
			dataAccess->getOriginator(),
			dataAccess->getAccessRegion(),
			dataAccess->getReductionTypeAndOperatorIndex(),
			dataAccess->getReductionIndex(),
			dataAccess->getLocation(),
			dataAccess->getOutputLocation(),
			dataAccess->getDataLinkStep(),
			instrumentationId);

		fragment->inheritFragmentStatus(dataAccess); //TODO is it necessary?

#ifndef NDEBUG
		fragment->setReachable();
#endif

		// This assertion is wrong: it is in fact possible for write satisfiability
		// to arrive before read satisfiable. This is due to race conditions in the
		// runtime system.
		// assert(fragment->readSatisfied() || !fragment->writeSatisfied());

		accessStructures._accessFragments.insert(*fragment);
		fragment->setInBottomMap();

		// NOTE: This may in the future need to be included in the common status changes code
		dataAccess->setHasSubaccesses();

		if (subregion != dataAccess->getAccessRegion()) {
			dataAccess->getAccessRegion().processIntersectingFragments(
				subregion,
				/* thisOnlyProcessor */
				[&](DataAccessRegion excludedSubregion) {
					BottomMapEntry *bottomMapEntry = ObjectAllocator<BottomMapEntry>::newObject(
						excludedSubregion,
						DataAccessLink(dataAccess->getOriginator(), fragment_type),
						dataAccess->getType(),
						dataAccess->getReductionTypeAndOperatorIndex());
					accessStructures._subaccessBottomMap.insert(*bottomMapEntry);
				},
				/* intersectingProcessor */
				[&](__attribute__((unused)) DataAccessRegion intersection) {
				},
				/* otherOnlyProcessor */
				[&](__attribute__((unused)) DataAccessRegion unmatchedRegion) {
					// This part is not covered by the access
				});
		}

		return fragment;
	}

	/*
	 * Fragment the linked object (access, fragment or taskwait) against the
	 * given region, and call the supplied function on the fragment of the
	 * access fully contained inside the region. It needs to check the type of
	 * the object and fragment it in the appropriate way (using the correct
	 * function to fragment it and correct processor to iterate over the
	 * corresponding list.
	 */
	template <typename ProcessorType>
	static inline bool followLink(
		DataAccessLink const &link,
		DataAccessRegion const &region,
		ProcessorType processor)
	{
		Task *task = link._task;
		assert(task != nullptr);

		TaskDataAccesses &accessStructures = task->getDataAccesses();
		// assert(accessStructures._lock.isLockedByThisThread());

		if (link._objectType == access_type) {
			/*
			 * An access, iterate over accessStructures._accesses and
			 * fragment using fragmentAccessObject.
			 */
			return accessStructures._accesses.processIntersecting(
				region,
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *access = &(*position);
					assert(!access->hasBeenDiscounted());

					/* Fragment it */
					access = fragmentAccessObject(access, region, accessStructures);

					/* call the processor on each access fragment */
					return processor(access);
				});
		} else if (link._objectType == fragment_type) {
			/*
			 * An fragment, iterate over accessStructures._accessFragments and
			 * fragment using fragmentFragmentObject.
			 */
			return accessStructures._accessFragments.processIntersecting(
				region,
				[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
					DataAccess *access = &(*position);
					assert(!access->hasBeenDiscounted());

					/* Fragment it */
					access = fragmentFragmentObject(access, region, accessStructures);

					/* call the processor on each fragment fragment */
					return processor(access);
				});
		} else {
			/*
			 * A taskwait fragment, iterate over accessStructures._taskwaitFragments and
			 * fragment using fragmentTaskwaitFragmentObject.
			 */
			assert((link._objectType == taskwait_type) || (link._objectType == top_level_sink_type));
			return accessStructures._taskwaitFragments.processIntersecting(
				region,
				[&](TaskDataAccesses::taskwait_fragments_t::iterator position) -> bool {
					DataAccess *access = &(*position);
					assert(!access->hasBeenDiscounted());

					/* Fragment it */
					access = fragmentTaskwaitFragmentObject(access, region, accessStructures);

					/* call the processor on each taskwait fragment */
					return processor(access);
				});
		}
	}

/*
 * Matching processor used to put a new task's data access into the bottom map.
 */
	template <typename MatchingProcessorType, typename MissingProcessorType>
	static inline bool foreachBottomMapMatchPossiblyCreatingInitialFragmentsAndMissingRegion(
		Task *parent, TaskDataAccesses &parentAccessStructures,
		DataAccessRegion region,
		MatchingProcessorType matchingProcessor, MissingProcessorType missingProcessor)
	{
		assert(parent != nullptr);
		assert((&parentAccessStructures) == (&parent->getDataAccesses()));
		assert(!parentAccessStructures.hasBeenDeleted());

		return parentAccessStructures._subaccessBottomMap.processIntersectingAndMissing(
			region,

			/*
			 * A region of the new task's data access is already in the bottom map.
			 */
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);

				DataAccessRegion subregion = region.intersect(bottomMapEntry->getAccessRegion());
				BottomMapEntryContents bmeContents = *bottomMapEntry;

				DataAccessLink target = bmeContents._link;
				assert(target._task != nullptr);

				bool result = true;
				if (target._task != parent) {
					TaskDataAccesses &subtaskAccessStructures = target._task->getDataAccesses();

					subtaskAccessStructures._lock.lock();

					// For each access of the subtask (predecessor on the bottom map) that matches
					result = followLink(
						target, subregion,
						[&](DataAccess *previous) -> bool {
							assert(!previous->hasNext());
							assert(previous->isInBottomMap());

							return matchingProcessor(previous, bmeContents);
						});

					subtaskAccessStructures._lock.unlock();
				} else {
					// A fragment
					assert(target._objectType == fragment_type);

					// For each fragment of the parent that matches
					result = followLink(
						target, subregion,
						[&](DataAccess *previous) -> bool {
							assert(!previous->hasNext());
							assert(previous->isInBottomMap());

							return matchingProcessor(previous, bmeContents);
						});
				}

				bottomMapEntry = fragmentBottomMapEntry(bottomMapEntry, subregion, parentAccessStructures);

				/*
				 * The (first part of) the bottom map entry has been fully
				 * handled, and it will be covered by the new bottom map entry
				 * for the new task. So we can delete the old bottom map entry
				 * now.
				 */
				parentAccessStructures._subaccessBottomMap.erase(*bottomMapEntry);
				ObjectAllocator<BottomMapEntry>::deleteObject(bottomMapEntry);

				return result;
			},

			/*
			 * A region of the new task's data access that is not (yet) in the bottom map.
			 * Iterate over the parent's accesses and divide into those parts that intersect
			 * parent accesses and those that do not.
			 */
			[&](DataAccessRegion missingRegion) -> bool {
				parentAccessStructures._accesses.processIntersectingAndMissing(
					missingRegion,
					/*
					 * intersectingProcessor: it's not in the bottom map yet, but it
					 * is part of one of the parent task's accesses. Create a new
					 * bottom map entry and now that it exists, give it to the
					 * matching processor.
					 */
					[&](TaskDataAccesses::accesses_t::iterator superaccessPosition) -> bool {
						DataAccessStatusEffects initialStatus;

						/* Create an initial fragment */
						DataAccess *previous = createInitialFragment(
							superaccessPosition, parentAccessStructures,
							missingRegion);
						assert(previous != nullptr);
						assert(previous->getObjectType() == fragment_type);

						previous->setRegistered(); /* register it immediately */

						DataAccessStatusEffects updatedStatus(previous);

						BottomMapEntryContents bmeContents(
							DataAccessLink(parent, fragment_type),
							previous->getType(),
							previous->getReductionTypeAndOperatorIndex());

						{
							CPUDependencyData hpDependencyData;
							handleDataAccessStatusChanges(
								initialStatus, updatedStatus,
								previous, parentAccessStructures, parent,
								hpDependencyData);
							assert(hpDependencyData.empty());
						}

						previous = fragmentAccess(previous, missingRegion, parentAccessStructures);

						/*
						 *Now that the bottom map entry has been created, pass it
						 * to the matching processor
						 */
						return matchingProcessor(previous, bmeContents);
					},

					/*
					 * missingProcessor: the region isn't in the bottom map or
					 * the parent task's accesses. Pass this "hole" up to the
					 * missing processor to deal with.
					 */
					[&](DataAccessRegion regionUncoveredByParent) -> bool {
						return missingProcessor(regionUncoveredByParent);
					});

				/* Keep going, with other regions of the bottom map */
				return true;
			});
	}

	template <typename MatchingProcessorType, typename MissingProcessorType>
	static inline bool foreachBottomMapMatchMissingRegionCreatingInitialFragments(
		Task *parent, TaskDataAccesses &parentAccessStructures,
		DataAccessRegion region,
		MatchingProcessorType matchingProcessor, MissingProcessorType missingProcessor
	) {
		assert(parent != nullptr);
		assert((&parentAccessStructures) == (&parent->getDataAccesses()));
		assert(!parentAccessStructures.hasBeenDeleted());

		return parentAccessStructures._subaccessBottomMap.processIntersectingAndMissing(
			region,

			/*
			 * A region of the new task's data access is already in the bottom map. Do nothing.
			 */
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator ) -> bool {
				return true;
			},

			/*
			 * A region of the new task's data access that is not (yet) in the bottom map.
			 * Iterate over the parent's accesses and divide into those parts that intersect
			 * parent accesses and those that do not.
			 */
			[&](DataAccessRegion missingRegion) -> bool {
				parentAccessStructures._accesses.processIntersectingAndMissing(
					missingRegion,
					/*
					 * intersectingProcessor: it's not in the bottom map yet, but it
					 * is part of one of the parent task's accesses. Create a new
					 * bottom map entry and now that it exists, give it to the
					 * matching processor.
					 */
					[&](TaskDataAccesses::accesses_t::iterator superaccessPosition) -> bool {
						DataAccessStatusEffects initialStatus;

						/* Create an initial fragment */
						DataAccess *previous = createInitialFragment(
							superaccessPosition, parentAccessStructures,
							missingRegion
						);
						assert(previous != nullptr);
						assert(previous->getObjectType() == fragment_type);

						previous->setRegistered(); /* register it immediately */

						DataAccessStatusEffects updatedStatus(previous);

						BottomMapEntryContents bmeContents(
							DataAccessLink(parent, fragment_type),
							previous->getType(),
							previous->getReductionTypeAndOperatorIndex()
						);

						{
							CPUDependencyData hpDependencyData;
							handleDataAccessStatusChanges(
								initialStatus, updatedStatus,
								previous, parentAccessStructures, parent,
								hpDependencyData
							);
							/* There should not be any delayed operations */
							assert(hpDependencyData.empty());
						}

						previous = fragmentAccess(previous, missingRegion, parentAccessStructures);

						/*
						 *Now that the bottom map entry has been created, pass it
						 * to the matching processor
						 */
						return matchingProcessor(previous, bmeContents);
					},

					/*
					 * missingProcessor: the region isn't in the bottom map or
					 * the parent task's accesses. Pass this "hole" up to the
					 * missing processor to deal with.
					 */
					[&](DataAccessRegion regionUncoveredByParent) -> bool {
						return missingProcessor(regionUncoveredByParent);
					}
				);

				/* Keep going, with other regions of the bottom map */
				return true;
			}
		);
	}


	template <typename ProcessorType, typename BottomMapEntryProcessorType>
	static inline void foreachBottomMapMatch(
		DataAccessRegion const &region,
		TaskDataAccesses &accessStructures, Task *task,
		ProcessorType processor,
		BottomMapEntryProcessorType bottomMapEntryProcessor = [](BottomMapEntry *) {})
	{
		assert(!accessStructures.hasBeenDeleted());
		// assert(accessStructures._lock.isLockedByThisThread());

		accessStructures._subaccessBottomMap.processIntersecting(
			region,
			/* processor: called with each part of the bottom map that intersects region */
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);

				DataAccessLink target = bottomMapEntry->_link;
				assert(target._task != nullptr);

				DataAccessRegion subregion = region.intersect(bottomMapEntry->getAccessRegion());

				if (target._task != task) {
					// An access from a subtask

					TaskDataAccesses &subtaskAccessStructures = target._task->getDataAccesses();
					subtaskAccessStructures._lock.lock();

					// For each access of the subtask that matches
					followLink(
						target, subregion,
						[&](DataAccess *subaccess) -> bool {
							assert(subaccess->isReachable());
							assert(subaccess->isInBottomMap());

							processor(subaccess, subtaskAccessStructures, target._task);

							return true;
						});

					subtaskAccessStructures._lock.unlock();
				} else {
					// A fragment from the current task, a taskwait fragment, or a top level sink
					assert(
						(target._objectType == fragment_type)
						|| (target._objectType == taskwait_type)
						|| (target._objectType == top_level_sink_type));

					followLink(
						target, subregion,
						[&](DataAccess *fragment) -> bool {
							assert(fragment->isReachable());
							assert(fragment->isInBottomMap());

							processor(fragment, accessStructures, task);

							return true;
						});
				}

				bottomMapEntryProcessor(bottomMapEntry);

				/* always continue through the bottom map*/
				return true;
			});
	}

#if 0
	template <typename ProcessorType, typename BottomMapEntryProcessorType>
	static inline void foreachBottomMapEntry(
		TaskDataAccesses &accessStructures, Task *task,
		ProcessorType processor,
		BottomMapEntryProcessorType bottomMapEntryProcessor = [](BottomMapEntry *) {})
	{
		assert(!accessStructures.hasBeenDeleted());
		// assert(accessStructures._lock.isLockedByThisThread());

		accessStructures._subaccessBottomMap.processAll(
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);

				DataAccessLink target = bottomMapEntry->_link;
				assert(target._task != nullptr);

				DataAccessRegion const &subregion = bottomMapEntry->getAccessRegion();

				if (target._task != task) {
					// An access from a subtask

					TaskDataAccesses &subtaskAccessStructures = target._task->getDataAccesses();
					subtaskAccessStructures._lock.lock();

					// For each access of the subtask that matches
					followLink(
						target, subregion,
						[&](DataAccess *subaccess) -> bool {
							assert(subaccess->isReachable());
							assert(subaccess->isInBottomMap());

							processor(subaccess, subtaskAccessStructures, target._task);

							return true;
						});

					subtaskAccessStructures._lock.unlock();
				} else {
					// A fragment from the current task
					assert(target._objectType == fragment_type);

					followLink(
						target, subregion,
						[&](DataAccess *fragment) -> bool {
							assert(fragment->isReachable());
							assert(fragment->isInBottomMap());

							processor(fragment, accessStructures, task);

							return true;
						});
				}

				bottomMapEntryProcessor(bottomMapEntry);

				return true;
			});
	}
#endif


	static inline void processBottomMapUpdate(
		BottomMapUpdateOperation &operation,
		TaskDataAccesses &accessStructures, Task *task,
		/* OUT */ CPUDependencyData &hpDependencyData)
	{
		assert(task != nullptr);
		assert(!operation.empty());
		assert(!operation._region.empty());
		assert(!accessStructures.hasBeenDeleted());
		// assert(accessStructures._lock.isLockedByThisThread());

		assert(operation._linkBottomMapAccessesToNext);
		foreachBottomMapMatch(
			operation._region,
			accessStructures, task,
			[&](DataAccess *access, TaskDataAccesses &currentAccessStructures, Task *currentTask) {
				FatalErrorHandler::failIf(
					((operation._parentAccessType == CONCURRENT_ACCESS_TYPE) || (operation._parentAccessType == COMMUTATIVE_ACCESS_TYPE))
						&& access->getType() == REDUCTION_ACCESS_TYPE,
					"Task '",
					(access->getOriginator()->getTaskInfo()->implementations[0].task_label != nullptr) ? access->getOriginator()->getTaskInfo()->implementations[0].task_label : access->getOriginator()->getTaskInfo()->implementations[0].declaration_source,
					"' declares a reduction within a region registered as ",
					(operation._parentAccessType == CONCURRENT_ACCESS_TYPE) ? "concurrent" : "commutative",
					" by task '",
					(task->getTaskInfo()->implementations[0].task_label != nullptr) ? task->getTaskInfo()->implementations[0].task_label : task->getTaskInfo()->implementations[0].declaration_source,
					"' without a taskwait");

				DataAccessStatusEffects initialStatus(access);

				if (operation._inhibitReadSatisfiabilityPropagation) {
					access->unsetCanPropagateReadSatisfiability();
				}

				if (operation._inhibitConcurrentSatisfiabilityPropagation) {
					access->unsetCanPropagateConcurrentSatisfiability();
				}

				if (operation._inhibitCommutativeSatisfiabilityPropagation) {
					access->unsetCanPropagateCommutativeSatisfiability();
				}

				if (operation._inhibitReductionInfoPropagation) {
					access->unsetCanPropagateReductionInfo();
				}

				if (operation._setCloseReduction) {
					// Note: It is currently unsupported that a strong reduction access has
					// subaccesses, as this implies a task-scheduling point.
					// Even if this becomes supported in the future, the following scenario
					// needs to be controlled, possibly by inserting a nested taskwait
					FatalErrorHandler::failIf(
						(operation._parentAccessType == REDUCTION_ACCESS_TYPE) && (access->getType() != REDUCTION_ACCESS_TYPE),
						"Task '",
						(access->getOriginator()->getTaskInfo()->implementations[0].task_label != nullptr) ? access->getOriginator()->getTaskInfo()->implementations[0].task_label : access->getOriginator()->getTaskInfo()->implementations[0].declaration_source,
						"' declares a non-reduction access within a region registered as reduction by task '",
						(task->getTaskInfo()->implementations[0].task_label != nullptr) ? task->getTaskInfo()->implementations[0].task_label : task->getTaskInfo()->implementations[0].declaration_source,
						"'");

					if (access->getType() == REDUCTION_ACCESS_TYPE) {
						access->setClosesReduction();
					}
				}

				assert(!access->hasNext());
				access->setNext(operation._next);
				if (operation._namespaceNextIsIn) {
					access->setNamespaceNextIsIn();
				}

				// We are setting the next of a child task of some task (A) to
				// point to the successor of A.  We need to make sure that
				// the child task does not pass the namespace information, since
				// it must be passed exactly once, and this is done by A itself.
				assert(!access->getPropagatedNamespaceInfo());
				access->setPropagatedNamespaceInfo();

				DataAccessStatusEffects updatedStatus(access);

				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					access, currentAccessStructures, currentTask,
					hpDependencyData);
			},
			[](BottomMapEntry *) {});
	}


	static inline void allocateReductionInfo(DataAccess &dataAccess, const Task &task)
	{
		assert(dataAccess.getType() == REDUCTION_ACCESS_TYPE);

		Instrument::enterAllocateReductionInfo(
			dataAccess.getInstrumentationId(),
			dataAccess.getAccessRegion());

		nanos6_task_info_t *taskInfo = task.getTaskInfo();
		assert(taskInfo != nullptr);

		reduction_index_t reductionIndex = dataAccess.getReductionIndex();

		ReductionInfo *newReductionInfo = ObjectAllocator<ReductionInfo>::newObject(
			dataAccess.getAccessRegion(),
			dataAccess.getReductionTypeAndOperatorIndex(),
			taskInfo->reduction_initializers[reductionIndex],
			taskInfo->reduction_combiners[reductionIndex]);

		// Note: ReceivedReductionInfo flag is not set, as the access will still receive
		// an (invalid) reduction info from the propagation system
		dataAccess.setReductionInfo(newReductionInfo);
		dataAccess.setAllocatedReductionInfo();

		Instrument::exitAllocateReductionInfo(
			dataAccess.getInstrumentationId(),
			*newReductionInfo);
	}

	/*
	 * Update the bottom map with a data access for a new task. This function
	 * is called by linkTaskAccesses.
	 */
	static inline void replaceMatchingInBottomMapLinkAndPropagate(
		DataAccessLink const &next,                /* link to the new task */
		TaskDataAccesses &accessStructures,        /* access structures for the new task */
		DataAccess *dataAccess,                    /* data access for the new task */
		Task *parent,                              /* parent of the new task */
		TaskDataAccesses &parentAccessStructures,  /* access structures for the parent */
		/* inout */ CPUDependencyData &hpDependencyData
	) {
		assert(dataAccess != nullptr);
		assert(parent != nullptr);
		assert(next._task != nullptr);
		assert(!accessStructures.hasBeenDeleted());
		assert(!parentAccessStructures.hasBeenDeleted());

		DataAccessRegion region = dataAccess->getAccessRegion();

		bool hasAllocatedReductionInfo = false;
		ReductionInfo *previousReductionInfo = nullptr;
		Container::vector<DataAccess *> previousReductionAccesses;

		bool local = false;
#ifndef NDEBUG
		bool lastWasLocal = false;
		bool first = true;
#endif

		DataAccessType parentAccessType = NO_ACCESS_TYPE;
		reduction_type_and_operator_index_t parentReductionTypeAndOperatorIndex = no_reduction_type_and_operator;

		/*
		 * Put the new data access (dataAccess) in the bottom map.
		 *
		 * There may be multiple entries in the bottom map that intersect the
		 * new data access ("foreachBottomMapMatch"). The new data access may
		 * alternatively be contained within accesses of the parent task that
		 * are not yet in the bottom map because no child task has accessed
		 * them yet ("PossiblyCreatingInitialFragments").  Finally the new data
		 * access may not be contained within any access of the parent task
		 * ("AndMissingRegion").
		 *
		 * Processing of the first two cases is done by the first big lambda
		 * (matchingProcessor) and processing of the last case is done by the
		 * second big lambda (missingProcessor).
		 */

		foreachBottomMapMatchPossiblyCreatingInitialFragmentsAndMissingRegion(
			parent,
			parentAccessStructures,  /* contains the parent's bottom map */
			region,

			/* matchingProcessor: handle a region (part of the new overall
			 * data access) that is now in the bottom map. This bottom map entry
			 * may have just been created by
			 * "foreachBottomMapMatchPossiblyCreatingInitialFragmentsAndMissingRegion".
			 * The bottom map access is 'previous'. */
			[&](DataAccess *previous, BottomMapEntryContents const &bottomMapEntryContents) -> bool {
				assert(previous != nullptr);
				assert(previous->isReachable());
				assert(!previous->hasBeenDiscounted());
				assert(!previous->hasNext());       /* no next access yet, as it was in the bottom map */

				Task *previousTask = previous->getOriginator();
				assert(previousTask != nullptr);

				parentAccessType = bottomMapEntryContents._accessType;
				parentReductionTypeAndOperatorIndex = bottomMapEntryContents._reductionTypeAndOperatorIndex;
				local = (bottomMapEntryContents._accessType == NO_ACCESS_TYPE);

				/*
				 * Handle reductions
				 */
				if ((dataAccess->getType() == REDUCTION_ACCESS_TYPE) && !hasAllocatedReductionInfo) {
					bool allocatesReductionInfo = false;

					if (previous->getReductionTypeAndOperatorIndex() != dataAccess->getReductionTypeAndOperatorIndex()) {
						// When a reduction access is to be linked with any non-matching access, we want to
						// allocate a new reductionInfo to it before it gets fragmented by propagation operations
						allocatesReductionInfo = true;
					} else {
						if (previousReductionInfo == nullptr) {
							previousReductionInfo = previous->getReductionInfo();
						} else if (previous->getReductionInfo() != previousReductionInfo) {
							// Has multiple previous reductions, need to allocate new reduction info
							allocatesReductionInfo = true;
						}
					}

					if (allocatesReductionInfo) {
						hasAllocatedReductionInfo = true;

						DataAccessStatusEffects initialStatus(dataAccess);
						allocateReductionInfo(*dataAccess, *next._task);
						DataAccessStatusEffects updatedStatus(dataAccess);

						handleDataAccessStatusChanges(
							initialStatus, updatedStatus,
							dataAccess, accessStructures, next._task,
							hpDependencyData);
					}
				}

#ifndef NDEBUG
				if (!first) {
					assert((local == lastWasLocal) && "This fails with wrongly nested regions");
				}
				first = false;
				lastWasLocal = local;
#endif

				// Generate an error if there is a weak non-concurrent access inside a concurrent
				// one. In the following example, d becomes concurrent satisfied twice (and the same
				// for the reduction info).
				//
				// (1) When a becomes concurrent satisfied, it will pass concurrent satisfiability to d
				// (2) When task a finishes, it sets the next of b to d (inhibiting the passing of
				//     concurrent satisfiability)
				// (3) When task b finishes, it sets the next of c to d (NOT inhibiting the passing
				//     of concurrent satisfiability)
				// (4) c passes concurrent satisfiability to d a second time
				//
				// #pragma oss task weakconcurrent(u) node(nanos6_cluster_no_offload) label("a")
				// {
				// 		#pragma oss task weakinout(u) node(nanos6_cluster_no_offload) label("b")
				// 		{
				// 			#pragma oss task inout(u) node(nanos6_cluster_no_offload) label("c")
				// 		    { sleep(1); }
				// 		}
				// 	}
				// #pragma oss task inout(u) label("d")
				// {}
				FatalErrorHandler::failIf(dataAccess->isWeak()
										&& bottomMapEntryContents._accessType == CONCURRENT_ACCESS_TYPE
										&& dataAccess->getType() != bottomMapEntryContents._accessType,
										"Warning: Weak access type ", dataAccessTypeNames[dataAccess->getType()],
										" nested inside access type ", dataAccessTypeNames[bottomMapEntryContents._accessType],
										" for task ", dataAccess->getOriginator()->getLabel(),
										" is not supported");

				TaskDataAccesses &previousAccessStructures = previousTask->getDataAccesses();
				assert(!previousAccessStructures.hasBeenDeleted());
				assert(previous->getAccessRegion().fullyContainedIn(region));

				DataAccessStatusEffects initialStatus(previous);

				// Mark end of reduction
				if (previous->getType() == REDUCTION_ACCESS_TYPE) {
					if (dataAccess->getReductionTypeAndOperatorIndex() != previous->getReductionTypeAndOperatorIndex()) {
						// When any access is to be linked with a non-matching reduction access,
						// we want to mark the preceding reduction access so that it is the
						// last access of its reduction chain
						previous->setClosesReduction();
					} else {
						assert(dataAccess->getType() == REDUCTION_ACCESS_TYPE);
						// When a reduction access is to be linked with a matching reduction
						// access, we don't know whether a ReductionInfo will be allocated yet
						// (it can partially overlap), so we want to keep track of the preceding
						// reduction access so that it can be later marked for closure if needed
						previousReductionAccesses.push_back(previous);
					}
				}
				/*
				 * Finished handling of reductions
				 */

				/*
				 * Link the dataAccess and unset
				 */
#ifdef USE_CLUSTER

				bool canPropagateInNamespace = false;
				if (parent->isNodeNamespace()) {
					if (previous->getDataReleased()) {
						Instrument::namespacePropagation(Instrument::NamespacePredecessorFinished, dataAccess->getAccessRegion());
					} else if ((dataAccess->getNamespacePredecessor() == previous->getOriginator()->getOffloadedTaskId())
						|| ((dataAccess->getNamespacePredecessor() != OffloadedTaskIdManager::InvalidOffloadedTaskId)
							&& (dataAccess->getNamespacePredecessor() == previous->getNamespacePredecessor()))) {

						// We should never connect in the namespace from a read access to a non-read access.
						// In the usual case, this is prevented by the offloader, which is responsible
						// for serializing the reads before passing satisfiability to the non-read. But
						// it can also happen in a rare convoluted case:
						// (1) Task A has a weakinout, and it offloads (Sub)Task A1 with inout to node 1
						//     The predecessor of A1 is task A (this seems wrong?)
						// (2) Task B is successor of Task A and it has a weakin. It is offloaded to node 1
						//     The predecessor of B is also task A.
						// (3) Tasks A1 and B arrive in the wrong order at node 1, i.e. B then A1, and it
						//     looks like we can connect in the namespace from B to A1, but we cannot (due
						//     to the clear dependence from A1's inout to B's in and the fact that in to
						//     inout is never allowed to propagate in the namespace)
						canPropagateInNamespace = true;
						if ((previous->getType() == READ_ACCESS_TYPE) && (dataAccess->getType() != READ_ACCESS_TYPE)) {
							canPropagateInNamespace = false;
						} else if ((previous->getType() == CONCURRENT_ACCESS_TYPE) || (previous->getType() == COMMUTATIVE_ACCESS_TYPE)) {
							canPropagateInNamespace = false;
						} else if((dataAccess->getType() == AUTO_ACCESS_TYPE) || (previous->getType() == AUTO_ACCESS_TYPE)) {
							canPropagateInNamespace = false;
						}
						if (canPropagateInNamespace) {
							Instrument::namespacePropagation(Instrument::NamespaceSuccessful, dataAccess->getAccessRegion());
						} else {
							Instrument::namespacePropagation(Instrument::NamespaceWrongPredecessor, dataAccess->getAccessRegion());
						}
					} else if (dataAccess->getNamespacePredecessor() != OffloadedTaskIdManager::InvalidOffloadedTaskId) {
						Instrument::namespacePropagation(Instrument::NamespaceWrongPredecessor, dataAccess->getAccessRegion());
					} else {
						Instrument::namespacePropagation(Instrument::NamespaceNotHintedWithAncestor, dataAccess->getAccessRegion());
					}
				}

				if (parent->isNodeNamespace() && !canPropagateInNamespace) {

					// No namespace propagation. Need to set Topmost and set up reduction info in a similar way
					// to the missing-from-bottom-map case below (in the second lambda).
					DataAccessRegion previousRegion = previous->getAccessRegion();
					accessStructures._accesses.processIntersecting(
						previousRegion,
						[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
							DataAccess *targetAccess = &(*position);
							assert(targetAccess != nullptr);
							assert(!targetAccess->hasBeenDiscounted());

							// We need to allocate the reductionInfo before fragmenting the access
							// (this might not work!)
							if ((dataAccess->getType() == REDUCTION_ACCESS_TYPE) && !hasAllocatedReductionInfo) {
								hasAllocatedReductionInfo = true;

								DataAccessStatusEffects initialStatusR(dataAccess);
								allocateReductionInfo(*dataAccess, *next._task);
								DataAccessStatusEffects updatedStatusR(dataAccess);

								handleDataAccessStatusChanges(
									initialStatusR, updatedStatusR,
									dataAccess, accessStructures, next._task,
									hpDependencyData
								);
							}

							targetAccess = fragmentAccess(targetAccess, previousRegion, accessStructures);

							DataAccessStatusEffects initialStatusT(targetAccess);

							targetAccess->setValidNamespacePrevious(
								VALID_NAMESPACE_NONE,
								OffloadedTaskIdManager::InvalidOffloadedTaskId
							);

							targetAccess->setReceivedReductionInfo();

							// Note: setting ReductionSlotSet as received is not necessary, as its not always propagated
							DataAccessStatusEffects updatedStatusT(targetAccess);

							// TODO: We could mark in the task that there are local accesses (and remove the mark in taskwaits)

							handleDataAccessStatusChanges(
								initialStatusT, updatedStatusT,
								targetAccess, accessStructures, next._task,
								hpDependencyData
							);

							/* keep going with the other task data accesses that intersect this
							 * hole in the bottom map */
							return true;
						}
					);

				} else {
					// Normal propagation: set the new access to be the next access after the
					// access that was in the bottom map.
					previous->setNext(next);
					if (parent->isNodeNamespace()) {
						if (dataAccess->getType() == READ_ACCESS_TYPE) {
							previous->setNamespaceNextIsIn();
						}
						if (dataAccess->getAccessRegion().fullyContainedIn(previous->getAccessRegion())) {
							dataAccess->setPropagateFromNamespace();
						} else {
							// Need to set propagate from namespace for the part of dataAccess that is covered
							// by previous. Unfortunately we cannot fragment dataAccess right now because we are in the
							// middle of handling all the bottom map entries that cover it. So make a delayed
							// operation to set propagate from namespace for this region.
							UpdateOperation updateOperation(DataAccessLink(dataAccess->getOriginator(), access_type), previous->getAccessRegion());
							updateOperation._setPropagateFromNamespace = true;
							hpDependencyData._delayedOperations.emplace_back(updateOperation);
						}
					}
				}
#else // USE_CLUSTER
				previous->setNext(next);
#endif // USE_CLUSTER
				previous->unsetInBottomMap();  /* only unsets the status bit, doesn't actually remove it */

				DataAccessStatusEffects updatedStatus(previous);

				/*
				 * Handle the data access status changes in the difference
				 * between initialStatus and updatedStatus.
				 */
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					previous, previousAccessStructures, previousTask,
					hpDependencyData);

				/* Keep going with other bottom map entries (don't stop here) */
				return true;
			},

			/* missingProcessor: handle a region (part of the overall data access)
			 * that is not part of the parent's accesses.
			 */
			[&](DataAccessRegion missingRegion) -> bool {
				assert(!parentAccessStructures._accesses.contains(missingRegion));

				if (parent->isNodeNamespace()) {
					if (dataAccess->getNamespacePredecessor() != OffloadedTaskIdManager::InvalidOffloadedTaskId) {
						Instrument::namespacePropagation(Instrument::NamespacePredecessorFinished, region);
					} else {
						Instrument::namespacePropagation(Instrument::NamespaceNotHintedNoPredecessor, region);
					}
				}

				// Not part of the parent
				local = true;

#ifdef USE_CLUSTER
				// The current implementation does not properly handle the case
				// when a subtask's accesses are not a subset of its parent's
				// accesses (unless the parent is main or the namespace). The problem is in
				// unregisterTaskDataAccesses1 for the parent, as it ignores
				// any bottom map entries that are not part of the task's
				// accesses. So the child's accesses continue to be flagged as
				// being on the bottom map and are therefore never deleted. The
				// program runs to completion, exits main, but then hangs. Since
				// this only happens for incorrect pragmas, which won't work properly
				// anyway, add a check and raise an error.
				FatalErrorHandler::failIf(parent->getParent() != nullptr,
										  "Access ", missingRegion,
										  " of ", dataAccess->getOriginator()->getLabel(),
										  " is not part of the parent's accesses");
#else
#ifndef NDEBUG
				if (!first) {
					assert((local == lastWasLocal) && "This fails with wrongly nested regions");
				}
				first = false;
				lastWasLocal = local;
#endif
#endif

				// NOTE: holes in the parent bottom map that are not in the parent accesses become fully satisfied
				accessStructures._accesses.processIntersecting(
					missingRegion,
					[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
						DataAccess *targetAccess = &(*position);
						assert(targetAccess != nullptr);
						assert(!targetAccess->hasBeenDiscounted());

						// We need to allocate the reductionInfo before fragmenting the access
						if ((dataAccess->getType() == REDUCTION_ACCESS_TYPE) && !hasAllocatedReductionInfo) {
							hasAllocatedReductionInfo = true;

							DataAccessStatusEffects initialStatus(dataAccess);
							allocateReductionInfo(*dataAccess, *next._task);
							DataAccessStatusEffects updatedStatus(dataAccess);

							handleDataAccessStatusChanges(
								initialStatus, updatedStatus,
								dataAccess, accessStructures, next._task,
								hpDependencyData);
						}

						targetAccess = fragmentAccess(targetAccess, missingRegion, accessStructures);

						DataAccessStatusEffects initialStatus(targetAccess);
						//! If this is a remote task, we will receive satisfiability
						//! information later on, otherwise this is a local access,
						//! so no location is setup yet.
						//! For now we set it to the Directory MemoryPlace.
						if (!targetAccess->getOriginator()->isRemoteTask()) {
							/* TBD? Is this an access from e.g. a malloc inside the parent task? */
							targetAccess->setReadSatisfied(Directory::getDirectoryMemoryPlace());
							targetAccess->setWriteSatisfied();
							targetAccess->setConcurrentSatisfied();
							targetAccess->setCommutativeSatisfied();
						}

						targetAccess->setReceivedReductionInfo();
						targetAccess->setValidNamespacePrevious(
							VALID_NAMESPACE_NONE,
							OffloadedTaskIdManager::InvalidOffloadedTaskId
						);

						// Note: setting ReductionSlotSet as received is not necessary, as its not always propagated
						DataAccessStatusEffects updatedStatus(targetAccess);

						// TODO: We could mark in the task that there are local accesses (and remove the mark in taskwaits)

						handleDataAccessStatusChanges(
							initialStatus, updatedStatus,
							targetAccess, accessStructures, next._task,
							hpDependencyData);

						/* keep going with the other task data accesses that intersect this
						 * hole in the bottom map */
						return true;
					});

				/* Keep going with other bottom map entries (don't stop here) */
				return true;
			});

		if (hasAllocatedReductionInfo && !previousReductionAccesses.empty()) {
			assert(dataAccess->getType() == REDUCTION_ACCESS_TYPE);

			for (DataAccess *&previousAccess : previousReductionAccesses) {
				assert(previousAccess->getType() == REDUCTION_ACCESS_TYPE);
				previousAccess->setClosesReduction();
			}
		}

		// Add the entry to the bottom map
		BottomMapEntry *bottomMapEntry = ObjectAllocator<BottomMapEntry>::newObject(
			region, next, parentAccessType, parentReductionTypeAndOperatorIndex);
		parentAccessStructures._subaccessBottomMap.insert(*bottomMapEntry);
	}


	/*
	 * This is called by registerTaskDataAccesses to actually link the task
	 * data accesses into the dependency system.
	 */
	static inline void linkTaskAccesses(
		/* OUT */ CPUDependencyData &hpDependencyData,
		Task *task)
	{
		assert(task != nullptr);

		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());

		// No accesses: do nothing.
		if (accessStructures._accesses.empty()) {
			return;
		}

		Task *parent = task->getParent();
		assert(parent != nullptr);

		TaskDataAccesses &parentAccessStructures = parent->getDataAccesses();
		assert(!parentAccessStructures.hasBeenDeleted());

		/*
		 * Lock the parent and the task itself.
		 */
		std::lock_guard<TaskDataAccesses::spinlock_t> parentGuard(parentAccessStructures._lock);
		std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);

		if (task->hasAllMemory()
			&& parent->getParent() != nullptr
			&& !parent->isNodeNamespace()) {
			// Inherit all the none accesses from the parent
			char *curAddress = (char *)(0x1); // ignore nullptr
			parentAccessStructures._accesses.processAll(
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess &access = *position;
					DataAccessRegion accessRegion = access.getAccessRegion();
					char *startAddr = (char *)accessRegion.getStartAddress();
					char *endAddr = (char *)accessRegion.getEndAddress();

					/* Remove auto regions in the parent's gap */
					if (accessRegion.getStartAddress() > curAddress) {
						DataAccessRegion gap = DataAccessRegion(curAddress, startAddr);
						accessStructures._accesses.processIntersecting(
							gap,
							[&](TaskDataAccesses::accesses_t::iterator childPosition) -> bool {
								DataAccess *childAccess = &(*childPosition);
								childAccess = fragmentUnregisteredAccessObject(childAccess, gap, accessStructures);
								childAccess->markAsDiscounted();
								accessStructures._accesses.erase(childAccess);
								ObjectAllocator<DataAccess>::deleteObject(childAccess);
								return true;
							}
						);
					}

					/* Demote any auto regions inside an in or weakin to be weakin */
					if (access.getType() == READ_ACCESS_TYPE) {
						accessStructures._accesses.processIntersecting(
							accessRegion,
							[&](TaskDataAccesses::accesses_t::iterator childPosition) -> bool {
								DataAccess *childAccess = &(*childPosition);
								childAccess = fragmentUnregisteredAccessObject(childAccess, accessRegion, accessStructures);
								if (childAccess->getType() == AUTO_ACCESS_TYPE) {
									childAccess->setType(READ_ACCESS_TYPE);
								}
								return true;
							}
						);
					}
					curAddress = endAddr;
					return true;
				}
			);
		}

		// Create any initial missing fragments in the parent, link the previous accesses
		// and possibly some parent fragments to the new task, and create propagation
		// operations from the previous accesses to the new task.
		//
		// The new task cannot be locked since it may have a predecessor multiple times,
		// and that could produce a dead lock if the latter is finishing (this one would
		// lock the new task first, and the predecessor later; the finishing task would
		// do the same in the reverse order). However, we need to protect the traversal
		// itself, since an already linked predecessor may produce fragmentation and thus
		// may rebalance the tree. Therefore, we just lock for advancing the iteration.
		accessStructures._accesses.processAll(
			/* processor: called for each task access */
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *dataAccess = &(*position);
				assert(dataAccess != nullptr);
				assert(!dataAccess->hasBeenDiscounted());

				if (dataAccess->getType() == NO_ACCESS_TYPE) {

					/* Remove any data accesses of type NO_ACCESS_TYPE that result from a non-upgraded
					 * none() pragma clause. Note: other accesses of NO_ACCESS_TYPE may be added later,
					 * for various reasons, but the ones that result from the none() pragma clause do
					 * not need to be kept.
					 */

					dataAccess->markAsDiscounted();
					accessStructures._accesses.erase(dataAccess);
					ObjectAllocator<DataAccess>::deleteObject(dataAccess);
					return true;
				}

				DataAccessStatusEffects initialStatus(dataAccess);
				dataAccess->setNewInstrumentationId(task->getInstrumentationTaskId());

				/* New task accesses always in the bottom map */
				dataAccess->setInBottomMap();

				/* This is the step where accesses become registered */
				dataAccess->setRegistered();

#ifndef NDEBUG
				/*
				 * The accesses now become reachable so for most updates the
				 * task data access structures will need locking.
				 */
				dataAccess->setReachable();
#endif
				DataAccessStatusEffects updatedStatus(dataAccess);

				/* Process the changes between initialStatus and updatedStatus */
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					dataAccess, accessStructures, task,
					hpDependencyData);

				/* Update bottom map */
				replaceMatchingInBottomMapLinkAndPropagate(
					DataAccessLink(task, access_type),
					accessStructures,
					dataAccess,
					parent, parentAccessStructures,
					hpDependencyData);

				return true;
			});

		unfragmentTaskAccesses(task, accessStructures, true);
	}


	/*
	 * Called by handleEnterTaskwait.
	 */
	static inline void finalizeFragments(
		Task *task, TaskDataAccesses &accessStructures,
		/* OUT */ CPUDependencyData &hpDependencyData)
	{
		assert(task != nullptr);
		assert(!accessStructures.hasBeenDeleted());

		// Mark the fragments as completed and propagate topmost property
		accessStructures._accessFragments.processAll(
			/* processor: set every fragment as complete (if not already) */
			[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
				DataAccess *fragment = &(*position);
				assert(fragment != nullptr);

				// The fragment may already be complete due to the use of the "release" directive
				if (fragment->complete()) {
					return true;
				}
				assert(!fragment->hasBeenDiscounted());

				/* Set access as complete */
				DataAccessStatusEffects initialStatus(fragment);
				fragment->setComplete();
				DataAccessStatusEffects updatedStatus(fragment);

				/* Handle consequences of access becoming complete */
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					fragment, accessStructures, task,
					hpDependencyData);

				return true;
			});
	}


	/*
	 * Apply a lambda function (processor) to a region of a single task access
	 * and all the fragments that intersect it (fragmenting them if needed).
	 */
	template <typename ProcessorType>
	static inline void applyToAccessAndFragments(
		DataAccess *dataAccess,            /* DataAccess containing the region */
		DataAccessRegion const &region,
		TaskDataAccesses &accessStructures,
		ProcessorType processor
	) {
		// Fragment the data access if necessary (then continue with the first
		// fragment - remaining fragments will be processed later)
		dataAccess = fragmentAccess(dataAccess, region, accessStructures);
		assert(dataAccess != nullptr);

		bool hasSubaccesses = dataAccess->hasSubaccesses();
		DataAccessRegion finalRegion = dataAccess->getAccessRegion();
		bool alsoSubaccesses = processor(dataAccess);

		if (alsoSubaccesses && hasSubaccesses) {
			accessStructures._accessFragments.processIntersecting(
				finalRegion,
				[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
					DataAccess *fragment = &(*position);
					assert(fragment != nullptr);
					assert(!fragment->hasBeenDiscounted());

					fragment = fragmentAccess(fragment, finalRegion, accessStructures);
					assert(fragment != nullptr);

					processor(fragment);

					return true;
				});
		}
	}


	static inline void releaseReductionStorage(
		__attribute__((unused)) Task *finishedTask, DataAccess *dataAccess,
		__attribute__((unused)) DataAccessRegion region,
		ComputePlace *computePlace)
	{
		assert(finishedTask != nullptr);
		assert(dataAccess != nullptr);
		assert(computePlace != nullptr);

		assert(dataAccess->getOriginator() == finishedTask);
		assert(!region.empty());

		// Release reduction slots (only when necessary)
		// Note: Remember weak accesses in final tasks will be promoted to strong
		if ((dataAccess->getType() == REDUCTION_ACCESS_TYPE) && !dataAccess->isWeak()) {
			assert(computePlace->getType() == nanos6_device_t::nanos6_host_device);

#ifdef NDEBUG
			CPU *cpu = static_cast<CPU *>(computePlace);
#else
			CPU *cpu = dynamic_cast<CPU *>(computePlace);
			assert(cpu != nullptr);
#endif

			ReductionInfo *reductionInfo = dataAccess->getReductionInfo();
			assert(reductionInfo != nullptr);

			reductionInfo->releaseSlotsInUse(cpu->getIndex());
		}
	}


	static inline void finalizeAccess(
		Task *finishedTask, DataAccess *dataAccess, DataAccessRegion region,
		WriteID writeID,
		MemoryPlace const *location, /* OUT */ CPUDependencyData &hpDependencyData, 
		bool isReleaseAccess
	) {
		(void)writeID;
		assert(finishedTask != nullptr);
		assert(dataAccess != nullptr);
		// assert((location != nullptr) || dataAccess->isWeak());

		assert(dataAccess->getOriginator() == finishedTask);
		assert(!region.empty());

		// The access may already have been released through the "release" directive
		if (dataAccess->complete()) {
			return;
		}
		assert(!dataAccess->hasBeenDiscounted());

		// Does the original access have early release in namespace? We need this
		// information when handling the fragments in the below loop, but it may
		// get deleted by the same loop when it handles the original access itself
		// (which is done first). This only happens when disable_autowait=true.
		bool dataAccessEarlyReleaseInNamespace = dataAccess->getEarlyReleaseInNamespace();
		bool propagateFromNamespace = dataAccess->getPropagateFromNamespace();

		// Set the writeID of the finalized access if we are given one
		if (writeID != 0) {
			dataAccess->setWriteID(writeID);
		}

		/*
		 * Set complete and update location for the access itself and all
		 * (child task) fragments.
		 */
		applyToAccessAndFragments(
			dataAccess, region,
			finishedTask->getDataAccesses(),
			[&](DataAccess *accessOrFragment) -> bool {

				assert(accessOrFragment->getOriginator() == finishedTask);

				DataAccessStatusEffects initialStatus(accessOrFragment);

				// If the access is early released in the namespace (autowait
				// feature), then set early release in the fragments too.
				if (dataAccessEarlyReleaseInNamespace) {
					if (accessOrFragment != dataAccess) {
						accessOrFragment->setEarlyReleaseInNamespace();
					}
				}

				// Send AccessInfo messages for any regions that were never accessed by
				// the task or a subtask
				if (finishedTask->isRemoteTask()
					&& ClusterManager::autoOptimizeNonAccessed()
					&& !propagateFromNamespace
					&& !accessOrFragment->getDisableEagerSend()) {
					if ((accessOrFragment->getObjectType() == access_type
							&& !accessOrFragment->isAutoHasBeenAccessed()
							&& !dataAccess->isStrongLocalAccess()
							&& !accessOrFragment->hasSubaccesses())
						|| (accessOrFragment->getObjectType() == fragment_type
							&& accessOrFragment->hasNext()
							&& accessOrFragment->getNext()._objectType == taskwait_type)) {

							if (ClusterManager::getEagerSend() || accessOrFragment->getType() == AUTO_ACCESS_TYPE) {
								ClusterNode *node = accessOrFragment->getOriginator()->getClusterContext()->getRemoteNode();
								accessOrFragment->setDisableEagerSend();
								hpDependencyData._accessInfoMap[node].emplace_back(
										accessOrFragment->getAccessRegion(),
										accessOrFragment->getOriginator()->getOffloadedTaskId(),
										/* noEagerSend */ true,
										/* isReadOnly */ false,
										accessOrFragment->getOriginator());
							}
						}
				}

				// the access will already be complete only if it is a task with a wait clause
				if (accessOrFragment->complete()) {
					return true;
				}

				accessOrFragment->setComplete();

				if (location != nullptr) {

					if (isReleaseAccess && accessOrFragment->hasDataLinkStep()
						&& accessOrFragment->getType() != COMMUTATIVE_ACCESS_TYPE
						&& accessOrFragment->getType() != CONCURRENT_ACCESS_TYPE) {
						bool notSat = false;
						if (!accessOrFragment->readSatisfied()) {
							accessOrFragment->setReadSatisfied(location);
							notSat = true;
						}
						if (!accessOrFragment->writeSatisfied()) {
							// Remote only releases an access once it gets write or pseudowrite satisfiability.
							// Update our access with write satisfiability, unless the remote only has pseudowrite.
							// When the remote has pseudowrite only (from our perspective) we must wait
							// for write satisfiability to be propagated at this end.
							if (!accessOrFragment->remoteHasPseudowrite()) {
								accessOrFragment->setWriteSatisfied();
							}
							notSat = true;
						}
						if (!accessOrFragment->concurrentSatisfied()) {
							if (!accessOrFragment->remoteHasPseudowrite()) {
								accessOrFragment->setConcurrentSatisfied();
							}
						}
						if (!accessOrFragment->receivedReductionInfo()) {
							accessOrFragment->setReceivedReductionInfo();
						}
						if (notSat) {
							accessOrFragment->unsetDataLinkStep();
						}
					}

					/* Normal non-cluster case e.g. for NUMA */
					accessOrFragment->setLocation(location);
				} else if (!accessOrFragment->isWeak()) {
					const MemoryPlace *oldLocation = accessOrFragment->getLocation();
					if (oldLocation == nullptr || Directory::isDirectoryMemoryPlace(oldLocation)) {
						// This happens for strong subtasks of a weak task when cluster.eager_weak_fetch is false
						accessOrFragment->setLocation(ClusterManager::getCurrentMemoryNode());
					}
				}
				DataAccessStatusEffects updatedStatus(accessOrFragment);

				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					accessOrFragment, finishedTask->getDataAccesses(), finishedTask,
					hpDependencyData
				);

				return true; // Apply also to subaccesses if any
			});

	}


	static void handleRemovableTasks(
		/* inout */ CPUDependencyData::removable_task_list_t &removableTasks
	) {
		// Early exit
		if (removableTasks.empty()) {
			return;
		}

		std::set<Task *> offloadedTaskSet;

		// We create a list here to avoid taking the lock too much when the offloadedTask is common
		// to multiple removableTasks entries.
		for (Task *removableTask : removableTasks) {
			Task *offloadedTask = removableTask->getOffloadedPredecesor();

			if (offloadedTask != nullptr && offloadedTask->hasDataReleaseStep()) {
				offloadedTaskSet.emplace(offloadedTask);
			}
		}

		for (Task *offloadedTask : offloadedTaskSet) {
			// This extra condition is to releasePendingAccesses for offloadedTask that are not
			// going to be disposed now. Those accesses need to be released now.
			// TODO: We may group them by destination node in the future.
			// TODO: The DataReleaseStep stores the accesses regions in a vector. Using a map may
			// simplify mergin those acccesses when they are contiguous, reducing the number of
			// regions, keeping them sorted and reducing the size of the target message.
			if (std::find(removableTasks.begin(), removableTasks.end(), offloadedTask)
				== removableTasks.end()) {
				// The offloadedTask is not going to be deleted, so only releasePendingAccesses.
				offloadedTask->getDataReleaseStep()->releasePendingAccesses(false);
			}
		}

		for (Task *removableTask : removableTasks) {
			TaskFinalization::disposeTask(removableTask);
		}

		removableTasks.clear();
	}

	static void handleCompletedTaskwaits(
		CPUDependencyData &hpDependencyData,
		__attribute__((unused)) ComputePlace *computePlace)
	{
		if (hpDependencyData._completedTaskwaits.empty()) {
			return;
		}

		Instrument::enterHandleCompletedTaskwaits();
		for (DataAccess *taskwait : hpDependencyData._completedTaskwaits) {
			assert(taskwait->getObjectType() == taskwait_type);
			ExecutionWorkflow::setupTaskwaitWorkflow(
				taskwait->getOriginator(),
				taskwait,
				hpDependencyData);
		}
		hpDependencyData._completedTaskwaits.clear();
		Instrument::exitHandleCompletedTaskwaits();
	}


	/*
	 * Create a single taskwait fragment (called by createTaskwait).
	 * Starts and finishes with the lock on task.
	 */
	static inline void createTaskwaitFragment(
		Task *task,
		BottomMapEntry *bottomMapEntry,
		ComputePlace *computePlace,
		DataAccessRegion region,
		TaskDataAccesses &accessStructures,
		CPUDependencyData &hpDependencyData,
		bool fetchData,
		bool dontRelease = false)
	{
		DataAccessLink previous = bottomMapEntry->_link;

		// **WARNING**: Bottom map entries are not fragmented by the parent
		// task's accesses.  So if a child task covers multiple task accesses
		// of different type, then bottomMapEntry->_accessType may not be
		// correct for all. We keep it for now since this access type is only
		// used (1) in the workflow to not fetch dmallocs in non-auto
		// tasks (i.e. accesses of NO_ACCESS_TYPE in the distributed region),
		// which will not be in the same dependency as accesses of a different
		// type, and (2) in createTaskwait to not fetch weakconcurrent or
		// auto accesses when cluster.eager_weak_fetch=true, which may
		// cause a few redundant eager fetches but not in the problematic case
		// of auto tasks (for which cluster.eager_weak_fetch=false).
		DataAccessType accessType = bottomMapEntry->_accessType;

		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex =
		bottomMapEntry->_reductionTypeAndOperatorIndex;
		{
			DataAccess *taskwaitFragment = createAccess(
				task,
				taskwait_type,
				accessType, /* not weak */ false, region,
				reductionTypeAndOperatorIndex);

			// No need for symbols in a taskwait

			DataAccessStatusEffects initialStatus(taskwaitFragment);
			taskwaitFragment->setNewInstrumentationId(task->getInstrumentationTaskId());
			taskwaitFragment->setInBottomMap();
			taskwaitFragment->setRegistered();

			if (fetchData) {
				assert(computePlace != nullptr);
				taskwaitFragment->setOutputLocation(computePlace->getMemoryPlace(0));
			}
			if (dontRelease) {
				taskwaitFragment->setDisableEagerSend();
			}
			// taskwaitFragment->setComplete();

		// NOTE: For now we create it as completed, but we could actually link
		// that part of the status to any other actions that needed to be carried
		// out. For instance, data transfers.
		// taskwaitFragment->setComplete();
#ifndef NDEBUG
			taskwaitFragment->setReachable();
#endif
			accessStructures._taskwaitFragments.insert(*taskwaitFragment);

			// Update the bottom map entry to now be of taskwait type
			bottomMapEntry->_link._objectType = taskwait_type;
			bottomMapEntry->_link._task = task;

			DataAccessStatusEffects updatedStatus(taskwaitFragment);

			handleDataAccessStatusChanges(
				initialStatus, updatedStatus,
				taskwaitFragment, accessStructures, task,
				hpDependencyData);
		}

		/*
		 * Previous task (that was previously in the bottom map)
		 */
		TaskDataAccesses &previousAccessStructures = previous._task->getDataAccesses();

		// Unlock parent task to avoid potential deadlock
		if (previous._task != task) {
			accessStructures._lock.unlock();
			previousAccessStructures._lock.lock();
		}

		followLink(
			previous, region,
			[&](DataAccess *previousAccess) -> bool {
				DataAccessStatusEffects initialStatus(previousAccess);
				// Mark end of reduction
				if ((previousAccess->getType() == REDUCTION_ACCESS_TYPE)
					&& (previousAccess->getReductionTypeAndOperatorIndex()
						!= reductionTypeAndOperatorIndex)) {
					// When a reduction access is to be linked with a taskwait, we want to mark the
					// reduction access so that it is the last access of its reduction chain
					//
					// Note: This should only be done when the reductionType of the parent access
					// (obtained by 'reductionTypeAndOperatorIndex')
					// is different from the reduction access reductionType.
					// Ie. The reduction in which the subaccess participates is different from its
					// parent's reduction, and thus it should be closed by the nested taskwait
					previousAccess->setClosesReduction();
				}

				/*
				 * Link to the taskwait and unset flag indicating that it was in bottom map.
				 */

				previousAccess->setNext(DataAccessLink(task, taskwait_type));
				previousAccess->unsetInBottomMap();
				DataAccessStatusEffects updatedStatus(previousAccess);

				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					previousAccess, previousAccessStructures, previous._task,
					hpDependencyData
				);

				return true;
			});

		// Relock to exit with the lock still on task
		if (previous._task != task) {
			previousAccessStructures._lock.unlock();
			accessStructures._lock.lock();
		}
	}

	/*
	 * Create a taskwait. The lock should already be taken on the task's
	 * access structures.
	 */
	static void createTaskwait(
		Task *task, TaskDataAccesses &accessStructures, ComputePlace *computePlace,
		/* OUT */ CPUDependencyData &hpDependencyData, bool noflush, bool nonLocalOnly)
	{
		assert(task != nullptr);
		// assert(accessStructures._lock.isLockedByThisThread());

		// The last taskwait fragment will decrease the blocking count.
		// This is necessary to force the task to wait until all taskwait fragments have finished.
		bool mustWait = false;

		/*
		 * There should not already be any taskwait fragments.
		 */
		assert(accessStructures._taskwaitFragments.empty());

		/*
		 * If a normal taskwait (without noflush) is performed after a taskwait noflush,
		 * there may be accesses that are not on the bottom map but that have not been
		 * brought back to this task. These can be recognized as task accesses that
		 * are not weak and not on the bottom map but whose location is not the current
		 * node. These accesses just need a new bottom map entry to cover them.
		 */
		if (computePlace && !noflush) {
			accessStructures._accesses.processAll(
				/* processor: called for each task access */
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *access = &(*position);
					assert(access != nullptr);
					const MemoryPlace *location = access->getLocation();

					if (!access->isWeak()) {
						// Non-weak accesses must already have a location
						assert(location);
						if (location->getType() == nanos6_cluster_device
						    && location != ClusterManager::getCurrentMemoryNode()) {

							DataAccessRegion region = access->getAccessRegion();
							DataAccessType accessType = access->getType();
							// access->setLocation(ClusterManager::getCurrentMemoryNode());
							bool foundGap = false;

							foreachBottomMapMatchMissingRegionCreatingInitialFragments(
								task,
								accessStructures,
								region,

								/* matchingProcessor: handle a region (part of the new overall
								 * data access) that is now in the bottom map. This bottom map entry
								 * may have just been created by
								 * "foreachBottomMapMatchPossiblyCreatingInitialFragmentsAndMissingRegion".
								 * The bottom map access is 'previous'. */

								[&](DataAccess *previous, BottomMapEntryContents const ) -> bool {
									(void)previous;
									assert(previous->isInBottomMap());
									foundGap = true;
									return true;
								},
								/* missingProcessor: handle a region (part of the overall data access)
								 * that is not part of the parent's accesses. Cannot happen.
								 */
								[&](DataAccessRegion ) -> bool {
									assert(false);
									return true; // avoid compiler warning
								}

							);

							// Add the entry to the bottom map
							if (foundGap) {
								DataAccessLink next = DataAccessLink(task, fragment_type);
								BottomMapEntry *bottomMapEntry = ObjectAllocator<BottomMapEntry>::newObject(
										region, next, accessType, no_reduction_type_and_operator);
								accessStructures._subaccessBottomMap.insert(*bottomMapEntry);
							}
						}
					}
					return true; /* always continue, don't stop here */
				}
			);
		}

		/*
		 * All taskwaits must also wait for all accesses that are on the bottom
		 * map.  Iterate through the bottom map and for each subaccess in the
		 * bottom map, create a new taskwait fragment that depends on it.
		 */
		if (!nonLocalOnly) {
			// Normal case: make a taskwait fragment for each entry on the bottom map

		  	accessStructures._subaccessBottomMap.processAll(
				   [&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
						BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
						assert(bottomMapEntry != nullptr);

						DataAccessRegion region = bottomMapEntry->_region;
						if (noflush || computePlace == nullptr) {
							// Taskwait noflush: so don't flush
							createTaskwaitFragment(task, bottomMapEntry, computePlace, region, accessStructures, hpDependencyData, /* fetchData */ false);
						} else if (ClusterManager::getEagerWeakFetch()
									&& bottomMapEntry->_accessType != CONCURRENT_ACCESS_TYPE) {
							// Eager weak fetch and not concurrent or auto, so always fetch, even if weak
							// **WARNING**: bottomMapEntry->_accessType might not be correct if a single bottom map entry
							// covers multiple task accesses, as bottom map entries are not fragmented for task accesses.
							// So we might do some unnecessary eager fetches of weakconcurrent or auto accesses. This
							// is only likely to be problematic for auto tasks, but in that case
							// cluster.eager_weak_fetch=false.
							createTaskwaitFragment(task, bottomMapEntry, computePlace, region, accessStructures, hpDependencyData, /* fetchData */ true);
						 } else {
							// Not eager weak fetch: so fetch data only for strong accesses. We need to check the accesses
							// to find out which parts are parts of strong accesses. Note a single bottom map entry may
							// cover multiple accesses. We also fetch lmalloc and stack local accesses even if they are
							// covered by a weak (auto) access.
							accessStructures._accesses.processIntersectingAndMissing(region,
								[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
									// Create taskwait fragment for an access, fetching the data if the access is strong
									DataAccess *dataAccess = &(*position);
									assert(dataAccess != nullptr);
									bool fetchData = !dataAccess->isWeak() || dataAccess->isStrongLocalAccess();
									DataAccessRegion subregion = region.intersect(dataAccess->getAccessRegion());
									bottomMapEntry = fragmentBottomMapEntry(bottomMapEntry, subregion, accessStructures);
									createTaskwaitFragment(task, bottomMapEntry, computePlace, subregion, accessStructures, hpDependencyData, fetchData);
									bottomMapEntry = &(*(++bottomMapPosition));
									return true;
								},
								[&](DataAccessRegion missingRegion) -> bool {
									// Create taskwait fragment for a missing region, never fetching the data
									DataAccessRegion subregion = region.intersect(missingRegion);
									bottomMapEntry = fragmentBottomMapEntry(bottomMapEntry, subregion, accessStructures);
									createTaskwaitFragment(task, bottomMapEntry, computePlace, subregion, accessStructures, hpDependencyData, /*fetchData*/ false);
									bottomMapEntry = &(*(++bottomMapPosition));
									return true;
									}
							);
						 }
						mustWait = true;
						return true;
					});
		} else {
			/* With autowait, only create taskwait fragments for bottom map
			 * entries that have not already been early released in the
			 * namespace (by unregisterLocallyPropagatedTaskDataAccesses). We have to
			 * ensure that bottom map entries not covered by any access still
			 * get a taskwait fragment, since otherwise there would be a hang
			 * on incorrect programs where the parent pragmas don't cover all
			 * the subtask accesses. We also have to use the "WithRestart"
			 * method to iterate over the accesses, since
			 * createTaskwaitFragment releases the lock on the access
			 * structures to set the next link of the access in the bottom map;
			 * this means that another thread may fragment the accesses while
			 * we are iterating over them.
			 */
			accessStructures._subaccessBottomMap.processAll(
				   [&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
						BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
						assert(bottomMapEntry != nullptr);
						DataAccessRegion region = bottomMapEntry->_region;
						void *addr = nullptr;
						accessStructures._accesses.processIntersectingWithRestart(
							region,
							[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
								bool continueWithoutRestart = true;
								DataAccess *dataAccess = &(*position);
								assert(dataAccess != nullptr);
								DataAccessRegion accessRegion = dataAccess->getAccessRegion();
								if (accessRegion.getEndAddress() <= addr) {
									// Restart revisits the same access a second time; skip the second
									// time a region is visited
									return true;
								}
								addr = accessRegion.getEndAddress();
								if (dataAccess->getEarlyReleaseInNamespace()
									&& !dataAccess->getNamespaceNextIsIn()) {
									// This access has early release in the namespace, so
									// don't make a taskwait fragment
								} else {
									// Make a taskwait fragment covering this access
									DataAccessRegion subregion = accessRegion.intersect(region);
									bottomMapEntry = fragmentBottomMapEntry(bottomMapEntry, subregion, accessStructures);
									bool dontRelease = (bottomMapEntry->_link._objectType == fragment_type)
														&& !dataAccess->isStrongLocalAccess()
														&& ClusterManager::autoOptimizeNonAccessed();
									createTaskwaitFragment(task, bottomMapEntry, computePlace, subregion, accessStructures, hpDependencyData, /* fetchData */ false, dontRelease);
									mustWait = true;
									continueWithoutRestart = false;
									bottomMapEntry = &(*(++bottomMapPosition));
								}
								return continueWithoutRestart;
							});
						/* Always continue with the rest of the accesses */
						return true;
						});
		}
		if (mustWait) {
			task->increaseBlockingCount();
		}
	}

	/*
	 * createTopLevelSinkFragment (called by createTopLevelSink).
	 * Starts and finishes with the lock on task.
	 */
	static inline void createTopLevelSinkFragment(
		Task *task,
		BottomMapEntry *bottomMapEntry,
		DataAccessRegion region,
		TaskDataAccesses &accessStructures,
		CPUDependencyData &hpDependencyData,
		bool dontRelease = false)
	{
		DataAccessLink previous = bottomMapEntry->_link;
		DataAccessType accessType = bottomMapEntry->_accessType;
		assert(bottomMapEntry->_reductionTypeAndOperatorIndex == no_reduction_type_and_operator);
		{
			DataAccess *topLevelSinkFragment = createAccess(
				task,
				top_level_sink_type,
				accessType, /* not weak */ false, region);

			// TODO, top level sink fragment, what to do with the symbols?

			DataAccessStatusEffects initialStatus(topLevelSinkFragment);
			topLevelSinkFragment->setNewInstrumentationId(task->getInstrumentationTaskId());
			topLevelSinkFragment->setInBottomMap();
			topLevelSinkFragment->setRegistered();

			if (dontRelease) {
				topLevelSinkFragment->setDisableEagerSend();
			}

			// NOTE: Do not set as complete until linked to access' next (if it has one)
#ifndef NDEBUG
			topLevelSinkFragment->setReachable();
#endif
			accessStructures._taskwaitFragments.insert(*topLevelSinkFragment);

			// Update the bottom map entry
			bottomMapEntry->_link._objectType = top_level_sink_type;
			bottomMapEntry->_link._task = task;

			DataAccessStatusEffects updatedStatus(topLevelSinkFragment);

			handleDataAccessStatusChanges(
				initialStatus, updatedStatus,
				topLevelSinkFragment, accessStructures, task,
				hpDependencyData);
		}

		TaskDataAccesses &previousAccessStructures = previous._task->getDataAccesses();

		// Unlock to avoid potential deadlock
		if (previous._task != task) {
			accessStructures._lock.unlock();
			previousAccessStructures._lock.lock();
		}

		/*
		 * Process every access of the previous task (that was in the
		 * bottom map) that intersects the current region, as its next
		 * access will be the new top-level sink taskwait fragment.
		 * Since previous is a DataAccessLink, followLink will apply
		 * the lambda function to the right kind of accesses (access,
		 * fragment or taskwait).
		 */
		followLink(
			previous,    /* previous task was in bottom map */
			region,
			/* processor: called for every intersecting access of the previous task */
			[&](DataAccess *previousAccess) -> bool
			{
				DataAccessStatusEffects initialStatus(previousAccess);
				// Mark end of reduction
				if (previousAccess->getType() == REDUCTION_ACCESS_TYPE) {
					// When a reduction access is to be linked with a top-level sink, we want to mark the
					// reduction access so that it is the last access of its reduction chain
					//
					// Note: This is different from the taskwait above in that a top-level sink will
					// _always_ mean the reduction is to be closed
					previousAccess->setClosesReduction();
				}

				/*
				 * Link to the top-level sink and unset flag indicating that it was in bottom map.
				 */
				previousAccess->setNext(DataAccessLink(task, taskwait_type));
				previousAccess->unsetInBottomMap();
				DataAccessStatusEffects updatedStatus(previousAccess);

				/* Handle the consequences */
				handleDataAccessStatusChanges(
					initialStatus, updatedStatus,
					previousAccess, previousAccessStructures, previous._task,
					hpDependencyData);

				/* Continue with all intersecting accesses of previous task */
				return true;
			});

		// Relock to exit with the lock still on task
		if (previous._task != task) {
			previousAccessStructures._lock.unlock();
			accessStructures._lock.lock();
		}
	}

	/*
	 * createTopLevelSink:
	 *
	 * This function is called by unregisterTaskDataAccesses when the task
	 * finishes. For each entry in the bottom map, a new taskwait fragment is
	 * created (of top_level_sink_type), which is the successor (next access)
	 * of the access that was in the bottom map.
	 *
	 * The task data accesses must already be locked by the caller.
	 */

	static void createTopLevelSink(
		Task *task, TaskDataAccesses &accessStructures, /* OUT */ CPUDependencyData &hpDependencyData)
	{
		assert(task != nullptr);

		accessStructures._subaccessBottomMap.processAll(
			   [&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
					BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
					assert(bottomMapEntry != nullptr);
					DataAccessRegion region = bottomMapEntry->_region;
					void *addr = nullptr;
					accessStructures._accesses.processIntersectingWithRestart(
						region,
						[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
							bool continueWithoutRestart = true;
							DataAccess *dataAccess = &(*position);
							assert(dataAccess != nullptr);
							DataAccessRegion accessRegion = dataAccess->getAccessRegion();
							if (accessRegion.getEndAddress() <= addr) {
								// Restart revisits the same access a second time; skip the second
								// time a region is visited
								return true;
							}
							addr = accessRegion.getEndAddress();
							if (dataAccess->hasNext() && !dataAccess->getNamespaceNextIsIn()) {
								// This access has early release in the namespace, so
								// don't make a top-level sink fragment
							} else {
								// Make a top-level sink fragment covering this access
								DataAccessRegion subregion = accessRegion.intersect(region);
								bottomMapEntry = fragmentBottomMapEntry(bottomMapEntry, subregion, accessStructures);
								bool dontRelease = (bottomMapEntry->_link._objectType == fragment_type) && ClusterManager::autoOptimizeNonAccessed();
								createTopLevelSinkFragment(task, bottomMapEntry, subregion, accessStructures, hpDependencyData, dontRelease);
								continueWithoutRestart = false;
								bottomMapEntry = &(*(++bottomMapPosition));
							}
							return continueWithoutRestart;
						});
					/* Always continue with the rest of the accesses */
					return true;
			});
	}


	/*
	 * Register a single task data access.
	 *
	 * This function does not link the data access with the parent and sibling
	 * tasks.  Linking is done later inside registerTaskDataAccesses. This
	 * function, registerTaskDataAccess, is (indirectly) called from the
	 * callback given to Nanos6 when the task was created.
	 */
	void registerTaskDataAccess(
		Task *task, DataAccessType accessType, bool weak, DataAccessRegion region, int symbolIndex,
		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex, reduction_index_t reductionIndex,
		OffloadedTaskIdManager::OffloadedTaskId namespacePredecessor)
	{
		assert(task != nullptr);

		DataAccess::symbols_t symbol_list; //TODO consider alternative to vector

		if (symbolIndex >= 0)
			symbol_list.set(symbolIndex);

		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());

		if (accessType == AUTO_ACCESS_TYPE) {
			task->setHasAllMemory();
		}

		/*
		 * This access may fragment an existing access.
		 *
		 * Also collect all symbols used by all intersecting access (?)
		 */
		accessStructures._accesses.fragmentIntersecting(
			region,
			/* duplicator */
			[&](DataAccess const &toBeDuplicated) -> DataAccess * {
				assert(!toBeDuplicated.isRegistered());
				return duplicateDataAccess(toBeDuplicated, accessStructures);
			},
			/* postprocessor */
			[&](__attribute__((unused)) DataAccess *newAccess, DataAccess *originalAccess) {
				symbol_list |= originalAccess->getSymbols();
			});

		/*
		 * The new access may overlap existing accesses. After fragmentation
		 * (above), the region divides up into parts that either match an
		 * existing access ("intersecting") or that are new ("missing"). Handle
		 * the two cases separately using the two lambdas below.
		 */
		accessStructures._accesses.processIntersectingAndMissing(
			region,
			/* intersectingProcessor: may need to upgrade (e.g. existing READ to READWRITE) */
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *oldAccess = &(*position);
				assert(oldAccess != nullptr);

				upgradeAccess(oldAccess, accessType, weak, reductionTypeAndOperatorIndex);
				oldAccess->addToSymbols(symbol_list);

				return true;
			},
			/* missingProcessor: create a new access for it */
			[&](DataAccessRegion missingRegion) -> bool {
				DataAccess *newAccess = createAccess(task, access_type, accessType, weak, missingRegion,
					reductionTypeAndOperatorIndex, reductionIndex);
				newAccess->addToSymbols(symbol_list);
				newAccess->setValidNamespacePrevious(ClusterManager::getCurrentClusterNode()->getIndex(), namespacePredecessor);

				accessStructures._accesses.insert(*newAccess);

				return true;
			}
		);
	}

	/*
	 * This function is called by submitTask to register a task and its
	 * dependencies in the dependency system. The function starts by calling
	 * the callback * "_taskInfo->register_depinfo" that came with the args
	 * block.  The callback registers each data access by a call to Nanos6,
	 * which results in a call to registerTaskDataAccess for each data access.
	 * After registering all the individual task data accesses in this way,
	 * they are linked to existing parent and sibling accesses.
	 */
	bool registerTaskDataAccesses(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData
	) {

		bool ready; /* return value: true if task is ready immediately */

		assert(task != nullptr);
		assert(computePlace != nullptr);

		Instrument::enterRegisterTaskDataAcesses();

		/*
		 * This part creates the DataAccesses and calculates any possible upgrade.
		 * It calls _taskInfo->register_depinfo with the args block, which results
		 * in a call to "registerTaskDataAccess" for each access.
		 * 
		 * however, if the task is a remote task, it means that the dependencies were
		 * registered earlier when we offload the task inside "remoteTaskCreateAndSubmit".
		 */
		if(!task->isRemoteTask()){
			task->registerDependencies();
		}

		/*
		 * Now that the task accesses have been registered, they need to
		 * be linked to the parent and sibling accesses.
		 */
		if (!task->getDataAccesses()._accesses.empty()) {

			/* Two extra predecessors, so cannot become ready early */
			task->increasePredecessors(2);

#ifndef NDEBUG
			{
				bool alreadyTaken = false;
				assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
			}
#endif
			/*
			 * This part actually inserts the accesses into the dependency system
			 */
			linkTaskAccesses(hpDependencyData, task);
			processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, computePlace, true);

#ifndef NDEBUG
			{
				bool alreadyTaken = true;
				assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
			}
#endif
			/*
			 * Remove the two extra predecessors. The task may become ready.
			 */
			ready = task->decreasePredecessors(2);

			// Special handling for tasks with commutative accesses
			if (ready && (task->getDataAccesses()._totalCommutativeBytes > 0UL)) {
				assert(hpDependencyData._satisfiedCommutativeOriginators.empty());
				assert(hpDependencyData._satisfiedOriginators.empty());

				hpDependencyData._satisfiedCommutativeOriginators.push_back(task);
				processSatisfiedCommutativeOriginators(hpDependencyData);

				if (!hpDependencyData._satisfiedOriginators.empty()) {
					assert(hpDependencyData._satisfiedOriginators.front() == task);
					hpDependencyData._satisfiedOriginators.clear();
				} else {
					// Failed to acquire all the commutative entries
					ready = false;
				}
			}

		} else {
			/*
			 * No accesses: so the task is immediately ready.
			 */
			ready = true;
		}

		Instrument::exitRegisterTaskDataAcesses();

		return ready;
	}


	/*
	 * Release a region accessed by a task
	 *
	 * It is used to (a) support the release directive and (b) for Nanos6@cluster,
	 * handle the receipt of a MessageReleaseAccess when a remote task releases
	 * an access.
	 *
	 */
	void releaseAccessRegion(
		Task *task,  /* The task that is releasing the region */
		DataAccessRegion region,
		__attribute__((unused)) DataAccessType accessType, __attribute__((unused)) bool weak,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData,
		WriteID writeID,
		MemoryPlace const *location,
		bool specifyingDependency
	) {
		Instrument::enterReleaseAccessRegion();
		assert(task != nullptr);

		//! The compute place may be none if it is released from inside a
		//! polling service
		//! assert(computePlace != nullptr);

		TaskDataAccesses &accessStructures = task->getDataAccesses();

		// printf("Node %d: Release acceses for Task: %p -> %p\n",
		// nanos6_get_cluster_node_id(), task, &accessStructures);

		assert(!accessStructures.hasBeenDeleted());
		TaskDataAccesses::accesses_t &accesses = accessStructures._accesses;

#ifndef NDEBUG
		{
			bool alreadyTaken = false;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
		}
#endif

		{
			std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);

			accesses.processIntersecting(
				region,
				/* processor: called for each access that intersects the region */
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);

					if (specifyingDependency) {
						assert(dataAccess->isWeak() == weak);

						FatalErrorHandler::failIf(dataAccess->getType() != accessType,
							"The 'release' construct does not currently support the type downgrade of dependencies; ",
							"the dependency type specified at that construct must be its complete type");
					}

					if (dataAccess->getType() == REDUCTION_ACCESS_TYPE && task->isRunnable()) {
						releaseReductionStorage(task, dataAccess, region, computePlace);
					}

					//! If a valid location has not been provided then we use
					//! the MemoryPlace assigned to the Task but only for non-weak
					//! accesses. For weak accesses we do not want to update the
					//! location of the access
					MemoryPlace const *releaseLocation = nullptr;
					if ((location == nullptr) && !dataAccess->isWeak()) {
						assert(task->hasMemoryPlace());
						releaseLocation = task->getMemoryPlace();
					} else {
						releaseLocation = location;
					}

					dataAccess = fragmentAccess(dataAccess, region, accessStructures);

					finalizeAccess(
						task, dataAccess,
						region, writeID,
						releaseLocation, /* OUT */ hpDependencyData,
						true
					);

					return true;
				});

			processAccessInfo(hpDependencyData);
		}

#ifndef NDEBUG
		{
			bool alreadyTaken = true;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
		}
#endif
		Instrument::exitReleaseAccessRegion();
	}

	void releaseTaskwaitFragment(
		Task *task,
		DataAccessRegion region,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData,
		bool doDelayedOperations)
	{
		assert(task != nullptr);
		Instrument::enterReleaseTaskwaitFragment();

		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());

		{
			// std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
			accessStructures._lock.readLock();
			// accessStructures._lock.writeLock();
			accessStructures._taskwaitFragments.processIntersecting(
				region,
				/* processor: called for each taskwait fragment that intersects the region */
				[&](TaskDataAccesses::taskwait_fragments_t::iterator position) -> bool {
					DataAccess *taskwait = &(*position);

					// Should be fully fragmented already, so the whole fragment becomes complete
					assert(taskwait->getAccessRegion().fullyContainedIn(region));

					/*
					 * Set the taskwait fragment as complete.
					 */
					DataAccessStatusEffects initialStatus(taskwait);
					taskwait->setComplete();
					DataAccessStatusEffects updatedStatus(taskwait);

					handleDataAccessStatusChanges(
						initialStatus, updatedStatus,
						taskwait, accessStructures, task,
						hpDependencyData);

					return true;
				});
			accessStructures._lock.readUnlock();
			// accessStructures._lock.writeUnlock();
		}

		if (doDelayedOperations) {
			processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
				hpDependencyData,
				computePlace,
				true);
		}

		Instrument::exitReleaseTaskwaitFragment();
	}

	void setLocationFromWorkflow(
		DataAccess *access,
		MemoryPlace const *location,
		CPUDependencyData &hpDependencyData)
	{
		Task *task = access->getOriginator();
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		DataAccessStatusEffects initialStatus(access);
		access->setLocation(location);
		DataAccessStatusEffects updatedStatus(access);

		/* Setting the location may cause read satisfiability to be
		 * propagated to the next task in the namespace (for in
		 * dependencies).
		 */
		handleDataAccessStatusChanges(initialStatus,
			updatedStatus, access, accessStructures,
			task, hpDependencyData);
	}

	/*
	 * Update the location information for all data accesses that intersect the
	 * region, fragmenting them if necessary. For clusters this is done when
	 * a data copy completes.
	 */
	void updateTaskDataAccessLocation(Task *origTask,
		DataAccessRegion const &region,
		MemoryPlace const *location,
		bool isTaskwait)
	{
		assert(origTask != nullptr);
		Instrument::enterTaskDataAccessLocation();

		CPUDependencyData hpDependencyData;

		// Take the lock on the task data accesses (all locking in
		// DataAccessRegistration is done on the task data accesses).
		Task *task = origTask;
		while (task != nullptr && !task->isNodeNamespace()) {

			TaskDataAccesses &accessStructures = task->getDataAccesses();
			assert(!accessStructures.hasBeenDeleted());
			std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);

			auto &accesses = (isTaskwait) ? accessStructures._taskwaitFragments : accessStructures._accesses;
			// At this point the region must be included in DataAccesses of the task
			if (task == origTask) {
				assert(accesses.contains(region));
			}

			accesses.processIntersecting(region,
				/* processor: lambda called for every task data access that intersects
				   the region */
				[&](TaskDataAccesses::accesses_t::iterator accessPosition) -> bool {
					DataAccess *access = &(*accessPosition);
					assert(access != nullptr);
					if (!access->complete()) {

						/* fragment the access (if not fully contained inside the region) */
						access = fragmentAccess(access, region, accessStructures);
						DataAccessStatusEffects initialStatus(access);
						access->setLocation(location);

						/* If it is a local location */
						if (location && location->isClusterLocalMemoryPlace()) {

							if (access->isWeak() || access->getType() == READ_ACCESS_TYPE) {
								// Weak or read-only access: register the existing Write ID
								// as local
								WriteIDManager::registerWriteIDasLocal(access->getWriteID(), access->getAccessRegion());
							} else {
								// Otherwise make a new write ID for our updated
								// version and register it as local.
								access->setNewLocalWriteID();
							}
						}

						DataAccessStatusEffects updatedStatus(access);

						/* Setting the location may cause read satisfiability to be
						 * propagated to the next task in the namespace (for in
						 * dependencies).
						 */
						handleDataAccessStatusChanges(initialStatus,
							updatedStatus, access, accessStructures,
							task, hpDependencyData);
					}

					/* always continue with remaining accesses: don't stop here */
					return true;
			});
			task = task->getParent();
			isTaskwait = false;
		}

		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, nullptr, false);
		Instrument::exitTaskDataAccessLocation();
	}

	/*
	 * Register a new data access on a task (after it has started). This
	 * is necessary for dmallocs, because all child data accesses should
	 * be contained within the parent data accesses (?).
	 */
	void registerLocalAccess(
		Task *task,
		DataAccessRegion const &region,
		const MemoryPlace *location = nullptr,
		__attribute__((unused)) bool isStack = false
	) {
		assert(task != nullptr);

		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());

		Instrument::registerTaskAccess(
			task->getInstrumentationTaskId(),
			NO_ACCESS_TYPE,
			false,
			region.getStartAddress(),
			region.getSize()
		);

		/* Take the lock on the task data accesses */
		std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);

		CPUDependencyData hpDependencyData;

		/* Check whether the access is already counted in the dependencies, as part of
		 * an "all" memory region for instance.
		 */
		bool foundIt = false;
		accessStructures._accesses.processIntersecting(
			region,
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				__attribute__((unused)) DataAccess *access = &(*position);
				/* We should never see a partial overlap but this newly
				 * registered region may fragment an existing access.
				 */
				assert(region.fullyContainedIn(access->getAccessRegion()));
				access = fragmentAccess(access, region, accessStructures);

				// Note: it is tempting to change the access, if it is weak,
				// into a strong one (calling access->upgrade). But there is no
				// need, and there is no logic in the dependency system to deal
				// with accesses changing from weak to strong.
				DataAccessStatusEffects initialStatus(access);
				if (location) {

					// Set the location. If it is the current node, then we
					// have to ensure that taskwaits fetch the data (it could
					// be an lmalloc or stack).  We do this by setting the
					// "strong local access" bit in the original access.  It is
					// tempting to instead change the existing access, if it is
					// weak, into a strong one (calling access->upgrade). But
					// there is no logic in the dependency system to deal with
					// accesses changing from weak to strong (e.g. a dependency
					// that previously wasn't enforcing a dependency may now be
					// enforcing one, but the task has already started).

					// Note: dmallocs pass a location of nullptr, so taskwaits
					// in the same auto task will not fetch dmalloc'ed
					// data, even if the home is the current node. Check that
					// this is the case, otherwise we may end up fetching the
					// parts of a dmalloc that are on this node.
					assert(location->isClusterLocalMemoryPlace());

					access->setLocation(location);
					if (location->isClusterLocalMemoryPlace()) {
						access->setIsStrongLocalAccess();
						if (!location->isDirectoryMemoryPlace()) {
							access->setNewLocalWriteID();
						}
					}
				}

				// access->setValidNamespacePrevious(VALID_NAMESPACE_NONE, nullptr);
				// access->setValidNamespaceSelf(VALID_NAMESPACE_NONE);
				DataAccessStatusEffects updatedStatus(access);
				updatedStatus._allowNamespacePropagation = false;

				/* Handle the above data access status changes */
				handleDataAccessStatusChanges(
					initialStatus,
					updatedStatus,
					access,
					accessStructures,
					task,
					hpDependencyData
				);

				foundIt = true;
				return !foundIt;
			});

		if (!foundIt) {

			/* We have not found this access region so create a new one. This
			 * must only happen for the stack (set in WorkerThread) or lmallocs
			 * in any task (allocating data to be used only by the task itself
			 * and its subtasks).
			 *
			 * Note: it is invalid to leave the task without doing the lfree.
			 * If this is an offloaded task or a subtask of an offloaded task,
			 * we may send more MessageReleaseAccess messages than originally
			 * expected, but the DataReleaseStep gets deleted when the counter
			 * on the bytes to release reaches zero. This may cause a
			 * use-after-free in the runtime.
			 */

			/* Create a new access. It will be of NO_ACCESS_TYPE, so the
			 * workflow will determine whether taskwaits fetch the data or not.
			 */
			DataAccess *newLocalAccess = createAccess(
				task,
				access_type,
				NO_ACCESS_TYPE,
				/* not weak */ false,
				region
			);

			/* Modifications to be done after the lock is taken  */
			DataAccessStatusEffects initialStatus(newLocalAccess);
			newLocalAccess->setNewInstrumentationId(task->getInstrumentationTaskId());

			const MemoryPlace *loc = location ? location : Directory::getDirectoryMemoryPlace();
			if (location) {
				newLocalAccess->setIsStrongLocalAccess();
			}
			newLocalAccess->setReadSatisfied(loc);
			if (location && location->isClusterLocalMemoryPlace() && !location->isDirectoryMemoryPlace()) {
				newLocalAccess->setNewLocalWriteID();
			}
			newLocalAccess->setWriteSatisfied();
			newLocalAccess->setConcurrentSatisfied();
			newLocalAccess->setCommutativeSatisfied();
			newLocalAccess->setReceivedReductionInfo();
			newLocalAccess->setValidNamespacePrevious(
				VALID_NAMESPACE_NONE,
				OffloadedTaskIdManager::InvalidOffloadedTaskId
			);
			newLocalAccess->setValidNamespaceSelf(VALID_NAMESPACE_NONE);
			newLocalAccess->setRegistered();
	#ifndef NDEBUG
			newLocalAccess->setReachable();
	#endif
			DataAccessStatusEffects updatedStatus(newLocalAccess);
			//! This is an exception to avoid decreasing predecessor and it
			//! is not used anywhere else.
			updatedStatus.setEnforcesDependency();

			/* Insert the new access (with the lock) */
			accessStructures._accesses.insert(*newLocalAccess);

			/* Handle the above data access status changes */
			handleDataAccessStatusChanges(
				initialStatus,
				updatedStatus,
				newLocalAccess,
				accessStructures,
				task,
				hpDependencyData
			);
		} else { // foundIt
			// There may already be fragments covering the newly registered region. This may happen
			// if part of the access containing the region has already been split, so all of its 
			// fragments have already been created
			if (location) {
				accessStructures._accessFragments.processIntersecting(
					region,
					/* processor: called for each task access fragment */
					[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
						DataAccess *fragment = &(*position);
						assert(fragment != nullptr);

						// If the fragment has a next and the location is already wrong then there is a problem
						assert(!fragment->hasNext() || fragment->getLocation() == location);

						fragment = fragmentAccess(fragment, region, accessStructures);
						DataAccessStatusEffects initialStatus(fragment);
						fragment->setLocation(location);
						if (location->isClusterLocalMemoryPlace()) {
							fragment->setIsStrongLocalAccess();
						}
						DataAccessStatusEffects updatedStatus(fragment);
						updatedStatus._allowNamespacePropagation = false;

						/* Handle the above data access status changes */
						handleDataAccessStatusChanges(
							initialStatus,
							updatedStatus,
							fragment,
							accessStructures,
							task,
							hpDependencyData
						);
						return true;
					});
			};
		}

		/* Do not expect any delayed operations */
		assert (hpDependencyData.empty());
	}

	/*
	 * Unregister a new data access on a task (after it has started). This
	 * is necessary after a dfree.
	 */
	void unregisterLocalAccess(Task *task, DataAccessRegion const &region, bool isStack)
	{
		assert(task != nullptr);

		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());

		using spinlock_t = TaskDataAccesses::spinlock_t;
		using access_fragments_t = TaskDataAccesses::access_fragments_t;
		using accesses_t = TaskDataAccesses::accesses_t;

		std::lock_guard<spinlock_t> guard(accessStructures._lock);

		//! Mark all the accesses intersecting the given region as complete
		bool containedInAccess = false;
		accessStructures._accesses.processIntersecting(region,
			[&](accesses_t::iterator position) -> bool {
				DataAccess *access = &(*position);
				assert(access != nullptr);
				assert(!access->hasBeenDiscounted());
				
				/*
				 * Fragment access, as only part inside region becomes complete.
				 */
				access = fragmentAccess(access, region, accessStructures);
				
				if (access->isStrongLocalAccess()) {
					access->unsetIsStrongLocalAccess();
				}

				if (access->getType() != NO_ACCESS_TYPE) {

					// A local access is typically registered as NO_ACCESS_TYPE. The
					// only way it can be otherwise if the local access is part of a
					// larger access, such as an auto access. In this case, we
					// only have to change the location of the unregistered region to
					// the directory, which indicates that the data is uninitialized.
					// This is needed for correctness, not just performance! If we
					// don't do this, every task with an all memory access would release
					// its own stack to its parent, and we may start copying this freed
					// stack among the nodes. Concretely, assume task A is offloaded from
					// node 0 to 1, then it offloads subtask B back to node 0. If B
					// releases its stack and indicates that it's on node 0, then if A
					// does a taskwait with eager-weak-fetch, the free'd stack will get
					// copied to node 1. Finally, after the taskwait on node 0, the stack
					// will get copied from node 1 to node 0, causing a use-after-free on
					// node 0.
					access->setLocation(Directory::getDirectoryMemoryPlace());
					access->setWriteID(0);
					containedInAccess = true;
					// Now the region in handled in the normal way as part of the larger access.
					return true;
				}

				// A local access (that is not part of a larger access) is created strong.
				assert(!access->isWeak());

				/* Set access as complete */
				DataAccessStatusEffects initialStatus(access);
				access->setComplete();
				DataAccessStatusEffects updatedStatus(access);

				/* Handle consequences of access becoming complete */
				CPUDependencyData hpDependencyData;
				handleDataAccessStatusChanges(initialStatus,
					updatedStatus, access, accessStructures,
					task, hpDependencyData);

				/* Do not expect any delayed operations */
				assert (hpDependencyData.empty());
				return true;
			});

		
		if (!containedInAccess) {
			//! If this local access is not covered by an "all memory" access, then
			//! Mark all the access fragments intersecting the given region as complete
			accessStructures._accessFragments.processIntersecting(region,
				[&](access_fragments_t::iterator position) -> bool {
					DataAccess *fragment = &(*position);
					assert(fragment != nullptr);
					// assert(fragment->getType() == NO_ACCESS_TYPE);

					/* Fragment the access (if not fully contained inside the region).
					   Given that the use case is dmalloc/dfree it seems unlikely. */
					fragment = fragmentAccess(fragment, region,
						accessStructures);

					/* Set access as complete */
					DataAccessStatusEffects initialStatus(fragment);
					fragment->setComplete();
					DataAccessStatusEffects updatedStatus(fragment);

					/* Handle consequences of access becoming complete */
					CPUDependencyData hpDependencyData;
					handleDataAccessStatusChanges(initialStatus,
						updatedStatus, fragment, accessStructures,
						task, hpDependencyData);

					/* Do not expect any delayed operations */
					assert (hpDependencyData.empty());
					return true;
				});

			//! By now all accesses and fragments intersecting the local region
			//! should be removed (unless the local region is covered by an "all
			//! region" access).
			assert(!accessStructures._accesses.contains(region));

			// If at this point there are still any fragments overlapping this
			// region, then we are calling lfree or dfree or removing the stack
			// without doing a taskwait first, which is unsafe. Note: it is not
			// sufficient to do a "taskwait on" covering the whole region.
			// While in principle it is safe, the below error will still be
			// triggered. This is because such fragments will still be on the
			// bottom map and will only be deleted in handleExitTaskwait. Also,
			// this check cannot be done in a task with an "all memory" access,
			// as the fragment will remain anyway. We may have to remove this
			// error or rethink how to do it.
			FatalErrorHandler::failIf(accessStructures._accessFragments.contains(region),
				task->getLabel(),
				isStack ? ": subtask accesses stack after task completion (add taskwait)"
						: ": lfree or dfree without preceding taskwait");
			assert(!accessStructures._accessFragments.contains(region));
		}
	}

	void combineTaskReductions(Task *task, ComputePlace *computePlace)
	{
		assert(task != nullptr);
		assert(computePlace != nullptr);
		assert(task->isRunnable());

		if (task->isTaskfor()) {
			// Loop callaborators only
			TaskDataAccesses &parentAccessStructures = task->getParent()->getDataAccesses();

			assert(!parentAccessStructures.hasBeenDeleted());
			TaskDataAccesses::accesses_t &parentAccesses = parentAccessStructures._accesses;

			std::lock_guard<TaskDataAccesses::spinlock_t> guard(parentAccessStructures._lock);

			// Process parent reduction access and release their storage
			parentAccesses.processAll(
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);

					if (dataAccess->getType() == REDUCTION_ACCESS_TYPE) {
						releaseReductionStorage(task->getParent(), dataAccess, dataAccess->getAccessRegion(), computePlace);
					}
					return true;
				});
		}

		TaskDataAccesses &accessStructures = task->getDataAccesses();

		assert(!accessStructures.hasBeenDeleted());
		TaskDataAccesses::accesses_t &accesses = accessStructures._accesses;

		std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);

		accesses.processAll(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *dataAccess = &(*position);
				assert(dataAccess != nullptr);

				if (dataAccess->getType() == REDUCTION_ACCESS_TYPE) {
					releaseReductionStorage(task, dataAccess, dataAccess->getAccessRegion(), computePlace);
				}
				return true;
			});
	}


	void unregisterLocallyPropagatedTaskDataAccesses(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData)
	{
		if (!task->mustDelayRelease()                   // No wait clause (implied by autowait)
			|| !task->delayedReleaseNonLocalOnly()      // or not non-local release only */
			|| !task->isRemoteTaskInNamespace()) {      // or not an offloaded task
			/* Do nothing */
			return;
		}

		// Lock the access structures
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		TaskDataAccesses::accesses_t &accesses = accessStructures._accesses;
		bool didAll = true;
		{
			std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);

			unfragmentTaskAccesses(task, accessStructures, false);

			/* Finalize all local accesses. */
			accesses.processAll(
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);

					bool finalizeNow =
						(dataAccess->getType() == AUTO_ACCESS_TYPE
							&& ClusterManager::autoOptimizeNonAccessed()
							&& !dataAccess->isStrongLocalAccess()
							&& accessStructures._subaccessBottomMap.empty()) // Allmemory access with no subaccesses
							||
						(dataAccess->hasNext()
						&& !dataAccess->getNamespaceNextIsIn()); // or early release in namespace


					// Do we need to finalize it?
					if (finalizeNow) {
						// assert(!dataAccess->isInBottomMap());
						dataAccess->setEarlyReleaseInNamespace();

						finalizeAccess(
							task, dataAccess,
							dataAccess->getAccessRegion(), 0,
							nullptr, /* OUT */ hpDependencyData,
							true
						);

					} else {
						didAll = false;
					}
					return true;
				});
			processAccessInfo(hpDependencyData);
		}
		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, computePlace, true);
		if (didAll) {
			/* If all accesses have already been finalized then we don't need to perform
			 * delayed release (and it would hang).
			 */
			task->completeDelayedRelease();
		}
	}


	/*
	 * First part of unregistering all the task data accesses (when the task
	 * completes). Handle the accesses themselves and any dependent accesses
	 * within the same task. But do not propagate to any other tasks yet.  This
	 * gives a safe point at which to dispose of the task, and for clusters
	 * send the MessageTaskFinished, BEFORE any effects on other tasks, which
	 * may also send MessageTaskFinished (as it means passing on read and write
	 * satisfiabilities). If this is not done carefully, then the namespace
	 * implementation may send MessageTaskFinished out of order.
	 */
	void unregisterTaskDataAccesses1(Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData,
		MemoryPlace *location,
		bool fromBusyThread
	) {
		(void)computePlace;
		(void)fromBusyThread;
		assert(task != nullptr);

		Instrument::enterUnregisterTaskDataAcesses();

		TaskDataAccesses &accessStructures = task->getDataAccesses();

		assert(!accessStructures.hasBeenDeleted());
		TaskDataAccesses::accesses_t &accesses = accessStructures._accesses;

		// This is an optimization to merge dependencies of consecutive regions with the same
		// characteristics. One of the performance issues detected with the regions dependency
		// system is the excessive cost of processDelayedOperationsSameTask.
		{
			accessStructures._lock.lock();
			unfragmentTaskAccesses(task, accessStructures, false);
			accessStructures._lock.unlock();
		}

		//! If a valid location has not been provided then we use
		//! the MemoryPlace assigned to the Task
		if (location == nullptr) {
			assert(task->hasMemoryPlace());
			location = task->getMemoryPlace();
		}
#ifndef NDEBUG
		{
			bool alreadyTaken = false;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
		}
#endif

		{
			accessStructures._lock.lock();

			if (task->isRemoteTask()) {
				// Remote tasks without wait or autowait require a top-level sink for
				// all accesses to collect the release regions to send back to the offloader.
				// If the task has wait or autowait, then the bottom map will already
				// be empty, so no top-level sinks will be created.
				createTopLevelSink(task, accessStructures, hpDependencyData);
			}

			if (task->isOffloadedTask()) {

				/* This task was executed on another node. All non-complete
				 * accesses that remain at this point are either (1) accesses
				 * that were propagated on the remote node or (2) part of a
				 * weakconcurrent access that was not accessed by a strong
				 * subtask or (3) unaccessed auto regions.  Otherwise they
				 * would have been set to complete by releaseAccessRegion,
				 * which is called on receipt of MessageReleaseAccess.  There
				 * should also be no fragments, since the task was not executed
				 * here. All non-concurrent accesses can therefore simply be
				 * removed, since they will never be accessed again on the
				 * current node.
				 */
				 assert(accessStructures._accessFragments.empty());
				 accesses.processAll(
				 	[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
						DataAccess *dataAccess = &(*position);
						assert(dataAccess != nullptr);
						if (!dataAccess->complete()) {
							if (dataAccess->getType() == AUTO_ACCESS_TYPE
										&& dataAccess->getDisableEagerSend()) {
								DataAccessStatusEffects initialStatus(dataAccess);
								dataAccess->setComplete();
								DataAccessStatusEffects updatedStatus(dataAccess);

								handleDataAccessStatusChanges(
									initialStatus, updatedStatus,
									dataAccess, accessStructures, dataAccess->getOriginator(),
									hpDependencyData);
							} else if (!dataAccess->remoteHasPseudowrite()
								&& dataAccess->getType() != CONCURRENT_ACCESS_TYPE) {
								// The remote end is propagating true write satisfiability
								// We will only get this far if the remote task has received true
								// write satisfiability, so we will never receive write satisfiability
								// here if we haven't already. We can just delete the access.
								assert(dataAccess->hasNext());
								dataAccess->markAsDiscounted();
								accessStructures._accesses.erase(dataAccess);
								ObjectAllocator<DataAccess>::deleteObject(dataAccess);

								assert(accessStructures._removalBlockers > 0);
								accessStructures._removalBlockers--;
								if (accessStructures._removalBlockers == 0) {
									if (task->decreaseRemovalBlockingCount()) {
										hpDependencyData._removableTasks.push_back(task);
									}
								}
							} else {
								// The remote end only has pseudowrite. This means that its
								// write satisfiability only means that all offloaded read
								// accesses on that node, that were propagated through the
								// namespace, have completed. We also propagate write
								// satisfiability on this node (which may be true write
								// satisfiability or in turn pseudowrite satisfiability
								// from another node). Only once we get write satisfiability
								// here do we know that all in accesses of tasks created on
								// this node have completed. Just set the complete bit and
								// let the normal mechanism check whether the access can be
								// deleted already.
								// If propagated remotely (i.e. not concurrent) there must be a next access
								assert(dataAccess->getType() == CONCURRENT_ACCESS_TYPE
										|| dataAccess->hasNext());
								assert(dataAccess->getType() == READ_ACCESS_TYPE
										|| dataAccess->getType() == CONCURRENT_ACCESS_TYPE);

								DataAccessStatusEffects initialStatus(dataAccess);
								dataAccess->setComplete();
								DataAccessStatusEffects updatedStatus(dataAccess);

								handleDataAccessStatusChanges(
									initialStatus, updatedStatus,
									dataAccess, accessStructures, dataAccess->getOriginator(),
									hpDependencyData);
							}
						}

						/* Keep going for all accesses */
						return true;
					});
			} else { // task->isOffloadedTask()

				/* The task was executed here. Finalize all accesses.
				 */
				accesses.processAll(
					[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
						DataAccess *dataAccess = &(*position);
						assert(dataAccess != nullptr);

						// If the access is already complete, it was handled by
						//  unregisterLocallyPropagatedTaskDataAccesses.
						if (dataAccess->complete()) {
							assert(dataAccess->hasNext() || dataAccess->getType() == AUTO_ACCESS_TYPE);
							return true;
						}

						if (dataAccess->getType() == NO_ACCESS_TYPE) {
							// Finalize an access of NO_ACCESS_TYPE. This is a region from
							// registerLocalAccess, i.e. an lmalloc'ed (or dmalloc'ed) region that
							// has not been unregistered. It is not part of one of the original
							// task accesses, otherwise it would have the other access type. We
							// check whether there are any tasks on the bottom map accessing this
							// region. If there are, then they need to have their "on bottom map"
							// status bit cleared, as they will never have a next and would otherwise
							// never become removable.
							accessStructures._subaccessBottomMap.processIntersecting(
								dataAccess->getAccessRegion(),
								[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
									BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
									assert(bottomMapEntry != nullptr);

									DataAccessLink target = bottomMapEntry->_link;
									DataAccessRegion subregion = bottomMapEntry->_region;
									followLink(
										target,
										subregion,
										[&](DataAccess *previous) -> bool {
											previous->unsetInBottomMap();
											return true;
										}
									);
									accessStructures._subaccessBottomMap.erase(*bottomMapEntry);
									ObjectAllocator<BottomMapEntry>::deleteObject(bottomMapEntry);
									return true;
								}
							);
						}

						finalizeAccess(
							task, dataAccess,
							dataAccess->getAccessRegion(), 0,
							nullptr, /* OUT */ hpDependencyData,
							false
						);

						return true;
					});
				processAccessInfo(hpDependencyData);
			}

			if (task->isRemoteTask()) {
				// Now that the accesses have been finalized, we can set the top-level sink
				// fragments, if we have any, to complete. Waiting until now prevents any
				// data releases before the top-level sink has been put on the namespace's
				// bottom map. It avoids a nasty race condition where:
				// 1. We make the top-level sink and set it to complete
				// 2. We release the lock to make the previous bottom map access have the
				//    top-level sink as next.
				// 3. The previous bottom map access completes, and since the top-level sink
				//    is complete, it triggers the data release and the top-level sink is
				//    discounted.
				// 4. Since our task's original access is still on the namespace's bottom
				//    map, another task can be linked in as the next.
				// 5. The next task on the namespace expects to get satisfiability in the
				//    namespace (and ignores "redundant" satisfiability from the parent).
				//    But it will never receive it so hangs.
				// This is best solved by not setting the top-level sink as complete until
				// after it has been linked into the namespace's bottom map.
				accessStructures._taskwaitFragments.processAll(
					/* processor: called for each task access fragment */
					[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
						DataAccess *taskwaitFragment = &(*position);
						assert(taskwaitFragment != nullptr);
						assert(taskwaitFragment->getObjectType() == top_level_sink_type);
						DataAccessStatusEffects initialStatus(taskwaitFragment);
						taskwaitFragment->setComplete();
						DataAccessStatusEffects updatedStatus(taskwaitFragment);

						/* Handle consequences of access becoming complete */
						handleDataAccessStatusChanges(
							initialStatus, updatedStatus,
							taskwaitFragment, accessStructures, task,
							hpDependencyData);
						return true;
					}
				);
			}

			// Process all delayed operations that do not involve
			// remote namespace propagation, i.e. among subtasks of the
			// same offloaded task (or among any subtasks that are not
			// descendents of any offloaded task). If delayed operations
			// could update later offloaded tasks, it would be possible
			// for the later offloaded tasks to complete and send
			// a MessageTaskFinished before sending the MessageTaskFinished
			// for the current task.
			// Enter function with lock on access structures, leave it without lock
			processDelayedOperationsSameTask(hpDependencyData, task);
			// assert(!accessStructures._lock.isLockedByThisThread());
		}
		Instrument::enterUnregisterTaskDataAcessesCallback();

		Task *offloadedTask = task->getOffloadedPredecesor();
		if (offloadedTask && offloadedTask->hasDataReleaseStep() && offloadedTask->hasNowait()) {
			offloadedTask->getDataReleaseStep()->releasePendingAccesses(false);
		}
	}

	/*
	 * Second part of unregistering all the task data accesses (when the task
	 * completes). Handle any effects on other tasks.
	 */
	void unregisterTaskDataAccesses2(ComputePlace *computePlace,
									CPUDependencyData &hpDependencyData,
									MemoryPlace *location,
									bool fromBusyThread)
	{
		Instrument::enterUnregisterTaskDataAcesses2();
		(void)location;

		assert(ClusterManager::inClusterMode() || hpDependencyData._delayedOperations.empty());

		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
			hpDependencyData,
			computePlace,
			fromBusyThread
		);

#ifndef NDEBUG
		{
			bool alreadyTaken = true;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
		}
#endif
		Instrument::exitUnregisterTaskDataAcesses2();
	}

	/*
	 * Called on receipt of MessageSatisfiability. Propagates satisfiability
	 * from the workflow into the dependency system.
	 */
#ifdef USE_CLUSTER
	void propagateSatisfiability(Task *task, DataAccessRegion const &region,
		ComputePlace *computePlace, CPUDependencyData &hpDependencyData,
		bool readSatisfied,    /* Change in read satisfiability (not new value) */
		bool writeSatisfied,   /* Change in write satisfiability (not new value) */
		WriteID writeID,
		MemoryPlace const *location,
		OffloadedTaskIdManager::OffloadedTaskId namespacePredecessor)
	{
		Instrument::enterPropagateSatisfiability();
		assert(task != nullptr);

		/* At least one of read or write satisfied (maybe both) must be changing */
		assert(readSatisfied
			|| writeSatisfied
			|| (namespacePredecessor != OffloadedTaskIdManager::InvalidOffloadedTaskId)
		);

		/*
		 * Create an update operation with the satisfiability information.
		 * It affects the task's accesses ("access_type"), not the fragments
		 * (which relate to its children) or taskwaits.
		 */
		UpdateOperation updateOperation;
		updateOperation._target = DataAccessLink(task, access_type);
		updateOperation._region = region;

		updateOperation._makeReadSatisfied = readSatisfied;
		updateOperation._makeWriteSatisfied = writeSatisfied;

		updateOperation._location = location;
		updateOperation._writeID = writeID;
		updateOperation._propagateSatisfiability = true;
		if (namespacePredecessor != OffloadedTaskIdManager::InvalidOffloadedTaskId) {
			updateOperation._validNamespace = ClusterManager::getCurrentClusterNode()->getIndex();
			updateOperation._namespacePredecessor = namespacePredecessor;
			updateOperation._namespaceAccessType = NO_ACCESS_TYPE; // actually means any access type (in was checked at offloader side)
		}

		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());

#ifndef NDEBUG
		{
			/* No other code should be using this hpDependencyData */
			bool alreadyTaken = false;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
		}
#endif

		{
			// Process the update operation (which requires the lock to be taken on the task's
			// access structures).
			std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);
			processUpdateOperation(updateOperation, hpDependencyData);
		}

		/*
		 * Finish processing with the operations that require locks other than
		 * the task's access structures.
		 */
		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
			hpDependencyData, computePlace, /* fromBusyThread */ true
		);

#ifndef NDEBUG
		{
			/* Allow other code to use this hpDependencyData */
			bool alreadyTaken = true;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
		}
#endif
		Instrument::exitPropagateSatisfiability();
	}
#endif // USE_CLUSTER

	/*
	 * Enter a taskwait (called from nanos6_taskwait).
	 *
	 * It creates taskwait fragments for all entries in the bottom map.
	 */
	void handleEnterTaskwait(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData,
		bool noflush,
		bool nonLocalOnly
	) {
		Instrument::enterHandleEnterTaskwait();
		assert(task != nullptr);

#ifndef NDEBUG
		{
			bool alreadyTaken = false;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, true));
		}
#endif

		{
			TaskDataAccesses &accessStructures = task->getDataAccesses();
			assert(!accessStructures.hasBeenDeleted());
			std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);

			// For tasks with wait or autowait, send AccessInfo messages for any regions
			// that were never accessed by the task or a subtask
			if (task->hasFinished()
				&& task->isRemoteTask()
				&& (ClusterManager::getEagerSend() || (task->hasAllMemory() && ClusterManager::autoOptimizeNonAccessed()))) {
				accessStructures._accesses.processAll(
					/* processor: called for each task access */
					[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
						DataAccess *access = &(*position);
						assert(access != nullptr);
						if (!access->getPropagateFromNamespace()
							&& !access->isAutoHasBeenAccessed()
							&& (ClusterManager::getEagerSend() ||
								(access->getType() == AUTO_ACCESS_TYPE && !access->isStrongLocalAccess()))
							&& !access->getDisableEagerSend()) {
								ClusterNode *node = task->getClusterContext()->getRemoteNode();
								if (!access->hasSubaccesses()) {
									access->setDisableEagerSend();
									hpDependencyData._accessInfoMap[node].emplace_back(
										access->getAccessRegion(),
										access->getOriginator()->getOffloadedTaskId(),
										/* noEagerSend */ true,
										/* isReadOnly */ false,
										access->getOriginator());
								} else {
									accessStructures._accessFragments.processIntersecting(
										access->getAccessRegion(),
										[&](TaskDataAccesses::access_fragments_t::iterator fragmentPosition) -> bool {
											DataAccess *fragment = &(*fragmentPosition);
											if (!fragment->hasNext() && !fragment->getDisableEagerSend()) {
												fragment->setDisableEagerSend();
												hpDependencyData._accessInfoMap[node].emplace_back(
													fragment->getAccessRegion(),
													access->getOriginator()->getOffloadedTaskId(),
													/* noEagerSend */ true,
													/* isReadOnly */ false,
													access->getOriginator());
											}
											return true;
										}
									);
								}
							}
						return true;
					}
				);
				processAccessInfo(hpDependencyData);
			}

			/* Create a taskwait fragment for each entry in the bottom map */
			createTaskwait(task, accessStructures, computePlace, hpDependencyData, noflush, nonLocalOnly);

			finalizeFragments(task, accessStructures, hpDependencyData);

		}
		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
			hpDependencyData, computePlace, true
		);

#ifndef NDEBUG
		{
			bool alreadyTaken = true;
			assert(hpDependencyData._inUse.compare_exchange_strong(alreadyTaken, false));
		}
#endif
		Instrument::exitHandleEnterTaskwait();
	}


	void handleExitTaskwait(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData
	) {
		Instrument::enterHandleExitTaskwait();
		assert(task != nullptr);

		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());
		std::lock_guard<TaskDataAccesses::spinlock_t> guard(accessStructures._lock);

		unfragmentTaskAccesses(task, accessStructures, false);
		if (!accessStructures._accesses.empty()) {
			// Mark all accesses as not having subaccesses (meaning fragments,
			// as they will all be deleted below
			accessStructures._accesses.processAll(
				/* processor: called for every access */
				[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);

					// When handleExitTaskwait is called by Task::markAsFinished
					// for a task with a wait clause, then the access may be discounted
					// assert(!dataAccess->hasBeenDiscounted());

					if (!dataAccess->getEarlyReleaseInNamespace()) {
						if (dataAccess->hasSubaccesses()) {
							dataAccess->unsetHasSubaccesses();
						}
					}

					/* continue, to process all accesses */
					return true;
				});

			// Delete all fragments. These are created when child task accesses when the tasks are
			// submitted, and are no longer needed now that all child tasks have finished.
			accessStructures._accessFragments.processAll(
				/* processor: called for every fragment */
				[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
					DataAccess *dataAccess = &(*position);
					assert(dataAccess != nullptr);
					if (!dataAccess->getEarlyReleaseInNamespace()) {
						Instrument::removedDataAccess(dataAccess->getInstrumentationId());
						accessStructures._accessFragments.erase(dataAccess);
						ObjectAllocator<DataAccess>::deleteObject(dataAccess);
					}

					/* continue, to process all access fragments */
					return true;
				});
		}

		unfragmentTaskwaits(accessStructures);
		accessStructures._taskwaitFragments.processAll(
			/* processor: called for each task access fragment */
			[&](TaskDataAccesses::access_fragments_t::iterator position) -> bool {
				DataAccess *taskwaitFragment = &(*position);
				assert(taskwaitFragment != nullptr);

#ifndef NDEBUG
				DataAccessStatusEffects initialStatus(taskwaitFragment);
				assert(initialStatus._isRemovable);
#endif
				assert(!taskwaitFragment->hasNext());
				assert ((taskwaitFragment->getObjectType() == taskwait_type)
					|| (taskwaitFragment->getObjectType() == top_level_sink_type));
				taskwaitFragment->markAsDiscounted();
				removeBottomMapTaskwaitOrTopLevelSink(
					taskwaitFragment, accessStructures, task, hpDependencyData
				);
				accessStructures._removalBlockers--;
				if (accessStructures._removalBlockers == 0) {
					if (task->decreaseRemovalBlockingCount()) {
						hpDependencyData._removableTasks.push_back(task);
					}
				}
				return true;
			}
		);

		// If removing the bottom map taskwait / top level sink set the
		// location of an access from non-local to local, we may need to
		// propagate read satisfiability to the next task (which may also
		// make it ready). Do it now.
		if (!hpDependencyData.empty()) {
			accessStructures._lock.unlock();
			processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
				hpDependencyData, computePlace, true
			);
			accessStructures._lock.lock();
		}

		// Delete all taskwait fragments
		accessStructures._taskwaitFragments.processAll(
			[&](TaskDataAccesses::taskwait_fragments_t::iterator position) -> bool {
				DataAccess *dataAccess = &(*position);
				assert(dataAccess != nullptr);

#ifndef NDEBUG
				DataAccessStatusEffects currentStatus(dataAccess);
				assert(currentStatus._isRemovable);
#endif

				Instrument::removedDataAccess(dataAccess->getInstrumentationId());
				accessStructures._taskwaitFragments.erase(dataAccess);
				ObjectAllocator<DataAccess>::deleteObject(dataAccess);

				/* continue, to process all taskwait fragments */
				return true;
			});
		accessStructures._taskwaitFragments.clear();

		// Clean up the bottom map
		accessStructures._subaccessBottomMap.processAll(
			/* processor: called for every bottom map entry */
			[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
				// Erase the bottom map entry.
				BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
				assert(bottomMapEntry != nullptr);
				// Locally propagated accesses (autowait) don't get a taskwait fragment
				// assert((bottomMapEntry->_link._objectType == taskwait_type) || (bottomMapEntry->_link._objectType == top_level_sink_type));

				accessStructures._subaccessBottomMap.erase(bottomMapEntry);
				ObjectAllocator<BottomMapEntry>::deleteObject(bottomMapEntry);

				/* continue, to process all bottom map entries */
				return true;
			});
		assert(accessStructures._subaccessBottomMap.empty());
		Instrument::exitHandleExitTaskwait();
	}

	void translateReductionAddresses(
		Task *task, ComputePlace *computePlace,
		nanos6_address_translation_entry_t *translationTable,
		int totalSymbols
	) {
		assert(task != nullptr);
		assert(computePlace != nullptr);
		assert(translationTable != nullptr);

		// Initialize translationTable
		for (int i = 0; i < totalSymbols; ++i)
			translationTable[i] = {0, 0};

		TaskDataAccesses &accessStruct = task->getDataAccesses();

		assert(!accessStruct.hasBeenDeleted());
		accessStruct._lock.lock();

		accessStruct._accesses.processAll(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *dataAccess = &(*position);
				assert(dataAccess != nullptr);

				if (dataAccess->getType() == REDUCTION_ACCESS_TYPE && !dataAccess->isWeak()) {
					FatalErrorHandler::failIf(computePlace->getType() != nanos6_host_device,
						"Region dependencies do not support CUDA reductions");

					ReductionInfo *reductionInfo = dataAccess->getReductionInfo();
					assert(reductionInfo != nullptr);

					size_t slotIndex = reductionInfo->getFreeSlotIndex(computePlace->getIndex());

					// Register assigned slot in the data access
					dataAccess->setReductionAccessedSlot(slotIndex);

					void *address = dataAccess->getAccessRegion().getStartAddress();
					void *translation = nullptr;
					const DataAccessRegion &originalFullRegion = reductionInfo->getOriginalRegion();
					translation = ((char *)reductionInfo->getFreeSlotStorage(slotIndex).getStartAddress()) + ((char *)address - (char *)originalFullRegion.getStartAddress());

					// As we're iterating accesses that might have been split by sibling tasks, it is
					// possible that we translate the same symbol twice. However, this is not an issue
					// because symbol translation is relative and it is not mandatory for "address"
					// to be equal to the first position of the translated symbol
					for (int j = 0; j < totalSymbols; ++j) {
						if (dataAccess->isInSymbol(j))
							translationTable[j] = {(size_t)address, (size_t)translation};
					}
				}

				return true;
			});

		accessStruct._lock.unlock();
	}

	// NOTE: you must call setNamespaceSelf with the lock on the data structures
	// Then call setNamespaceSelfDone without the lock
	void setNamespaceSelf(DataAccess *access, int targetNamespace, CPUDependencyData &hpDependencyData)
	{
		// This is called with the lock on the task accesses already taken
		Task *task = access->getOriginator();
		TaskDataAccesses &accessStructures = task->getDataAccesses();
		assert(!accessStructures.hasBeenDeleted());

		DataAccessStatusEffects initialStatus(access);
		access->setValidNamespaceSelf(targetNamespace);
		DataAccessStatusEffects updatedStatus(access);

		handleDataAccessStatusChanges(
			initialStatus, updatedStatus,
			access, accessStructures, access->getOriginator(),
			hpDependencyData);
	}

	// Remove a list of regions from the namespace's bottom map.
	// This must be done before the tasks are deleted. Note: some
	// of the tasks may already be deleted! We can only know they
	// are still alive if they have entries on the bottom map.
	static void removeFromNamespaceBottomMap(CPUDependencyData &hpDependencyData)
	{
		if (hpDependencyData._namespaceRegionsToRemove.empty()) {
			return;
		}
		Task *parent = NodeNamespace::getNamespaceTask();
		Task *lastLocked = nullptr;

		{
			TaskDataAccesses &parentAccessStructures = parent->getDataAccesses();
			std::lock_guard<TaskDataAccesses::spinlock_t> guardParent(parentAccessStructures._lock);

			for(CPUDependencyData::TaskAndRegion taskAndRegion : hpDependencyData._namespaceRegionsToRemove) {
				DataAccessRegion region = taskAndRegion._region;

				// We must not dereference the task unless it is still in the bottom map!
				// If it is, then remove it from the namespace bottom map
				bool wasInBottomMap = false;
				parentAccessStructures._subaccessBottomMap.processIntersecting(
					region,
					/* processor: called with each part of the bottom map that intersects region */
					[&](TaskDataAccesses::subaccess_bottom_map_t::iterator bottomMapPosition) -> bool {
						BottomMapEntry *bottomMapEntry = &(*bottomMapPosition);
						assert(bottomMapEntry != nullptr);
						bottomMapEntry = fragmentBottomMapEntry(bottomMapEntry, region, parentAccessStructures);
						DataAccessLink target = bottomMapEntry->_link;
						if (target._task == taskAndRegion._task) {
							wasInBottomMap = true;
							parentAccessStructures._subaccessBottomMap.erase(bottomMapEntry);
							ObjectAllocator<BottomMapEntry>::deleteObject(bottomMapEntry);
						}
						return true;
					}
				);
				if (!wasInBottomMap) {
					continue;
				}

				Task *task = taskAndRegion._task;
				assert(task != nullptr);
				assert(task->isRemoteTask());
				TaskDataAccesses &accessStructures = task->getDataAccesses();

				if (task != lastLocked) {
					if (lastLocked) {
						lastLocked->getDataAccesses()._lock.unlock();
					}
					accessStructures._lock.lock();
					lastLocked = task;
				}
				accessStructures._accesses.processIntersecting(
					region,
					[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
						DataAccess *access = &(*position);
						assert(access != nullptr);
						assert (!access->hasNext());

						if (access->isInBottomMap()) {
							access = fragmentAccess(access, region, accessStructures);
							DataAccessStatusEffects initialStatus(access);
							access->unsetInBottomMap();
							DataAccessStatusEffects updatedStatus(access);
							handleDataAccessStatusChanges(
								initialStatus, updatedStatus,
								access, accessStructures, task,
								hpDependencyData);
						}
						return true;
					}
				);
			}
		}
		if (lastLocked != nullptr) {
			lastLocked->getDataAccesses()._lock.unlock();
		}
		hpDependencyData._namespaceRegionsToRemove.clear();
		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, nullptr, false);
	}

	void accessInfo(Task *task, DataAccessRegion region, CPUDependencyData &hpDependencyData, bool noEagerSend, __attribute__((unused)) bool isReadOnly)
	{
		assert(noEagerSend || isReadOnly);
		TaskDataAccesses &accessStructures = task->getDataAccesses();

		accessStructures._lock.lock();
		accessStructures._accesses.processIntersecting(
			region,
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *dataAccess = &(*position);
				assert(dataAccess != nullptr);
				assert(!dataAccess->complete());
				assert(!dataAccess->hasBeenDiscounted());
				dataAccess = fragmentAccess(dataAccess, region, accessStructures);

				if (dataAccess->getType() == AUTO_ACCESS_TYPE && ClusterManager::autoOptimizeNonAccessed()) {
					DataAccessStatusEffects initialStatus(dataAccess);
					if (noEagerSend) {
						assert(!dataAccess->getDisableEagerSend());
						dataAccess->setDisableEagerSend();

						ExecutionWorkflow::DataLinkStep *step = dataAccess->getDataLinkStep();
						bool linksRead = !dataAccess->readSatisfied();
						bool linksWrite = !dataAccess->writeSatisfied();
						if (step) {
							if (linksRead || linksWrite) {
								step->linkRegion(dataAccess, linksRead, linksWrite, hpDependencyData._satisfiabilityMap, hpDependencyData._dataSendRegionInfoMap);
							}
						}
					} else {
						assert(isReadOnly);
						assert(!dataAccess->isAutoReadOnly());
						dataAccess->setAutoReadOnly();
					}
					DataAccessStatusEffects updatedStatus(dataAccess);
					handleDataAccessStatusChanges(
						initialStatus, updatedStatus,
						dataAccess, accessStructures, task,
						hpDependencyData);

				} else {
					if (noEagerSend) {
						dataAccess->setDisableEagerSend();
					}
				}

				return true;
			}
		);
		accessStructures._lock.unlock();
		processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(hpDependencyData, nullptr, true);
	}

	bool supportsDataTracking()
	{
		return true;
	}
}; // namespace DataAccessRegistration

#pragma GCC visibility pop

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <map>
#include <utility>
#include <vector>

#include <nanos6/task-instantiation.h>

#include "ClusterTaskContext.hpp"
#include "TaskOffloading.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "system/ompss/AddTask.hpp"
#include "tasks/Task.hpp"
#include "tasks/Taskfor.hpp"
#include "tasks/Taskloop.hpp"
#include "executors/threads/TaskFinalization.hpp"

#include <ClusterManager.hpp>
#include <RemoteTasksInfoMap.hpp>
#include <DataAccessRegistration.hpp>
#include <Directory.hpp>
#include <MessageTaskNew.hpp>
#include "MessageSatisfiability.hpp"
#include <MessageAccessInfo.hpp>
#include <MessageDataSend.hpp>
#include <NodeNamespace.hpp>

#include "cluster/WriteID.hpp"
#include "cluster/polling-services/PendingQueue.hpp"
#include "OffloadedTasksInfoMap.hpp"
#include <LiveDataTransfers.hpp>
#include "scheduling/Scheduler.hpp"
#include "cluster/ClusterMetrics.hpp"
#include "dependencies/SymbolTranslation.hpp"

namespace TaskOffloading {

	template <class T>
	static void mergeList(const std::vector<T> &invec, std::vector <T> &outvec)
	{
		assert(outvec.empty());
		outvec.reserve(invec.size());
		T *last = nullptr;
		for(const T &in : invec) {
			if (last) {
				// Accesses must already be ordered correctly
				assert(in._region.getStartAddress() >= last->_region.getEndAddress());
			}

			if (last && in.canMergeWith(*last)) {
				last->_region = DataAccessRegion(last->_region.getStartAddress(), in._region.getEndAddress());
			} else {
				outvec.emplace_back(in);
				last = &outvec.back();
			}
		}
	}

	static void propagateSatisfiability(
		Task *localTask,
		SatisfiabilityInfo const &satInfo,
		CPU * const cpu,
		OffloadedTaskIdManager::OffloadedTaskId namespacePredecessor,
		CPUDependencyData &hpDependencyData
	) {
		assert(localTask != nullptr);

		// Convert integer source id to a pointer to the relevant MemoryPlace -1 means nullptr: see
		// comment in ClusterDataLinkStep::linkRegion().  It happens for race conditions when write
		// satisfiability is propagated before read satisfiability.  Otherwise it is either the node
		// index or the directory (which is used for uninitialized memory regions).
		MemoryPlace const *loc =
			(satInfo._src == -1) ? nullptr : ClusterManager::getMemoryNodeOrDirectory(satInfo._src);

		DataAccessRegistration::propagateSatisfiability(
			localTask, satInfo._region, cpu,
			hpDependencyData,
			satInfo._readSat, satInfo._writeSat, satInfo._writeID, loc, namespacePredecessor
		);
	}

	void offloadTask(
		Task *task,
		SatisfiabilityInfoVector const &satInfo,
		ClusterNode *remoteNode
	) {
		assert(task != nullptr);
		assert(remoteNode != nullptr);

		remoteNode->incNumOffloadedTasks(1);
		nanos6_task_info_t *taskInfo = task->getTaskInfo();
		nanos6_task_invocation_info_t *taskInvocationInfo = task->getTaskInvokationInfo();
		size_t flags = task->getFlags();
		void *argsBlock = task->getArgsBlock();
		size_t argsBlockSize = task->getArgsBlockSize();
		size_t nrSatInfo = satInfo.size();
		SatisfiabilityInfo const *satInfoPtr = (nrSatInfo == 0) ? nullptr : satInfo.data();

		OffloadedTaskIdManager::OffloadedTaskId taskId = task->getOffloadedTaskId();
		OffloadedTasksInfoMap::createOffloadedTaskInfo(taskId, task, remoteNode);

		Instrument::taskIsOffloaded(task->getInstrumentationTaskId());
		task->markAsOffloaded();

		// Allocate private storage to receive reductions if needed
		nanos6_address_translation_entry_t
			stackTranslationTable[SymbolTranslation::MAX_STACK_SYMBOLS];
		size_t tableSize = 0;
		nanos6_address_translation_entry_t *translationTable =
			SymbolTranslation::generateTranslationTable(
				task, remoteNode, stackTranslationTable, tableSize);
		// Free up all symbol translation
		if (tableSize > 0) {
			MemoryAllocator::free(translationTable, tableSize);
		}

		MessageTaskNew *msg = new MessageTaskNew(
			taskInfo, taskInvocationInfo, flags,
			taskInfo->implementation_count, taskInfo->implementations,
			nrSatInfo, satInfoPtr,
			argsBlockSize, argsBlock,
			taskId
		);

		// If this offloaded task is a taskfor, then include the loop bounds in the message.
		if (task->isTaskforSource()) {
			Taskfor *taskfor = static_cast<Taskfor *>(task);
			msg->setBounds(taskfor->getBounds());
		} else if (task->isTaskloop()) {
			Taskloop *taskloop = static_cast<Taskloop *>(task);
			msg->setBounds(taskloop->getBounds());
		}

		ClusterMetrics::incSentNumNewTask();
		ClusterManager::sendMessage(msg, remoteNode);

		// Offloaded tasks do not need the "wait" clause, since any waiting will be handled
		// already at the remote side.
		task->setEarlyRelease(nanos6_no_wait);
	}

	void sendSatisfiability(
		SatisfiabilityInfoMap &satInfoMap) {
		if (!ClusterManager::getGroupMessagesEnabled()) {
			assert(satInfoMap.empty());
		}

		if (satInfoMap.empty()) {
			return;
		}

		for (auto &it: satInfoMap) {
			assert(it.first != nullptr);
			assert(it.first != ClusterManager::getCurrentClusterNode());
			MessageSatisfiability *msg = new MessageSatisfiability(it.second);
			ClusterManager::sendMessage(msg, it.first);
		}
		satInfoMap.clear();

	}


	void propagateSatisfiabilityForHandler(
		__attribute__((unused)) ClusterNode const *from,
		const size_t nSatisfiabilities,
		TaskOffloading::SatisfiabilityInfo *_satisfiabilityInfoList
	) {
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		CPU * const cpu = (currentThread == nullptr) ? nullptr : currentThread->getComputePlace();

		CPUDependencyData localDependencyData;
		CPUDependencyData &hpDependencyData =
			(cpu != nullptr) ? cpu->getDependencyData() : localDependencyData;

		for (size_t i = 0; i < nSatisfiabilities; ++i) {
			SatisfiabilityInfo &satInfo = _satisfiabilityInfoList[i];

			// This is called from the MessageSatisfiability::handleMessage.
			// In Satisfiability messages the satInfo._id contains the remote task identifier (not the
			// predecessor like in tasknew)
			RemoteTaskInfo &taskInfo = RemoteTasksInfoMap::getRemoteTaskInfo(satInfo._id);

			taskInfo._lock.lock();
			if (taskInfo._localTask == nullptr) {
				// The remote task has not been created yet, so we just add the info to the
				// temporary vector.
				satInfo._eagerSendTag = 0;
				taskInfo._satInfo.push_back(satInfo);
				taskInfo._lock.unlock();
			} else {
				// We *HAVE* to leave the lock now, because propagating satisfiability might lead to
				// unregistering the remote task.
				taskInfo._lock.unlock();
				propagateSatisfiability(
					taskInfo._localTask, satInfo, cpu,
					OffloadedTaskIdManager::InvalidOffloadedTaskId, hpDependencyData
				);
			}
		}

		DataAccessRegistration::processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
			hpDependencyData, cpu, true
		);
	}


	void releaseRemoteAccessForHandler(
		Task *task,
		const size_t nRegions,
		MessageReleaseAccess::ReleaseAccessInfo *regionInfoList
	) {
		if (nRegions == 0) {
			return;
		}
		assert(task != nullptr);
		assert(regionInfoList != nullptr);

		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		CPU * const cpu = (currentThread == nullptr) ? nullptr : currentThread->getComputePlace();

		CPUDependencyData localDependencyData;
		CPUDependencyData &hpDependencyData =
			(cpu != nullptr) ? cpu->getDependencyData() : localDependencyData;

		for (size_t i = 0; i < nRegions; ++i) {
			MessageReleaseAccess::ReleaseAccessInfo &accessinfo = regionInfoList[i];
			MemoryPlace const *location
				= ClusterManager::getMemoryNodeOrDirectory(accessinfo._location);

			assert(Directory::isDirectoryMemoryPlace(location)
				|| location->getType() == nanos6_cluster_device);

			DataTransfer *dt = nullptr;
			if (accessinfo._eagerReleaseTag != -1) {
				DataAccessRegion translatedRegion = DataAccessRegistration::getTranslatedRegion(task, accessinfo._region);
				// Program data transfer all in one piece now
				dt = ClusterManager::fetchDataRaw(
					translatedRegion,
					location,
					accessinfo._eagerReleaseTag,
					/* block */ false);
				LiveDataTransfers::add(dt);
			}

			DataAccessRegistration::releaseAccessRegion(
				task, accessinfo._region,
				NO_ACCESS_TYPE,                 // not relevant as specifyingDependency = false
				false,                          // not relevant as specifyingDependency = false
				cpu, hpDependencyData,
				accessinfo._writeID, location,
				false,                          // specifyingDependency
				dt
			);
			if (dt) {
				ClusterPollingServices::PendingQueue<DataTransfer>::addPending(dt);
			}
		}
		DataAccessRegistration::processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
			hpDependencyData, cpu, true
		);
	}

	void sendAccessInfo(ClusterNode *offloader, std::vector<AccessInfo> &regions)
	{
		std::vector<AccessInfo> unfragmented;
		std::sort(
			regions.begin(),
			regions.end(),
			[](const AccessInfo &a, const AccessInfo &b) -> bool {
				return a._region.getStartAddress() < b._region.getStartAddress();
			});
		mergeList(regions, unfragmented);
		MessageAccessInfo *msg = new MessageAccessInfo(unfragmented.size(), unfragmented);
		ClusterManager::sendMessage(msg, offloader);
	}

	void remoteTaskCreateAndSubmit(
		MessageTaskNew *msg,
		Task *parent,
		bool useCallbackInContext
	) {
		assert(msg != nullptr);
		assert(parent != nullptr);

		nanos6_task_info_t * const taskInfo = msg->getTaskInfo();

		size_t numTaskImplementations;
		nanos6_task_implementation_info_t *taskImplementations =
			msg->getImplementations(numTaskImplementations);

		taskInfo->implementations = taskImplementations;
		nanos6_task_invocation_info_t *taskInvocationInfo = msg->getTaskInvocationInfo();

		size_t argsBlockSize;
		void *argsBlock = msg->getArgsBlock(argsBlockSize);

		OffloadedTaskIdManager::OffloadedTaskId remoteTaskIdentifier = msg->getOffloadedTaskId();

		size_t flags = msg->getFlags();
		flags |= (size_t)Task::nanos6_task_runtime_flag_t::nanos6_remote_flag;

		// Create the task with no dependencies. Treat this call
		// as user code since we are inside a spawned task context
		Task *task = AddTask::createTask(
			taskInfo, taskInvocationInfo,
			nullptr, argsBlockSize,
			flags, 0, true
		);
		assert(task != nullptr);

		// If it is a taskfor, then initialize it using the loop bounds in the message
		if (task->isTaskfor()) {
			nanos6_loop_bounds_t bounds = msg->getBounds();
			Taskfor *taskfor = static_cast<Taskfor *>(task);
			taskfor->initialize(bounds.lower_bound, bounds.upper_bound, bounds.chunksize);
		} else if (task->isTaskloop()) {
			nanos6_loop_bounds_t bounds = msg->getBounds();
			Taskloop *taskloop = static_cast<Taskloop *>(task);
			taskloop->initialize(bounds.lower_bound, bounds.upper_bound, bounds.grainsize, bounds.chunksize);
		}

		// Set the task's offloaded task ID to match the original task.
		task->setOffloadedTaskId(remoteTaskIdentifier);

		// Check satisfiability for noRemotePropagation
		// When CreateAndSubmit the satInfo._id member contains the namespace predecessor.
		size_t numSatInfo;
		TaskOffloading::SatisfiabilityInfo *satInfo = msg->getSatisfiabilityInfo(numSatInfo);

		// Offloaded tasks use dependency info sent from the offloading node
        // and register dependencies manually at this point, and so skipping 
        // Mercurium generated code for registering depndencies later inside 
        // registerTaskDataAccesses by checking if it is a remote task.
		for (size_t i = 0; i < numSatInfo; ++i) {
			DataAccessRegistration::registerTaskDataAccess(
				task,
				satInfo[i]._accessType,
				satInfo[i]._weak,
				satInfo[i]._region,
				0, /* TODO: send symbol list, ignored for the moment */
				satInfo[i]._reductionTypeAndOperatorIndex,
				satInfo[i]._reductionIndex,
				satInfo[i]._id);
		}

		void *argsBlockPtr = task->getArgsBlock();
		if (argsBlockSize != 0) {
			memcpy(argsBlockPtr, argsBlock, argsBlockSize);
		}

		ClusterTaskContext *clusterContext = new TaskOffloading::ClusterTaskContext(msg, task);
		assert(clusterContext);
		task->setClusterContext(clusterContext);

		// This is used only in the Namespace. The callback is called during the ClusterTaskContext
		// destructor. And the ClusterTaskContext destructor is called during the ~Task when set.
		if (useCallbackInContext) {
			assert(NodeNamespace::isEnabled());

			clusterContext->setCallback(remoteTaskCleanup, clusterContext);
		}

		// Register remote Task with TaskOffloading mechanism before
		// submitting it to the dependency system
		RemoteTaskInfo &remoteTaskInfo = RemoteTasksInfoMap::getRemoteTaskInfo(remoteTaskIdentifier);

		{
			std::lock_guard<PaddedSpinLock<>> lock(remoteTaskInfo._lock);
			// assert(remoteTaskInfo._localTask == nullptr);
			remoteTaskInfo._localTask = task;

			// Increase the blocking count for the task while it is being constructed.
			// This is for two reasons:
			//
			// (1) If the offloaded task has a weak access, and no strong subtask
			//     accesses the region, then as soon as we submit the task, another task
			//     may execute it to completion. We increase the blocking count to make
			//     sure it doesn't get disposed. Otherwise, when we propagate the
			//     satisfiabilities (below), we would get a use-after-free error.
			// (2) If a read-only task is propagated in our namespace, then it
			//     may pick up the read and write (actually pseudowrite)
			//     satisfiability, as it should, from the namespace. As above, we
			//     do not want it to run to completion and dispose the task. In this
			//     case, read and write satisfiability may be included in the
			//     task new message, which would be a use-after-free error.
			task->increaseRemovalBlockingCount();

			// If the task does not have the wait or nowait flag then, unless
			// cluster.disable_autowait=false, set the "autowait" flag, which will
			// enable early release for accesses ("locally") propagated in the namespace
			// and use delayed release for the ("non-local") accesses that require a
			// message back to the offloader.
			if (!task->mustDelayRelease()
				&& !task->hasNowait()
				&& !ClusterManager::getDisableAutowait()) {

				task->setEarlyRelease(nanos6_autowait);
			}

			// Submit the task
			AddTask::submitTask(task, parent, true);

			// If there are some satisfiabilities already arrived OR the task has some accesses in
			// the satinfo. the process all them.
			if (numSatInfo > 0 || !remoteTaskInfo._satInfo.empty()) {
				WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
				CPU * const cpu =
					(currentThread == nullptr) ? nullptr : currentThread->getComputePlace();

				CPUDependencyData localDependencyData;
				CPUDependencyData &hpDependencyData =
					(cpu != nullptr) ? cpu->getDependencyData() : localDependencyData;

				// Propagate satisfiability embedded in the Message
				for (size_t i = 0; i < numSatInfo; ++i) {
					if (satInfo[i]._readSat || satInfo[i]._writeSat) {
						propagateSatisfiability(task, satInfo[i], cpu, OffloadedTaskIdManager::InvalidOffloadedTaskId, hpDependencyData);
					}
				}

				// Propagate also any satisfiability that has already arrived
				for (SatisfiabilityInfo const &sat : remoteTaskInfo._satInfo) {
					propagateSatisfiability(task, sat, cpu,
						OffloadedTaskIdManager::InvalidOffloadedTaskId, hpDependencyData
					);
				}
				remoteTaskInfo._satInfo.clear();

				DataAccessRegistration::processDelayedOperationsSatisfiedOriginatorsAndRemovableTasks(
					hpDependencyData, cpu, true
				);
			}
		}

		if (task->decreaseRemovalBlockingCount()) {
			// The task must have run to completion so we can dispose it now.
			TaskFinalization::disposeTask(task);
		}
	}

	void remoteTaskCleanup(void *args)
	{
		// The remote task can be discounted from the namespace because it is finishing. This
		// function remoteTaskCleanup is called in the callback from disposeTask when calling
		// runHook
		NodeNamespace::callbackDecrement();

		assert(args != nullptr);
		ClusterTaskContext *clusterContext = static_cast<ClusterTaskContext *>(args);

		OffloadedTaskIdManager::OffloadedTaskId offloadedTaskId
			= clusterContext->getRemoteIdentifier();

		RemoteTasksInfoMap::eraseRemoteTaskInfo(offloadedTaskId);
		assert(clusterContext->getOwnerTask()->hasDataReleaseStep());

		if (ClusterManager::getMergeReleaseAndFinish()) {

			clusterContext->getOwnerTask()->getDataReleaseStep()->releasePendingAccesses(true);
		} else {
			// The notify back sending message
			clusterContext->getOwnerTask()->getDataReleaseStep()->releasePendingAccesses(false);

			clusterContext->getOwnerTask()->getDataReleaseStep()->releasePendingAccesses(true);
		}
		ClusterMetrics::incSentNumTaskFinished();

		// For the moment, we do not delete the Message since it includes the
		// buffers that hold the nanos6_task_info_t and the
		// nanos6_task_implementation_info_t which we might need later on,
		// e.g. Extrae is using these during shutdown. This will change once
		// mercurium gives us access to the respective fields within the
		// binary.
		// delete msg;
	}
}

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CPU_DEPENDENCY_DATA_HPP
#define CPU_DEPENDENCY_DATA_HPP


#include <atomic>
#include <bitset>
#include <boost/dynamic_bitset.hpp>
#include <limits.h>


#include "CommutativeScoreboard.hpp"
#include "DataAccessLink.hpp"
#include "DataAccessRegion.hpp"
#include "dependencies/DataAccessType.hpp"
#include "support/Containers.hpp"

#include <ExecutionStep.hpp>


#ifdef USE_CLUSTER
#include <SatisfiabilityInfo.hpp>  // Already includes WriteID.hpp
#include <AccessInfo.hpp>
#include <DataSendInfo.hpp>
#endif // USE_CLUSTER

struct DataAccess;
class Task;
class ReductionInfo;
class MemoryPlace;
namespace ExecutionWorkflow {
	class Step;
}


struct CPUDependencyData {
	struct UpdateOperation {
		DataAccessLink _target;
		DataAccessRegion _region;

		bool _makeReadSatisfied;
		bool _makeWriteSatisfied;
		bool _makeConcurrentSatisfied;
		bool _makeCommutativeSatisfied;
		bool _propagateSatisfiability;
		bool _setPropagateFromNamespace;
		bool _setReductionInfo; // Note: Both this and _reductionInfo are required, as a null ReductionInfo can be propagated
		bool _previousIsCommutative;
		MemoryPlace const *_location;
		WriteID _writeID;
		ReductionInfo *_reductionInfo;
		OffloadedTaskIdManager::OffloadedTaskId _namespacePredecessor;
		DataAccessType _namespaceAccessType;
		int _validNamespace;

		boost::dynamic_bitset<> _reductionSlotSet;

		UpdateOperation()
			: _target(), _region(),
			_makeReadSatisfied(false), _makeWriteSatisfied(false),
			_makeConcurrentSatisfied(false), _makeCommutativeSatisfied(false),
			 _propagateSatisfiability(false),
			 _setPropagateFromNamespace(false),
			_setReductionInfo(false),
			_previousIsCommutative(false),
			_location(nullptr),
			_writeID(0),
			_reductionInfo(nullptr),
			_namespacePredecessor(OffloadedTaskIdManager::InvalidOffloadedTaskId),
			_namespaceAccessType(NO_ACCESS_TYPE),
			_validNamespace(-1)
		{
		}

		UpdateOperation(DataAccessLink const &target, DataAccessRegion const &region)
			: _target(target), _region(region),
			_makeReadSatisfied(false), _makeWriteSatisfied(false),
			_makeConcurrentSatisfied(false), _makeCommutativeSatisfied(false),
			 _propagateSatisfiability(false),
			 _setPropagateFromNamespace(false),
			_setReductionInfo(false),
			_previousIsCommutative(false),
			_location(nullptr),
			_writeID(0),
			_reductionInfo(nullptr),
			_namespacePredecessor(OffloadedTaskIdManager::InvalidOffloadedTaskId),
			_namespaceAccessType(NO_ACCESS_TYPE),
			_validNamespace(-1)
		{
		}

		bool empty() const
		{
			return !_makeReadSatisfied && !_makeWriteSatisfied
				&& !_makeConcurrentSatisfied && !_makeCommutativeSatisfied
				&& !_setPropagateFromNamespace
				&& !_setReductionInfo
				&& (_reductionSlotSet.size() == 0)
				&& _namespacePredecessor == OffloadedTaskIdManager::InvalidOffloadedTaskId;
		}
	};

	struct TaskAndRegion {
		Task *_task;
		DataAccessRegion _region;
		const MemoryPlace *_location;

		TaskAndRegion(Task *task, DataAccessRegion const &region, const MemoryPlace *location)
			: _task(task), _region(region), _location(location)
		{
		}

		bool operator<(TaskAndRegion const &other) const
		{
			if (_task < other._task) {
				return true;
			} else if (_task > other._task) {
				return false;
			} else {
				return (_region.getStartAddress() < other._region.getStartAddress());
			}
		}
		bool operator>(TaskAndRegion const &other) const
		{
			if (_task > other._task) {
				return true;
			} else if (_task < other._task) {
				return false;
			} else {
				return (_region.getStartAddress() > other._region.getStartAddress());
			}
		}
		bool operator==(TaskAndRegion const &other) const
		{
			return (_task == other._task) && (_region == other._region);
		}
		bool operator!=(TaskAndRegion const &other) const
		{
			return (_task != other._task) || (_region != other._region);
		}
	};

	typedef Container::list<UpdateOperation> delayed_operations_t;
	typedef Container::deque<Task *> satisfied_originator_list_t;
	typedef Container::deque<Task *> removable_task_list_t;
	typedef Container::deque<CommutativeScoreboard::entry_t *> acquired_commutative_scoreboard_entries_t;
	typedef Container::deque<TaskAndRegion> released_commutative_regions_t;
	typedef Container::deque<DataAccess *> satisfied_taskwait_accesses_t;
	typedef std::vector<TaskAndRegion> namespace_regions_to_remove_t;

	//! Tasks whose accesses have been satisfied after ending a task
	satisfied_originator_list_t _satisfiedOriginators;
	satisfied_originator_list_t _satisfiedCommutativeOriginators;
	delayed_operations_t _delayedOperations;
	removable_task_list_t _removableTasks;
	acquired_commutative_scoreboard_entries_t _acquiredCommutativeScoreboardEntries;
	released_commutative_regions_t _releasedCommutativeRegions;
	satisfied_taskwait_accesses_t _completedTaskwaits;
	namespace_regions_to_remove_t _namespaceRegionsToRemove;
#ifndef NDEBUG
	std::atomic<bool> _inUse;
#endif // NDEBUG

#ifdef USE_CLUSTER
	TaskOffloading::SatisfiabilityInfoMap _satisfiabilityMap; // Node's: list of satisfiabilities to send.
	TaskOffloading::DataSendRegionInfoMap _dataSendRegionInfoMap;
	TaskOffloading::AccessInfoMap _accessInfoMap;
	std::vector<ExecutionWorkflow::Step *> _stepsToStart;
#endif // USE_CLUSTER

	CPUDependencyData()
		: _satisfiedOriginators(), _satisfiedCommutativeOriginators(),
		_delayedOperations(), _removableTasks(),
		_acquiredCommutativeScoreboardEntries(), _releasedCommutativeRegions(),
		_completedTaskwaits()
#ifndef NDEBUG
		, _inUse(false)
#endif // NDEBUG
#ifdef USE_CLUSTER
		, _satisfiabilityMap() // Node's: list of satisfiabilities to send.
		, _dataSendRegionInfoMap()
#endif // USE_CLUSTER
	{
	}

	~CPUDependencyData()
	{
		assert(empty());
	}

	inline bool empty() const
	{
		return _satisfiedOriginators.empty() && _satisfiedCommutativeOriginators.empty()
			&& _delayedOperations.empty() && _removableTasks.empty()
			&& _acquiredCommutativeScoreboardEntries.empty()
			&& _completedTaskwaits.empty()
#ifdef USE_CLUSTER
			&& _satisfiabilityMap.empty()
			&& _dataSendRegionInfoMap.empty()
#endif // USE_CLUSTER
			;
	}

	inline void initBytesInNUMA(int)
	{
	}
};


#endif // CPU_DEPENDENCY_DATA_HPP

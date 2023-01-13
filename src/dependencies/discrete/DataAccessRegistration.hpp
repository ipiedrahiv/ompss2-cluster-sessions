/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_ACCESS_REGISTRATION_HPP
#define DATA_ACCESS_REGISTRATION_HPP

#include <stddef.h>

#include "DataAccess.hpp"
#include "CPUDependencyData.hpp"
#include "ReductionSpecific.hpp"
#include "dependencies/DataAccessType.hpp"
#include "cluster/offloading/OffloadedTaskId.hpp"

#include <api/nanos6/task-instantiation.h>

class ComputePlace;
class Task;
struct TaskDataAccesses;

namespace DataAccessRegistration {
	//! \brief creates a task data access taking into account repeated accesses but does not link it to previous accesses nor superaccesses
	//!
	//! \param[in,out] task the task that performs the access
	//! \param[in] accessType the type of access
	//! \param[in] weak whether access is weak or strong
	//! \param[in] address the starting address of the access
	//! \param[in] the length of the access
	//! \param[in] reductionTypeAndOperatorIndex an index that identifies the type and the operation of the reduction
	//! \param[in] reductionIndex an index that identifies the reduction within the task

	void registerTaskDataAccess(
		Task *task, DataAccessType accessType,
		bool weak,
		void *address,
		size_t length,
		reduction_type_and_operator_index_t reductionTypeAndOperatorIndex,
		reduction_index_t reductionIndex,
		int symbolIndex,
		OffloadedTaskIdManager::OffloadedTaskId namespacePredecessor = OffloadedTaskIdManager::InvalidOffloadedTaskId
	);

	//! \brief Performs the task dependency registration procedure
	//!
	//! \param[in] task the Task whose dependencies need to be calculated
	//!
	//! \returns true if the task is already ready
	bool registerTaskDataAccesses(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData
	);

	void unregisterTaskDataAccesses(
		Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData,
		MemoryPlace *location = nullptr,
		bool fromBusyThread = false,
		std::function<void()> callback = nullptr
	);

	void releaseAccessRegion(
		Task *task, void * address,
		DataAccessType accessType,
		bool weak,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData,
		MemoryPlace const *location = nullptr
	);

	void handleEnterTaskwait(Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData,
		bool noflush=false,
		bool nonLocalOnly=false
	);

	void handleExitTaskwait(Task *task,
		ComputePlace *computePlace,
		CPUDependencyData &dependencyData,
		bool nowait
	);

	void combineTaskReductions(Task *task, ComputePlace *computePlace);
	void translateReductionAddresses(Task *task, ComputePlace *computePlace,
		nanos6_address_translation_entry_t * translationTable, int totalSymbols);

	template <typename ProcessorType>
	inline bool processAllDataAccesses(Task *task, ProcessorType processor);

	//! \brief Mark a Taskwait fragment as completed
	//!
	//! \param[in] task is the Task that created the taskwait fragment
	//! \param[in] region is the taskwait region that has been completed
	//! \param[in] computePlace is the current ComputePlace of the caller
	//! \param[in] hpDependencyData is the CPUDependencyData used for delayed operations
	void releaseTaskwaitFragment(
		Task *task,
		DataAccessRegion region,
		ComputePlace *computePlace,
		CPUDependencyData &hpDependencyData,
		bool doDelayedOperations
	);

	bool supportsDataTracking();
} // namespace DataAccessRegistration

#endif // DATA_ACCESS_REGISTRATION_HPP

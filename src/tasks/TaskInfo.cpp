/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include <string>

#include "TaskInfo.hpp"


TaskInfo::task_type_map_t TaskInfo::_taskTypes;
SpinLock TaskInfo::_lock;
std::atomic<size_t> TaskInfo::_numUnlabeledTaskTypes(0);


void TaskInfo::registerTaskInfo(nanos6_task_info_t *taskInfo)
{
	assert(taskInfo != nullptr);
	assert(taskInfo->implementations != nullptr);
	assert(taskInfo->implementations->declaration_source != nullptr);

	std::string label;
	if (taskInfo->implementations->task_label != nullptr) {
		label = std::string(taskInfo->implementations->task_label);
	} else {
		// Avoid comparing empty strings and identify them separately
		size_t unlabeledId = _numUnlabeledTaskTypes++;
		label = "Unlabeled" + std::to_string(unlabeledId);
	}

	std::string declarationSource(taskInfo->implementations->declaration_source);

	// NOTE: We try to emplace the new TaskInfo in the map:
	// 1) If the element is emplaced, it's a new type of task and a new
	// TaskTypeData has been created
	// 2) If the key already existed, it's a duplicated type of task, and the
	// iterator points to the original copy
	std::pair<task_type_map_t::iterator, bool> emplacedElement;

	_lock.lock();

	emplacedElement = _taskTypes.emplace(
		std::piecewise_construct,
		std::forward_as_tuple(label, declarationSource),
		std::forward_as_tuple()
	);

	_lock.unlock();

	// Save a reference of this task type in the task info
	task_type_map_t::iterator it = emplacedElement.first;
	taskInfo->task_type_data = &(it->second);
}
/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef SERIALIZE_HPP
#define SERIALIZE_HPP

#include <vector>
#include <set>
#include <DataAccessRegion.hpp>
#include <nanos6/task-instantiation.h>

class Task;

class Serialize {
	typedef std::set<DataAccessRegion> regionSet;

	struct SerializeArgs
	{
		Task *_task;
		const DataAccessRegion _fullRegion; // to calculate relative offsets
		int _nodeIdx;                       // Target Node
		size_t _process;
		size_t _id;
		bool _isSerialize;
		const size_t _numRegions;
		DataAccessRegion _regionsDeps[];

		SerializeArgs(
			Task *task,
			const DataAccessRegion &fullRegion,
			int nodeIdx,
			size_t process,
			size_t id,
			bool isSerialize,
			const regionSet &inputs
		) : _task(task), _fullRegion(fullRegion), _nodeIdx(nodeIdx),
			_process(process), _id(id), _isSerialize(isSerialize), _numRegions(inputs.size())
		{
			std::copy(inputs.begin(), inputs.end(), _regionsDeps);
		}
	};

	static std::string getFilename(const SerializeArgs * const serializeArgs)
	{
		return std::to_string(serializeArgs->_process) + "/" + std::to_string(serializeArgs->_id);
	}

	// Get a vector with sets for all the regions and their homeNode
	static std::vector<regionSet> getHomeRegions(const DataAccessRegion &region);

public:
	// Static types for task
	static nanos6_task_invocation_info_t invocationInfo;
	static nanos6_task_implementation_info_t implementationsSerialize;
	static nanos6_task_info_t infoVarSerialize;

	// Static functions for task
	static void serialize(void *arg, void *, nanos6_address_translation_entry_t *);
	static void registerDependencies(void *arg, void *, void *);
	static void getConstraints(void* arg, nanos6_task_constraints_t *const constraints);

	static int serializeRegion(
		const DataAccessRegion &region, size_t process, size_t id, bool serialize
	);
};

#endif // SERIALIZE_HPP

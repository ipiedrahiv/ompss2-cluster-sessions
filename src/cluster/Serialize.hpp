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
#include <cstring>

class Task;

class Serialize {
	typedef std::set<DataAccessRegion> regionSet;

	struct SerializeArgs
	{
		Task *_task;
		const DataAccessRegion _fullRegion;       // to calculate relative offsets
		const int _nodeIdx;                       // Target Node
		const size_t _process;
		const size_t _id;
		const bool _isSerialize;
		const bool _isWeak;
		const DataAccessRegion _sentinelRegion;
		const size_t _numRegions;
		DataAccessRegion _regionsDeps[];

		SerializeArgs(
			Task *task,
			const DataAccessRegion &fullRegion,
			int nodeIdx,
			size_t process,
			size_t id,
			bool isSerialize,
			int *sentinel,
			const regionSet &inputs
		) : _task(task), _fullRegion(fullRegion), _nodeIdx(nodeIdx),
			_process(process), _id(id), _isSerialize(isSerialize),
			_isWeak(true), _sentinelRegion(sentinel, sizeof(int)), _numRegions(inputs.size())
		{
			std::copy(inputs.begin(), inputs.end(), _regionsDeps);
		}


		SerializeArgs(Task *task, const SerializeArgs *other, void *sentinel)
			: _task(task), _fullRegion(other->_fullRegion), _nodeIdx(other->_nodeIdx),
			  _process(other->_process), _id(other->_id), _isSerialize(other->_isSerialize),
			  _isWeak(false),
			  _sentinelRegion(sentinel, sizeof(int)),
			  _numRegions(other->_numRegions)
		{
			// This must be called only to create strong accesses from weak
			assert(other->_isWeak);
			// In the target
			std::memcpy(
				_regionsDeps, other->_regionsDeps, other->_numRegions * sizeof(DataAccessRegion)
			);
		}


	};

	static std::string getFilename(const SerializeArgs * const serializeArgs)
	{
		return std::to_string(serializeArgs->_process) + "/" + std::to_string(serializeArgs->_id);
	}

	// Get a vector with sets for all the regions and their homeNode
	static std::vector<regionSet> getHomeRegions(const DataAccessRegion &region);

	const size_t _nSentinels;
	int *_sentinels;

	static Serialize *_singleton;

	Serialize(int clusterMaxSize);

	~Serialize();

	static void createAndSubmitStrong(
		Task *parent,
		nanos6_task_invocation_info_t* invocationInfo,
		const SerializeArgs *const serializeArgs,
		void *sentinel
	);

	static void ioData(const SerializeArgs * const arg);

public:

	// Only finalize if it was already initialized. This is not called at the moment because we
	// don't have a proper place to do it yet.
	static void tryFinalize()
	{
		if (_singleton != nullptr) {
			delete _singleton;
			_singleton = nullptr;
		}
	}

	// Static types for weak task
	static nanos6_task_invocation_info_t weakInvocationInfo;
	static nanos6_task_implementation_info_t weakImplementationsSerialize;
	static nanos6_task_info_t weakInfoVarSerialize;

	// Static functions for task


	// Static types for strong task
	static nanos6_task_invocation_info_t fetchInvocationInfo;
	static nanos6_task_invocation_info_t ioInvocationInfo;
	static nanos6_task_implementation_info_t implementationsSerialize;
	static nanos6_task_info_t infoVarSerialize;

	// Static functions for task
	static void serialize(void *arg, void *, nanos6_address_translation_entry_t *);

	static void registerDependencies(void *arg, void *, void *);
	static void getConstraints(void* arg, nanos6_task_constraints_t *const constraints);

	// Public function
	static int serializeRegion(
		const DataAccessRegion &region, size_t process, size_t id, bool serialize
	);
};

#endif // SERIALIZE_HPP

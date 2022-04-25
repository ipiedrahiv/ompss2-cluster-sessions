/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_INIT_SPAWNED_HPP
#define DATA_INIT_SPAWNED_HPP

#include <nanos6/cluster.h>
#include <VirtualMemoryManagement.hpp>

struct DataInitSpawn {

	DataAccessRegion _virtualAllocation;
	DataAccessRegion _virtualDistributedRegion;
	size_t _localSizePerNode;
	size_t _numMinNodes, _numMaxNodes;

	DataInitSpawn()
		: _virtualAllocation(), _virtualDistributedRegion(), _numMinNodes(0), _numMaxNodes(0)
	{
	}

	inline bool clusterMalleabilityEnabled() const
	{
		assert(_numMinNodes != 0);
		assert(_numMaxNodes != 0);
		return _numMinNodes != _numMaxNodes;
	}

};

#endif /* DATA_INIT_SPAWNED_HPP */

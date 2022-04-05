/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_INIT_SPAWNED_HPP
#define DATA_INIT_SPAWNED_HPP

#include <nanos6/cluster.h>
#include <VirtualMemoryManagement.hpp>

struct DataInitSpawn {

	DataAccessRegion _virtualRegion;

	DataInitSpawn()
	{
		VirtualMemoryManagement::VirtualMemoryAllocation *allocation
			= VirtualMemoryManagement::getAllocations()[0];

		_virtualRegion = DataAccessRegion(allocation->getStartAddress(), allocation->getSize());
	}

};

#endif /* DATA_INIT_SPAWNED_HPP */

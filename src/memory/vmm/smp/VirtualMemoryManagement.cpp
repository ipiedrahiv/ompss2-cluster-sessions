/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <string>

#include "VirtualMemoryManagement.hpp"
#include "hardware/HardwareInfo.hpp"
#include "memory/vmm/VirtualMemoryArea.hpp"
#include "support/config/ConfigVariable.hpp"
#include "system/RuntimeInfo.hpp"


size_t VirtualMemoryManagement::_size;
std::vector<VirtualMemoryAllocation *> VirtualMemoryManagement::_allocations;
std::vector<VirtualMemoryManagement::node_allocations_t> VirtualMemoryManagement::_localNUMAVMA;
VirtualMemoryManagement::vmm_lock_t VirtualMemoryManagement::_lock;

void VirtualMemoryManagement::initialize()
{
	// The cluster.local_memory variable determines the size of the local address
	// space per cluster node. The default value is the minimum between 2GB and
	// the 5% of the total physical memory of the machine
	ConfigVariable<StringifiedMemorySize> _sizeEnv("cluster.local_memory");
	_size = _sizeEnv.getValue();

	// _size == 0 when not set in any toml.
	if (_size == 0) {
		const size_t totalMemory = HardwareInfo::getPhysicalMemorySize();
		_size = std::min(2UL << 30, totalMemory / 20);
	}
	assert(_size > 0);
	_size = ROUND_UP(_size, HardwareInfo::getPageSize());

	_allocations.resize(1);
	_allocations[0] = new VirtualMemoryAllocation(nullptr, _size);

	_localNUMAVMA.resize(HardwareInfo::getMemoryPlaceCount(nanos6_device_t::nanos6_host_device));
	HostInfo *deviceInfo = (HostInfo *)HardwareInfo::getDeviceInfo(nanos6_device_t::nanos6_host_device);
	setupMemoryLayout(_allocations[0], deviceInfo->getMemoryPlaces());

	RuntimeInfo::addEntry("local_memory_size", "Size of local memory per node", _size);
}

void VirtualMemoryManagement::shutdown()
{
	for (auto &map : _localNUMAVMA) {
		for (auto &vma : map) {
			delete vma;
		}
	}

	for (auto &allocation : _allocations) {
		delete allocation;
	}
}

void VirtualMemoryManagement::setupMemoryLayout(
	VirtualMemoryAllocation *allocation,
	const std::vector<MemoryPlace *> &numaNodes
) {
	void *address = allocation->getAddress();
	const size_t size = allocation->getSize();
	const size_t numaNodeCount = numaNodes.size();

	// Divide the address space between the NUMA nodes and the
	// making sure that all areas have a size that is multiple
	// of PAGE_SIZE
	const size_t localPages = size / HardwareInfo::getPageSize();
	const size_t pagesPerNUMA = localPages / numaNodeCount;
	const size_t sizePerNUMA = pagesPerNUMA * HardwareInfo::getPageSize();
	size_t extraPages = localPages % numaNodeCount;
	char *ptr = (char *) address;
	for (size_t i = 0; i < numaNodeCount; ++i) {
		size_t nodeId = numaNodes[i]->getIndex();
		size_t numaSize = sizePerNUMA;
		if (extraPages > 0) {
			numaSize += HardwareInfo::getPageSize();
			extraPages--;
		}
		_singleton->_localNUMAVMA[nodeId].push_back(new VirtualMemoryArea(ptr, numaSize));
		ptr += numaSize;
	}
}

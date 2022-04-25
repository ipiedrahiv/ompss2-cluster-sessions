/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#include <sys/mman.h>
#include <iostream>
#include <string>
#include <regex>

#include "VirtualMemoryManagement.hpp"
#include "cluster/ClusterManager.hpp"
#include "cluster/messages/MessageId.hpp"
#include "hardware/HardwareInfo.hpp"
#include "hardware/cluster/ClusterNode.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "memory/vmm/VirtualMemoryArea.hpp"
#include "support/config/ConfigVariable.hpp"
#include "system/RuntimeInfo.hpp"

#include <DataAccessRegion.hpp>

VirtualMemoryManagement *VirtualMemoryManagement::_singleton = nullptr;

//! \brief Returns a vector with all the mappings of the process
//!
//! It parses /proc/self/maps to find out all the current mappings of the
//! process.
//!
//! \returns a vector of DataAccessRegion objects describing the mappings
std::vector<DataAccessRegion> VirtualMemoryManagement::findMappedRegions(size_t ngaps)
{
	std::vector<DataAccessRegion> maps;

	const std::regex line_regex(
		"([[:xdigit:]]+)-([[:xdigit:]]+) [rwpx-]{4} [[:xdigit:]]+ [[:xdigit:]]{2}:[[:xdigit:]]{2} [0-9]+ +(.*)");

	std::ifstream mapfile("/proc/self/maps");

	std::string line;

	if (mapfile.is_open()) {
		while (getline(mapfile, line)) {

			std::smatch submatch;
			if (std::regex_match(line, submatch, line_regex)) {
				assert(submatch.ready());
				size_t startAddress = std::stoull(submatch[1].str(), NULL, 16);
				size_t endAddress = std::stoull(submatch[2].str(), NULL, 16);

				// The lower-end of the canonical virtual addresses finish
				// at the 2^47 limit. The upper-end of the canonical addresses
				// are normally used by the linux kernel. So we don't want to
				// look there.
				if (endAddress >= (1UL << 47)) {
					break;
				}

				// Add an extra padding to the end of the heap.
				// This avoids the nasty memory error we had long time ago.
				if (submatch[3].str() == "[heap]") {
					endAddress += (ngaps * HardwareInfo::getPhysicalMemorySize());
				}

				maps.emplace_back((void *)startAddress, (void *)endAddress);
			}
		}

		if (!mapfile.eof()) {
			// Check the fail only if not at eof.
			FatalErrorHandler::failIf(mapfile.fail(), "Could not read virtual memory mappings");
		}
		mapfile.close();
	} else {
		FatalErrorHandler::fail("Could not open memory mappings file");
	}

	return maps;
}

//! \brief Finds an memory region to map the Nanos6 region
//!
//! This finds the biggest common gap (region not currently mapped in the
//! virtual address space) of all Nanos6 instances and returns it.
//!
//! \returns an available memory region to map Nanos6 memory, or an empty region
//!          if none available
DataAccessRegion VirtualMemoryManagement::findSuitableMemoryRegion()
{
	std::vector<DataAccessRegion> maps = VirtualMemoryManagement::findMappedRegions(2);
	const size_t length = maps.size();
	DataAccessRegion gap;

	// Find the biggest gap locally
	for (size_t i = 1; i < length; ++i) {
		void *previousEnd = maps[i - 1].getEndAddress();
		void *nextStart = maps[i].getStartAddress();

		if (previousEnd >= nextStart) {
			continue;
		}

		DataAccessRegion region(previousEnd, nextStart);
		if (region.getSize() > gap.getSize()) {
			gap = region;
		}
	}

	// If not in cluster mode and malleability is disabled, we are done here
	if (!ClusterManager::inClusterMode()
		&& ClusterManager::getInitData().clusterMalleabilityEnabled() == false) {
		return gap;
	}

	if (ClusterManager::isMasterNode()) {
		// Master node gathers all the gaps from all other nodes and
		// calculates the intersection of all those.
		DataAccessRegion remoteGap;
		DataAccessRegion buffer(&remoteGap, sizeof(remoteGap));

		std::vector<ClusterNode *> const &nodes = ClusterManager::getClusterNodes();

		for (ClusterNode *remote : nodes) {
			if (remote == ClusterManager::getCurrentClusterNode()) {
				continue;
			}

			MemoryPlace *memoryNode = remote->getMemoryNode();
			// do not instrument as instrumentation subsystem not initialized yet
			ClusterManager::fetchDataRaw(buffer, memoryNode, 0, true, false);

			gap = gap.intersect(remoteGap);
		}

		// Finally, it send the common gap to all other nodes.
		remoteGap = gap;
		for (ClusterNode *remote : nodes) {
			if (remote == ClusterManager::getCurrentClusterNode()) {
				continue;
			}

			MemoryPlace *memoryNode = remote->getMemoryNode();
			// do not instrument as instrumentation subsystem not initialized yet
			ClusterManager::sendDataRaw(buffer, memoryNode, 0, /* block */ true, /* instrument */ false);
		}
	} else {
		DataAccessRegion buffer(&gap, sizeof(gap));
		MemoryPlace *masterMemory = ClusterManager::getMasterNode()->getMemoryNode();

		// First send my local gap to master node
		ClusterManager::sendDataRaw(buffer, masterMemory, 0, true, false);

		// Then receive the intersection of all gaps
		ClusterManager::fetchDataRaw(buffer, masterMemory, 0, true, false);
	}

	return gap;
}


const DataInitSpawn &VirtualMemoryManagement::setupInitData()
{
	// This function is called BEFORE the constructor. It actually may be in the ClusterManager, But
	// I implemented it here because it handles memory stuff.
	assert(_singleton == nullptr);

	// Get the maximum size, when zero malleability is disabled, when numeric limit something is
	// wrong. If malleability is disabled then use the current cluster size.
	DataInitSpawn &initData = ClusterManager::getInitData();

	if (initData._virtualAllocation.empty()) {
		assert(initData._virtualDistributedRegion.empty());

		// The cluster.local_memory variable determines the size of the local address space
		// per cluster node. The default value is the minimum between 2GB and the 5% of the
		// total physical memory of the machine
		ConfigVariable<StringifiedMemorySize> localSizeEnv("cluster.local_memory");
		size_t localSizePerNode = localSizeEnv.getValue();
		if (localSizePerNode == 0) {
			FatalErrorHandler::warn("cluster.local_memory not from toml.");
			localSizePerNode = std::min(2UL << 30, HardwareInfo::getPhysicalMemorySize() / 20);
		}
		FatalErrorHandler::failIf(localSizePerNode <= 0, "cluster.local_memory is zero.");
		localSizePerNode = ROUND_UP(localSizePerNode, HardwareInfo::getPageSize());

		// The cluster.distributed_memory variable determines the total address space to be
		// used for distributed allocations across the cluster The efault value is 2GB
		ConfigVariable<StringifiedMemorySize> distribSizeEnv("cluster.distributed_memory");
		FatalErrorHandler::failIf(distribSizeEnv.getValue() <= 0, "cluster.distributed_memory is zero.");
		const size_t distribSize = ROUND_UP(distribSizeEnv.getValue(), HardwareInfo::getPageSize());

		const size_t totalVirtualMemory = distribSize + localSizePerNode * initData._numMaxNodes;

		assert(!ClusterManager::isSpawned());
		ConfigVariable<uint64_t> confStartAddress("cluster.va_start");
		void *startAddress = (void *) confStartAddress.getValue();

		// If the start address was not specified then coordinate with other processes.
		if (startAddress == nullptr) {
			DataAccessRegion totalGap = VirtualMemoryManagement::findSuitableMemoryRegion();

			FatalErrorHandler::failIf(totalGap.getSize() < totalVirtualMemory,
				"Cannot allocate virtual memory region with size: ", totalVirtualMemory);

			startAddress = totalGap.getStartAddress();
		}

		initData._virtualAllocation = DataAccessRegion(startAddress, totalVirtualMemory);

		void* distribStart
			= (char *)initData._virtualAllocation.getStartAddress()
			+ localSizePerNode * initData._numMaxNodes;

		initData._virtualDistributedRegion = DataAccessRegion(distribStart, distribSize);

		initData._localSizePerNode = localSizePerNode;
	}

	return initData;
}

VirtualMemoryManagement::VirtualMemoryManagement(const DataInitSpawn &initData)
	: _allocation(new VirtualMemoryAllocation(initData._virtualAllocation)),
	  _distributedVirtualRegion(new VirtualMemoryArea(initData._virtualDistributedRegion)),
	  _localSizePerNode(initData._localSizePerNode)
{
	assert(!initData._virtualAllocation.empty());
	assert(!initData._virtualDistributedRegion.empty());
	assert(initData._localSizePerNode > 0);
	assert(_allocation != nullptr);
	assert(_distributedVirtualRegion != nullptr);

	FatalErrorHandler::failIf(_allocation->fullyContainsRegion(*_distributedVirtualRegion) == false,
		"Distributed virtual region: ", _distributedVirtualRegion,
		" not fully contained in allocated virtual region: ", std::ref(*_allocation));

	RuntimeInfo::addEntry(
		"va_start", "Virtual address space start",
		(unsigned long)_allocation->getStartAddress()
	);
	RuntimeInfo::addEntry("distributed_memory_size", "Size of distributed memory",
		_allocation->getSize());
	RuntimeInfo::addEntry("local_memorysize", "Size of local memory per node", _localSizePerNode);
}


VirtualMemoryManagement::~VirtualMemoryManagement()
{
	for (auto &vma : _localNUMAVMA) {
		delete vma;
	}
	delete _distributedVirtualRegion;
	delete _allocation;

	_localNUMAVMA.clear();
}


void VirtualMemoryManagement::setupMemoryLayout()
{
	assert(!_allocation->empty());
	assert(!_distributedVirtualRegion->empty());
	assert(_allocation->getSize() > _distributedVirtualRegion->getSize());

	// Register local addresses with the Directory
	for (const ClusterNode *node : ClusterManager::getClusterNodes()) {
		if (node != ClusterManager::getCurrentClusterNode()) {
			registerNodeLocalRegion(node);
			continue;
		}

		// We have one VMA per NUMA node. At the moment we divide the local
		// address space equally among these areas.
		assert(_localNUMAVMA.empty());
		const size_t numaNodeCount =
			HardwareInfo::getMemoryPlaceCount(nanos6_device_t::nanos6_host_device);
		assert(numaNodeCount > 0);
		_localNUMAVMA.reserve(numaNodeCount);

		// Divide the address space between the NUMA nodes making sure that all areas have a size that
		// is multiple of PAGE_SIZE
		const size_t pageSize = HardwareInfo::getPageSize();
		assert(pageSize > 0);
		const size_t localPages = _localSizePerNode / pageSize;
		assert(localPages > 0);
		const size_t pagesPerNUMA = localPages / numaNodeCount;
		assert(pagesPerNUMA > 0);
		const size_t sizePerNUMA = pagesPerNUMA * pageSize;
		assert(sizePerNUMA > 0);

		size_t extraPages = localPages % numaNodeCount;
		char *ptr = (char *)_allocation->getStartAddress() + _localSizePerNode * node->getIndex();

		assert(ptr <= _distributedVirtualRegion->getStartAddress());
		assert(ptr + _localSizePerNode <= _distributedVirtualRegion->getStartAddress());

		for (size_t i = 0; i < numaNodeCount; ++i) {
			size_t numaSize = sizePerNUMA;
			if (extraPages > 0) {
				numaSize += pageSize;
				extraPages--;
			}
			_localNUMAVMA.push_back(new VirtualMemoryArea(ptr, numaSize));

			// Register the region with the Directory
			const DataAccessRegion numaRegion(ptr, numaSize);
			Directory::insert(numaRegion, HardwareInfo::getMemoryPlace(nanos6_host_device, i));

			ptr += numaSize;
		}
	}
}

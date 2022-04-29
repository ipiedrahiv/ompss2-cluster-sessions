/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef __VIRTUAL_MEMORY_MANAGEMENT_HPP__
#define __VIRTUAL_MEMORY_MANAGEMENT_HPP__

#include "memory/vmm/VirtualMemoryArea.hpp"
#include <ClusterNode.hpp>
#include <Directory.hpp>

#include <vector>

class DataInitSpawn;

class VirtualMemoryManagement {
public:
	static std::vector<DataAccessRegion> findMappedRegions(size_t ngaps);
	static DataAccessRegion findSuitableMemoryRegion();

	// Subclass allocation; only needed and used here
	class VirtualMemoryAllocation : public DataAccessRegion
	{
	private:
		static bool IsUsableMemoryRegion(const DataAccessRegion &region)
		{
			std::vector<DataAccessRegion> maps = VirtualMemoryManagement::findMappedRegions(1);
			const size_t length = maps.size();

			// Find the biggest gap locally
			for (size_t i = 1; i < length; ++i) {
				void *previousEnd = maps[i - 1].getEndAddress();
				void *nextStart = maps[i].getStartAddress();

				if (previousEnd >= nextStart                    // This should never happen
					|| previousEnd > region.getEndAddress()) {  // Too early range
					continue;
				}

				DataAccessRegion gapRegion(previousEnd, nextStart);

				if (region.fullyContainedIn(gapRegion)) {
					return true;
				}
			}

			return false;
		}

	public:
		VirtualMemoryAllocation(DataAccessRegion const &totalRegion) : DataAccessRegion(totalRegion)
		{
			FatalErrorHandler::failIf(totalRegion.getStartAddress() == nullptr,
				"Virtual memory constructor received a null start address");
			FatalErrorHandler::failIf(totalRegion.getSize() == 0,
				"Virtual memory constructor received a zero size");
			FatalErrorHandler::failIf(IsUsableMemoryRegion(totalRegion) ==  false,
				"The region: ", totalRegion, " is already mapped, so not usable.");

			// For the moment we are using fixed memory protection and allocation flags, but in the
			// future we could make those arguments fields of the class
			constexpr int prot = PROT_READ|PROT_WRITE;
			constexpr int flags = MAP_ANONYMOUS|MAP_PRIVATE|MAP_NORESERVE|MAP_FIXED;

			const void *ret = mmap(this->getStartAddress(), this->getSize(), prot, flags, -1, 0);

			FatalErrorHandler::failIf(ret == MAP_FAILED,
				"mapping virtual address space failed. errno: ", errno);
			FatalErrorHandler::failIf(ret != this->getStartAddress(),
				"mapping virtual address space couldn't use address hint");
		}

		//! Virtual allocations should be unique, so can't copy
		VirtualMemoryAllocation(VirtualMemoryAllocation const &) = delete;
		VirtualMemoryAllocation operator=(VirtualMemoryAllocation const &) = delete;

		~VirtualMemoryAllocation()
		{
			const int ret = munmap(this->getStartAddress(), this->getSize());
			FatalErrorHandler::failIf(ret != 0, "Could not unmap memory allocation");
		}

	}; // class VirtualMemoryAllocation

private:
	//! memory allocations from OS
	VirtualMemoryManagement::VirtualMemoryAllocation const * const _allocation = nullptr;

	VirtualMemoryArea * _distributedVirtualRegion;
	const size_t _localSizePerNode;

	//! addresses for local NUMA allocations
	std::vector<VirtualMemoryArea *> _localNUMAVMA;

	static DataInitSpawn const &setupInitData();

	//! Setting up the memory layout
	void setupMemoryLayout();

	//! private constructor, this is a singleton.
	VirtualMemoryManagement(const DataInitSpawn &initData);

	~VirtualMemoryManagement();

	static VirtualMemoryManagement *_singleton;

public:

	static inline void initialize()
	{
		const DataInitSpawn &initData = VirtualMemoryManagement::setupInitData();

		assert(_singleton == nullptr);
		_singleton = new VirtualMemoryManagement(initData);
		assert(_singleton != nullptr);

		_singleton->setupMemoryLayout();
	}

	static inline void shutdown()
	{
		assert(_singleton != nullptr);
		delete _singleton;
		_singleton = nullptr;
	}

	static void registerNodeLocalRegion(const ClusterNode *node)
	{
		assert(_singleton != nullptr);
		// TODO: add some assertion here to check the region is nor already registered.
		char *ptr = ((char *)_singleton->_allocation->getStartAddress()
			+ _singleton->_localSizePerNode * node->getIndex());

		DataAccessRegion tmpRegion((void *)ptr, _singleton->_localSizePerNode);
		assert(tmpRegion.getEndAddress() <= _singleton->_distributedVirtualRegion->getStartAddress());

		Directory::insert(tmpRegion, node->getMemoryNode());
	}

	static void unregisterNodeLocalRegion(const ClusterNode *node)
	{
		assert(_singleton != nullptr);
		// TODO: add some assertion here to check the region is nor already registered.
		char *ptr = ((char *)_singleton->_allocation->getStartAddress()
			+ _singleton->_localSizePerNode * node->getIndex());

		DataAccessRegion tmpRegion((void *)ptr, _singleton->_localSizePerNode);
		assert(tmpRegion.getEndAddress() <= _singleton->_distributedVirtualRegion->getStartAddress());

		Directory::erase(tmpRegion);
	}

	/** allocate a block of generic addresses.
	 *
	 * This region is meant to be used for allocations that can be mapped
	 * to various memory nodes (cluster or NUMA) based on a policy. So this
	 * is the pool for distributed allocations or other generic allocations.
	 */
	static inline void *allocDistrib(size_t size)
	{
		assert(_singleton != nullptr);
		return _singleton->_distributedVirtualRegion->allocBlock(size);
	}

	/** allocate a block of local addresses on a NUMA node.
	 *
	 * \param size the size to allocate
	 * \param NUMAId is the the id of the NUMA node to allocate
	 */
	static inline void *allocLocalNUMA(size_t size, size_t NUMAId)
	{
		assert(_singleton != nullptr);
		VirtualMemoryArea *vma = _singleton->_localNUMAVMA.at(NUMAId);
		assert(vma != nullptr);

		return vma->allocBlock(size);
	}

	//! return the NUMA node id of the node containing 'ptr' or
	//! the NUMA node count if not found
	static inline size_t findNUMA(void *ptr)
	{
		assert(_singleton != nullptr);
		for (size_t i = 0; i < _singleton->_localNUMAVMA.size(); ++i) {
			if (_singleton->_localNUMAVMA[i]->containsAddress(ptr)) {
				return i;
			}
		}
		//! Non-NUMA allocation
		return _singleton->_localNUMAVMA.size();
	}

	//! \brief Check if a region is within the distributed memory region
	//!
	//! \param[in] region the DataAccessRegion to check
	//!
	//! \return true if the region is within distributed memory
	static inline bool isDistributedRegion(DataAccessRegion const &region)
	{
		assert(_singleton != nullptr);
		return _singleton->_distributedVirtualRegion->fullyContainsRegion(region);
	}

	//! \brief Check if a memory region is (cluster) local memory
	//!
	//! \param[in] region the DataAccessRegion to check
	//!
	//! \returns true if the region is within local memory
	static inline bool isLocalRegion(DataAccessRegion const &region)
	{
		assert(_singleton != nullptr);
		for (const auto &it : _singleton->_localNUMAVMA) {
			// TODO: I think there is a bug here. a region could be crossing boundaries between two
			// contiguous numa nodes; so fullyContainsRegion will return false in spite of it is
			// local.
			if (it->fullyContainsRegion(region)) {
				return true;
			}
		}

		return false;
	}

	//! \brief Check if a memory region can handled correctly by Cluster
	//!
	//! \param[in] region the DataAccessRegion to check
	//!
	//! \return true if the region is in cluster-capable memory
	static inline bool isClusterMemory(DataAccessRegion const &region)
	{
		return isDistributedRegion(region) || isLocalRegion(region);
	}

};


#endif /* __VIRTUAL_MEMORY_MANAGEMENT_HPP__ */

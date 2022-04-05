/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef __VIRTUAL_MEMORY_AREA_HPP__
#define __VIRTUAL_MEMORY_AREA_HPP__

#ifndef ROUND_UP
#define ROUND_UP(_s, _r) ((((_s) + (_r) - 1) / (_r)) * (_r))
#endif /* ROUND_UP */

#include "hardware/HardwareInfo.hpp"

#include "lowlevel/FatalErrorHandler.hpp"

#include <sys/mman.h>

class VirtualMemoryArea  : public DataAccessRegion {
	char *_nextFree;       // next free pointer in the area

	size_t _available;     // amount of available memory

	size_t _count_allocs;  // counter of allocations.

public:
	VirtualMemoryArea(void *address, size_t size)
		: DataAccessRegion(address, size),
		_nextFree((char *)address), _available(size), _count_allocs(0)
	{
		assert(_available > 0);
		assert(size > 0);
	}

	//! Virtual addresses should be unique.
	VirtualMemoryArea(VirtualMemoryArea const &) = delete;
	VirtualMemoryArea operator=(VirtualMemoryArea const &) = delete;

	~VirtualMemoryArea()
	{
	}

	/** Returns a block of virtual address from the virtual memory area.
	 *
	 * This method is not thread-safe. Synchronization needs to be handled externally.
	 */
	inline void *allocBlock(size_t size)
	{
		/** Rounding up the size allocations to PAGE_SIZE is the easy
		 * way to ensure all allocations are aligned to PAGE_SIZE */
		size = ROUND_UP(size, HardwareInfo::getPageSize());
		if (size > _available) {
			FatalErrorHandler::warn(
				"VirtualMemoryArea wanted: ", size,
				" bytes but only: ", _available, " are available.",
				" vm_start: ", getStartAddress(), " size: ", getSize(),
				" allocations: ", _count_allocs
			);
			return nullptr;
		}

		void *ret = (void *)_nextFree;

		_available -= size;
		_nextFree += size;
		++_count_allocs;

		// This assertion also prevents overflows
		assert(_available <= getSize());

		return ret;
	}
};

#endif /* __VIRTUAL_MEMORY_AREA_HPP__ */

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef WRITEID_HPP
#define WRITEID_HPP

#include <cstddef>
#include <cstdint>
#include <utility>
#include <atomic>
#include <vector>
#include <iostream>

#include "lowlevel/RWSpinLock.hpp"
#include "DataAccessRegion.hpp"
#include "LinearRegionMap.hpp"
#include "LinearRegionMapImplementation.hpp"

#include "support/config/ConfigVariable.hpp"

// Identify redundant data fetches using a globally unique write ID.  Each new
// data output or inout is allocated a new write ID.  The write ID remains the
// same for all dependent read accesses.
//
// We hold the tree of locally present regions and WriteIDs in a
// LinearRegionMap, which means that subaccesses of the original access are
// also identified as locally present.  Larger accesses, all of whose parts
// have been registered as present, are also identified as such. The API does
// not however, let us identify which parts (if not all) of a larger access are
// present.
//
// Accessing this tree requires taking a (reader/writer) lock, so we hash the
// WriteID and use one of a small number of trees. The number of trees is given
// by numMaps. This means that read and updates for the trees corresponding to
// different WriteIDs can be done concurrently. Note: Different values of
// numMaps does not change the behaviour of WriteID, but it may affect the
// locking overhead.
//
// Note: the old WriteID is not removed when the same region with a later
// WriteID is registered as present. But the dependency system already ensures
// that we only check the WriteID for the currently-valid WriteID.

// 64-bit Write ID.
typedef size_t HashID;
typedef size_t WriteID;

// Represent a region that is present locally with a given WriteID
struct WriteIDEntryRegion {
	DataAccessRegion _region;
	WriteID _writeID;

	DataAccessRegion const &getAccessRegion() const
	{
		return _region;
	}

	DataAccessRegion &getAccessRegion()
	{
		return _region;
	}

	WriteIDEntryRegion(DataAccessRegion region) : _region(region)
	{
	}
};

typedef LinearRegionMap<WriteIDEntryRegion> WriteIDLinearRegionMap;

struct WriteIDEntry {
	RWSpinLock _lock;
	WriteIDLinearRegionMap _regions;

	// Register a write ID as being present locally
	void registerWriteIDInEntry(WriteID id, const DataAccessRegion &region, bool insertMissing)
	{
		assert(id != 0);
		// Update the tree
		_regions.processIntersectingAndMissing(
			region,
			[&](WriteIDLinearRegionMap::iterator position) -> bool {
				// Region already in the map: update the writeID if it has changed
				if (position->_writeID != id) {
					if (!position->_region.fullyContainedIn(region)) {
						position = _regions.fragmentByIntersection(
							position, region, /* removeIntersection */ false
						);
					}
					position->_writeID = id;
				}
				return true;
			},
			[&](DataAccessRegion const &missingRegion) -> bool {
				if (insertMissing) {
					// Region not yet in the map: insert it with the given WriteID
					WriteIDLinearRegionMap::iterator position = _regions.emplace(missingRegion);
					position->_writeID = id;
				}
				return true;
			}
		);
	}

	// Apply processor to all the subregions in the entry intersecting with Region and with WriteID
	// id. This does not modify the entry.
	void processIntersectionsWithWriteIDInEntry(
		WriteID id,
		const DataAccessRegion &region,
		std::function<void(const DataAccessRegion &)> processor
	) {
		assert(id != 0);
		// Add up all the bytes in this region that correspond to the correct WriteID
		_regions.processIntersecting(
			region,
			[&](WriteIDLinearRegionMap::iterator position) -> bool {
				if (position->_writeID == id) {
					const DataAccessRegion foundRegion = position->_region;
					DataAccessRegion subregion = foundRegion.intersect(region);
					processor(subregion);
				}
				return true;
			}
		);
	}
};

class WriteIDManager
{
private:
	static constexpr int logMaxNodes = 8;
	static WriteIDManager *_singleton;

	/* Counter */
	std::atomic<WriteID> _counter;

	static constexpr int numMaps = 512;
	std::vector<WriteIDEntry> _localWriteIDs;

	static HashID hash(WriteID id)
	{
		// Based on https://xorshift.di.unimi.it/splitmix64.c
		uint64_t z =  id;
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
		return z ^ (z >> 31);
	}

	WriteIDManager(WriteID &counter) : _counter(counter), _localWriteIDs(numMaps)
	{
	}

public:
	static void initialize(int nodeIndex, __attribute__((unused)) int clusterSize)
	{
		assert(_singleton == nullptr);

		ConfigVariable<bool> enableWriteID("cluster.enable_write_id");

		if (enableWriteID.getValue()) {
			WriteID initCounter = 1 + (((size_t)nodeIndex) << (64 - logMaxNodes));
			_singleton = new WriteIDManager(initCounter);
			assert(_singleton != nullptr);
		}
	}

	static void finalize()
	{
		if (_singleton != nullptr) {
			delete _singleton;
			_singleton = nullptr;
		}
	}

	static bool isEnabled()
	{
		return _singleton != nullptr;
	}

	// Register a write ID as being present locally
	static void registerWriteIDasLocal(WriteID id, const DataAccessRegion &region)
	{
		if (id) {
			assert(_singleton != nullptr);

			// Find the right tree and take a writer lock
			int idx = hash(id) % numMaps;
			WriteIDEntry &entry = _singleton->_localWriteIDs.at(idx);
			entry._lock.writeLock();
			entry.registerWriteIDInEntry(id, region, true);
			entry._lock.writeUnlock();
		}
	}

	// Check whether the whole region for a write ID is definitely present locally.
	static bool checkWriteIDLocal(WriteID id, const DataAccessRegion &region)
	{
		if (id) {
			assert(_singleton != nullptr);
			// Find the right tree and take a reader lock
			size_t bytesFound = 0;
			int idx = hash(id) % numMaps;
			WriteIDEntry &entry = _singleton->_localWriteIDs.at(idx);
			entry._lock.readLock();
			entry.processIntersectionsWithWriteIDInEntry(
				id, region,
				[&](const DataAccessRegion &subregion) {
					bytesFound += subregion.getSize();
				});
			entry._lock.readUnlock();

			// Return true if all of the bytes of the region have been found
			assert(bytesFound <= region.getSize());
			return bytesFound == region.getSize();
		}
		return false;
	}

	// Check whether the whole region for a write ID is definitely present locally.
	static void updateExistingWriteID(WriteID oldId, WriteID newId, const DataAccessRegion &region)
	{
		assert(oldId != 0);
		assert(newId != 0);
		assert(_singleton != nullptr);

		// Find the right tree and take a reader lock
		const int oldIdx = hash(oldId) % numMaps;
		const int newIdx = hash(newId) % numMaps;

		WriteIDEntry &oldEntry = _singleton->_localWriteIDs.at(oldIdx);

		if (oldIdx == newIdx) {
			// The new and old writeID go in the same entry. So we use the special function.
			oldEntry._lock.writeLock();
			oldEntry._regions.processIntersectingAndMissing(
				region,
				[&](WriteIDLinearRegionMap::iterator position) -> bool {
					// Region already in the map: update the writeID if it has changed
					if (position->_writeID == oldId) {
						if (!position->_region.fullyContainedIn(region)) {
							position = oldEntry._regions.fragmentByIntersection(
								position, region, /* removeIntersection */ false
							);
						}
						position->_writeID = newId;
					}
					return true;
				},
				[](DataAccessRegion const &) -> bool { return true; }
			);
			oldEntry._lock.writeUnlock();
		} else {
			std::vector<DataAccessRegion> subregions;
			WriteIDEntry &newEntry = _singleton->_localWriteIDs.at(newIdx);

			oldEntry._lock.readLock();
			oldEntry.processIntersectionsWithWriteIDInEntry(
				oldId, region,
				[&subregions](const DataAccessRegion &subregion) {
					subregions.push_back(subregion);
				}
			);
			oldEntry._lock.readUnlock();

			// Register the found regions.
			newEntry._lock.writeLock();
			for (const DataAccessRegion &subregion : subregions) {
				newEntry.registerWriteIDInEntry(newId, subregion, false);
			}
			newEntry._lock.writeUnlock();
		}
	}

	static void limitWriteIDToMaxNodes(int maxNodes)
	{
		for (WriteIDEntry &entry: _singleton->_localWriteIDs) {
			entry._lock.writeLock();
			entry._regions.processAll(
				[&](WriteIDLinearRegionMap::iterator position) -> bool {
					if (WriteIDManager::getWriteIDNode(position->_writeID) >= maxNodes) {
						entry._regions.erase(position);
					}
					return true;
				}
			);
			entry._lock.writeUnlock();
		}
	}

	static inline WriteID createWriteID()
	{
		/* This happens for every access, so it should be fast */
		if (_singleton != nullptr) {
			return _singleton->_counter.fetch_add(1);
		}
		return 0;
	}

	static inline int getWriteIDNode(WriteID ID)
	{
		/* This happens for every access, so it should be fast */
		return ID >> (64 - logMaxNodes);
	}
};

#endif // WRITEID_HPP

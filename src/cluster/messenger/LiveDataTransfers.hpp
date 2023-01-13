/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#ifndef LIVE_DATA_TRANSFERS_HPP
#define LIVE_DATA_TRANSFERS_HPP

#include <functional>
#include <mutex>
#include <vector>
#include "lowlevel/PaddedSpinLock.hpp"
#include <DataAccessRegion.hpp>
#include <DataTransfer.hpp>

class DataTransfer;

class LiveDataTransfers {

	static PaddedSpinLock<> _lock;
	static std::vector<DataTransfer *> _liveDataTransfers;

	static void addUnlocked(DataTransfer *dataTransfer)
	{
		_liveDataTransfers.push_back(dataTransfer);

		// Important: remove from live data transfers before calling the no priority callbacks
		// (priority > 0) (otherwise callbacks could potentially be lost)
		dataTransfer->addCompletionCallback(
			[=]() -> void { LiveDataTransfers::remove(dataTransfer); }, 1
		);
	}


public:

	static void add(DataTransfer *dataTransfer)
	{
		std::lock_guard<PaddedSpinLock<>> guard(_lock);
		LiveDataTransfers::addUnlocked(dataTransfer);
	}

	static void remove(DataTransfer *dataTransfer)
	{
		std::lock_guard<PaddedSpinLock<>> guard(_lock);
		auto it = std::find(_liveDataTransfers.begin(), _liveDataTransfers.end(), dataTransfer);
		assert(it != _liveDataTransfers.end());
		_liveDataTransfers.erase(it);
	}

	static bool check(
		std::function<bool(DataTransfer *)> checkPending,
		std::function<DataTransfer *()> createNew
	);
};

#endif /* LIVE_DATA_TRANSFERS_HPP */

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2021 Barcelona Supercomputing Center (BSC)
*/

#include "LiveDataTransfers.hpp"

#include "cluster/WriteID.hpp"

PaddedSpinLock<> LiveDataTransfers::_lock;
std::vector<DataTransfer *> LiveDataTransfers::_liveDataTransfers;

bool LiveDataTransfers::check(
	std::function<bool(DataTransfer *)> checkPending,
	std::function<DataTransfer *()> createNew
) {
	std::lock_guard<PaddedSpinLock<>> guard(_lock);
	if (WriteIDManager::isEnabled()) {
		for(DataTransfer *dataTransfer : _liveDataTransfers) {
			bool done = checkPending(dataTransfer);
			if (done) {
				/* Return done flag */
				return true;
			}
		}
	}
	/* Return not done */
	DataTransfer *dataTransferNew = createNew();
	LiveDataTransfers::addUnlocked(dataTransferNew);
	return false;
}

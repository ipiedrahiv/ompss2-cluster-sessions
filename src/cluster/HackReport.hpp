/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef HACK_REPORT_HPP
#define HACK_REPORT_HPP

#include <unistd.h>
#include <time.h>
#include <cassert>

struct HackReport {
	int oldSize, newSize, delta;
	size_t bytesTransfers;
	size_t nTransfers;

	timespec startResize, endResize, startTransfer, endTransfer;
	double MPITime;

	static timespec getDiffTime(const timespec &t1, const timespec &t2)
	{
		assert(t2.tv_sec >= t1.tv_sec);
		timespec ret;

		if (t2.tv_nsec < t1.tv_nsec) {
			ret.tv_nsec = 1000000000L + t2.tv_nsec - t1.tv_nsec;
			ret.tv_sec = (t2.tv_sec - 1 - t1.tv_sec);
		} else {
			ret.tv_nsec = (t2.tv_nsec - t1.tv_nsec);
			ret.tv_sec = (t2.tv_sec - t1.tv_sec);
		}
		return ret;
	}

	static double timeToDouble(const timespec &deltaRes)
	{
		return deltaRes.tv_sec * 1.0E9 + deltaRes.tv_nsec;
	}

	static double diffToDouble(const timespec &t1, const timespec &t2)
	{
		return timeToDouble(getDiffTime(t1, t2));
	}

	static timespec getTime()
	{
		timespec tmp;
		clock_gettime(CLOCK_MONOTONIC, &tmp);
		return tmp;
	}

	void addTransfer(const DataAccessRegion &region)
	{
		++nTransfers;
		bytesTransfers += region.getSize();
	}

	void init(int _oldSize, int _newSize, int _delta)
	{
		assert(_newSize = _oldSize + _delta);

		clock_gettime(CLOCK_MONOTONIC, &startResize);

		oldSize = _oldSize;
		newSize = _newSize;
		delta = _delta;

		bytesTransfers = 0;
		nTransfers = 0;

		// Reset all timers to have delta zero.
		endResize = startResize;
		startTransfer = startResize;
		endTransfer = startResize;

		MPITime = 0.0;
	}

	void fini()
	{
		assert(delta != 0);
		endResize = getTime();

		const double transTime = diffToDouble(startTransfer, endTransfer);
		const double totalTime = diffToDouble(startResize, endResize);

		std::cout << (delta > 0 ? "## Spawn:" : "## Shrink:")
			<< oldSize << "->" << newSize <<":" <<delta
			<< " trans:" << nTransfers << ":" << bytesTransfers << ":" << transTime
			<< " mpi:" << MPITime << " total:" << totalTime
			<< std::endl;
	}
};

#endif // HACK_REPORT_HPP

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef DATA_ACCESS_REGION_HPP
#define DATA_ACCESS_REGION_HPP


#include <algorithm>
#include <cassert>
#include <cstddef>
#include <ostream>
#include <utility>
#include <functional>

class DataAccessRegion {
private:
	//! The starting address of the data access
	void *_startAddress;

	//! The length of the accesses
	size_t _length;

	struct FragmentBoundaries {
		char * const _firstStart, *_firstEnd;
		char * const _secondStart, *_secondEnd;

		FragmentBoundaries(DataAccessRegion const &first, DataAccessRegion const &second)
			: _firstStart((char *) first._startAddress),
			_firstEnd(_firstStart + first._length),
			_secondStart((char *) second._startAddress),
			_secondEnd(_secondStart + second._length)
		{
		}
	};


public:
	DataAccessRegion(const void *startAddress, const size_t length)
		: _startAddress((void *)startAddress), _length(length)
	{
	}

	DataAccessRegion(const void *startAddress, const void *endAddress)
		: _startAddress((void *)startAddress),
		_length((size_t)((char *)endAddress - (char *)startAddress))
	{
		assert(endAddress > startAddress);
	}

	DataAccessRegion() : _startAddress(nullptr), _length(0)
	{
	}

	bool empty() const
	{
		return (_startAddress == nullptr) && (_length == 0);
	}

	bool operator==(DataAccessRegion const &other) const
	{
		return (_startAddress == other._startAddress) && (_length == other._length);
	}

	bool operator!=(DataAccessRegion const &other) const
	{
		return (_startAddress != other._startAddress) || (_length != other._length);
	}


	void *getStartAddress() const
	{
		return _startAddress;
	}

	void *const &getStartAddressConstRef() const
	{
		return _startAddress;
	}

	void *getEndAddress() const
	{
		char *start = (char *) _startAddress;
		char *end = ((char *) start) + _length;

		return end;
	}

	size_t getSize() const
	{
		return _length;
	}


	std::pair<void *, void *> getBounds() const
	{
		char *start = (char *) _startAddress;
		char *end = ((char *) start) + _length - 1;

		return std::pair<void *, void *>(start, end);
	}


	//! \brief Returns the intersection or an empty DataAccessRegion if there is none
	DataAccessRegion intersect(DataAccessRegion const &other) const
	{
		FragmentBoundaries boundaries(*this, other);

		const void *start = std::max(boundaries._firstStart, boundaries._secondStart);
		const void *end = std::min(boundaries._firstEnd, boundaries._secondEnd);

		if (start < end) {
			return DataAccessRegion(start, end);
		}

		return DataAccessRegion();
	}


	bool contiguous(DataAccessRegion const &other) const
	{
		FragmentBoundaries boundaries(*this, other);

		return (boundaries._firstStart == boundaries._secondEnd
				|| boundaries._firstEnd == boundaries._secondStart);
	}


	DataAccessRegion contiguousUnion(DataAccessRegion const &other) const
	{
		assert(contiguous(other));
		assert(intersect(other).empty());

		FragmentBoundaries boundaries(*this, other);

		char *start = std::min(boundaries._firstStart, boundaries._secondStart);
		char *end = std::max(boundaries._firstEnd, boundaries._secondEnd);

		return DataAccessRegion(start, end);
	}


	bool fullyContainedIn(DataAccessRegion const &other) const
	{
		return (other.getStartAddress() <= this->getStartAddress()
			&& this->getEndAddress() <= other.getEndAddress());
	}

	bool fullyContainsRegion(DataAccessRegion const &other) const
	{
		return other.fullyContainedIn(*this);
	}

	bool containsAddress(void *ptr) const
	{
		return (getStartAddress() <= ptr && ptr < getEndAddress());
	}

	void processIntersectingFragments(
		DataAccessRegion const &fragmeterRegion,
		std::function<void(DataAccessRegion &)> thisOnlyProcessor,
		std::function<void(DataAccessRegion &)> intersectingProcessor,
		std::function<void(DataAccessRegion &)> otherOnlyProcessor
	) const {
		FragmentBoundaries boundaries(*this, fragmeterRegion);

		char *intersectionStart = std::max(boundaries._firstStart, boundaries._secondStart);
		char *intersectionEnd = std::min(boundaries._firstEnd, boundaries._secondEnd);

		// There must be an intersection
		assert(intersectionStart < intersectionEnd);

		// Intersection
		DataAccessRegion intersection(intersectionStart, intersectionEnd);
		intersectingProcessor(intersection);

		// Left of intersection
		if (boundaries._firstStart < intersectionStart) {
			DataAccessRegion leftOfIntersection(boundaries._firstStart, intersectionStart);
			thisOnlyProcessor(leftOfIntersection);
		} else if (boundaries._secondStart < intersectionStart) {
			DataAccessRegion leftOfIntersection(boundaries._secondStart, intersectionStart);
			otherOnlyProcessor(leftOfIntersection);
		}

		// Right of intersection
		if (intersectionEnd < boundaries._firstEnd) {
			DataAccessRegion rightOfIntersection(intersectionEnd, boundaries._firstEnd);
			thisOnlyProcessor(rightOfIntersection);
		} else if (intersectionEnd < boundaries._secondEnd) {
			DataAccessRegion rightOfIntersection(intersectionEnd, boundaries._secondEnd);
			otherOnlyProcessor(rightOfIntersection);
		}
	}

	friend inline std::ostream & operator<<(std::ostream &o, const DataAccessRegion& region)
	{
		return o << region._startAddress << ":" << region._length;
	}


};




#endif // DATA_ACCESS_REGION_HPP

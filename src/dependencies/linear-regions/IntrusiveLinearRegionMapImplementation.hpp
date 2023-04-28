/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2015-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INTRUSIVE_LINEAR_REGION_MAP_IMPLEMENTATION_HPP
#define INTRUSIVE_LINEAR_REGION_MAP_IMPLEMENTATION_HPP


#include <cassert>
#include <mutex>

#include "IntrusiveLinearRegionMap.hpp"


template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::processAll(std::function<bool(iterator)> processor)
{
	VERIFY_MAP();
	for (iterator it = BaseType::begin(); it != BaseType::end(); ) {
		iterator position = it;
		it++; // Advance before processing to allow the processor to fragment the node without passing a second time over some new fragments
		VERIFY_MAP();

		bool cont = processor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
		VERIFY_MAP();
		if (!cont) {
			return false;
		}
	}
	VERIFY_MAP();

	return true;
}

template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::processAllWithErase(
	std::function<bool(iterator)> processor
) {
	VERIFY_MAP();
	for (iterator it = BaseType::begin(); it != BaseType::end(); ) {
		iterator position = it;
		VERIFY_MAP();

		bool toerase = processor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
		VERIFY_MAP();
		if (toerase) {
			it = BaseType::erase(position);
		} else {
			it++;
		}
	}
	VERIFY_MAP();

	return true;
}

template <typename ContentType, class Hook>
void IntrusiveLinearRegionMap<ContentType, Hook>::processAllWithRestart(
	std::function<bool(iterator)> processor
) {
	VERIFY_MAP();
	for (iterator it = BaseType::begin(); it != BaseType::end(); ) {
		iterator position = it;
		it++; // Advance before processing to allow the processor to fragment the node without passing a second time over some new fragments
		VERIFY_MAP();

		// Keep an identifier for the current position so that the traversal can be restarted from there
		typename IntrusiveLinearRegionMapInternals::KeyOfNodeArtifact<ContentType>::type positionIdentifier =
			IntrusiveLinearRegionMapInternals::KeyOfNodeArtifact<ContentType>()(*position);

		bool cont = processor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
		VERIFY_MAP();
		if (!cont) {
			it = BaseType::find(positionIdentifier);
			assert(it != BaseType::end());
		}
	}
	VERIFY_MAP();
}


template <typename ContentType, class Hook>
void IntrusiveLinearRegionMap<ContentType, Hook>::processAllWithRearangement(
	std::function<bool(iterator)> processor
) {
	VERIFY_MAP();
	for (iterator it = BaseType::begin(); it != BaseType::end(); ) {
		iterator position = it;
		it++; // Advance before processing to allow the processor to fragment the node without passing a second time over some new fragments
		VERIFY_MAP();

		// Keep an identifier for the next position so that the traversal can be restarted from there
		bool nextIsEnd = (it == BaseType::end());
		typename IntrusiveLinearRegionMapInternals::KeyOfNodeArtifact<ContentType>::type positionIdentifier;
		if (!nextIsEnd) {
			positionIdentifier = IntrusiveLinearRegionMapInternals::KeyOfNodeArtifact<ContentType>()(*it);
		}

		bool cont = processor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
		VERIFY_MAP();
		if (!cont) {
			if (nextIsEnd) {
				return;
			}
			it = BaseType::lower_bound(positionIdentifier);
			// The next could end up being end() since the processor can have removed the remaining nodes
		}
	}
	VERIFY_MAP();
}


template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::processIntersecting(
	DataAccessRegion const &region,
	std::function<bool(iterator)> processor
) {
	VERIFY_MAP();
	iterator it = BaseType::lower_bound(region.getStartAddress());

	if (it != BaseType::begin()) {
		if ((it == BaseType::end()) || (it->getAccessRegion().getStartAddress() > region.getStartAddress())) {
			it--;
		}
	}

	VERIFY_MAP();
	while ((it != BaseType::end()) && (it->getAccessRegion().getStartAddress() < region.getEndAddress())) {
		VERIFY_MAP();
		// The "processor" may replace the node with something else, so advance before that happens
		iterator position = it;
		it++;

		if (!region.intersect(position->getAccessRegion()).empty()) {
			VERIFY_MAP();
			bool cont = processor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
			if (!cont) {
				return false;
			}
		}
		VERIFY_MAP();
	}

	return true;
}

template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::processIntersectingWithRecentAdditions(
	DataAccessRegion const &region,
	std::function<bool(iterator)> processor
) {
	VERIFY_MAP();
	iterator it = BaseType::lower_bound(region.getStartAddress());

	if (it != BaseType::begin()) {
		if ((it == BaseType::end()) || (it->getAccessRegion().getStartAddress() > region.getStartAddress())) {
			it--;
		}
	}

	VERIFY_MAP();
	while ((it != BaseType::end()) && (it->getAccessRegion().getStartAddress() < region.getEndAddress())) {
		VERIFY_MAP();
		iterator position = it;

		if (!region.intersect(position->getAccessRegion()).empty()) {
			VERIFY_MAP();
			bool cont = processor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
			if (!cont) {
				return false;
			}
		}
		VERIFY_MAP();

		++it;

		VERIFY_MAP();
	}

	return true;
}


template <typename ContentType, class Hook>
void IntrusiveLinearRegionMap<ContentType, Hook>::processIntersectingWithRestart(
	DataAccessRegion const &region,
	std::function<bool(iterator)> processor
) {
	VERIFY_MAP();
	iterator it = BaseType::lower_bound(region.getStartAddress());

	if (it != BaseType::begin()) {
		if ((it == BaseType::end()) || (it->getAccessRegion().getStartAddress() > region.getStartAddress())) {
			it--;
		}
	}

	VERIFY_MAP();
	while ((it != BaseType::end()) && (it->getAccessRegion().getStartAddress() < region.getEndAddress())) {
		VERIFY_MAP();
		// The "processor" may replace the node with something else, so advance before that happens
		iterator position = it;
		it++;

		// Keep an identifier for the current position so that the traversal can be restarted from there
		typename IntrusiveLinearRegionMapInternals::KeyOfNodeArtifact<ContentType>::type positionIdentifier =
		IntrusiveLinearRegionMapInternals::KeyOfNodeArtifact<ContentType>()(*position);

		if (!region.intersect(position->getAccessRegion()).empty()) {
			VERIFY_MAP();
			bool cont = processor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
			if (!cont) {
				it = BaseType::find(positionIdentifier);
				assert(it != BaseType::end());
			}
		}
		VERIFY_MAP();
	}
}


template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::processIntersectingAndMissing(
	DataAccessRegion const &region,
	std::function<bool(iterator)> intersectingProcessor,
	std::function<bool(DataAccessRegion const &region)> missingProcessor
) {
	VERIFY_MAP();
	if (BaseType::empty()) {
		return missingProcessor(region); // NOTE: an error here indicates that the lambda is missing the "bool" return type
	}

	VERIFY_MAP();
	iterator it = BaseType::lower_bound(region.getStartAddress());
	iterator initial = it;

	VERIFY_MAP();
	if (it != BaseType::begin()) {
		if ((it == BaseType::end()) || (it->getAccessRegion().getStartAddress() > region.getStartAddress())) {
			it--;
		}
	}

	void *lastEnd = region.getStartAddress();
	VERIFY_MAP();
	assert(!BaseType::empty());
	if (it->getAccessRegion().getEndAddress() <= region.getStartAddress()) {
		it = initial;
	}

	VERIFY_MAP();
	while ((it != BaseType::end()) && (it->getAccessRegion().getStartAddress() < region.getEndAddress())) {
		bool cont = true;

		// The "processor" may replace the node with something else, so advance before that happens
		iterator position = it;
		it++;

		VERIFY_MAP();
		if (lastEnd < position->getAccessRegion().getStartAddress()) {
			DataAccessRegion missingRegion(lastEnd, position->getAccessRegion().getStartAddress());
			VERIFY_MAP();
			cont = missingProcessor(missingRegion); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
			if (!cont) {
				return false;
			}
		}

		if (position->getAccessRegion().getEndAddress() <= region.getEndAddress()) {
			lastEnd = position->getAccessRegion().getEndAddress();
			VERIFY_MAP();
			cont = intersectingProcessor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
		} else {
			assert(position->getAccessRegion().getEndAddress() > region.getEndAddress());
			assert((position->getAccessRegion().getStartAddress() >= lastEnd) || (position->getAccessRegion().getStartAddress() < region.getStartAddress()));

			VERIFY_MAP();
			cont = intersectingProcessor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
			lastEnd = region.getEndAddress();
		}
		VERIFY_MAP();

		if (!cont) {
			return false;
		}
	}

	if (lastEnd < region.getEndAddress()) {
		DataAccessRegion missingRegion(lastEnd, region.getEndAddress());
		VERIFY_MAP();
		bool result = missingProcessor(missingRegion); // NOTE: an error here indicates that the lambda is missing the "bool" return type
		VERIFY_MAP();
		return result;
	}

	return true;
}


template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::processIntersectingAndMissingWithRecentAdditions(
	DataAccessRegion const &region,
	std::function<bool(iterator)> intersectingProcessor,
	std::function<bool(DataAccessRegion const &region)> missingProcessor
) {
	VERIFY_MAP();
	if (BaseType::empty()) {
		return missingProcessor(region); // NOTE: an error here indicates that the lambda is missing the "bool" return type
	}

	VERIFY_MAP();
	iterator it = BaseType::lower_bound(region.getStartAddress());
	iterator initial = it;

	VERIFY_MAP();
	if (it != BaseType::begin()) {
		if ((it == BaseType::end()) || (it->getAccessRegion().getStartAddress() > region.getStartAddress())) {
			it--;
		}
	}

	void *lastEnd = region.getStartAddress();
	VERIFY_MAP();
	assert(!BaseType::empty());
	if (it->getAccessRegion().getEndAddress() <= region.getStartAddress()) {
		it = initial;
	}

	VERIFY_MAP();
	while ((it != BaseType::end()) && (it->getAccessRegion().getStartAddress() < region.getEndAddress())) {
		bool cont = true;

		iterator position = it;

		VERIFY_MAP();
		if (lastEnd < position->getAccessRegion().getStartAddress()) {
			DataAccessRegion missingRegion(lastEnd, position->getAccessRegion().getStartAddress());
			VERIFY_MAP();
			cont = missingProcessor(missingRegion); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
			if (!cont) {
				return false;
			}
		}

		if (position->getAccessRegion().getEndAddress() <= region.getEndAddress()) {
			lastEnd = position->getAccessRegion().getEndAddress();
			VERIFY_MAP();
			cont = intersectingProcessor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
		} else {
			assert(position->getAccessRegion().getEndAddress() > region.getEndAddress());
			assert((position->getAccessRegion().getStartAddress() >= lastEnd) || (position->getAccessRegion().getStartAddress() < region.getStartAddress()));

			VERIFY_MAP();
			cont = intersectingProcessor(position); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
			lastEnd = region.getEndAddress();
		}
		VERIFY_MAP();

		++it;

		VERIFY_MAP();

		if (!cont) {
			return false;
		}
	}

	if (lastEnd < region.getEndAddress()) {
		DataAccessRegion missingRegion(lastEnd, region.getEndAddress());
		VERIFY_MAP();
		bool result = missingProcessor(missingRegion); // NOTE: an error here indicates that the lambda is missing the "bool" return type
		VERIFY_MAP();
		return result;
	}

	return true;
}


template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::processMissing(
	DataAccessRegion const &region,
	std::function<bool(DataAccessRegion const &region)> missingProcessor
) {
	VERIFY_MAP();
	return processIntersectingAndMissing(
		region,
		[&](__attribute__((unused)) iterator position) -> bool { return true; },
		missingProcessor
	);
}


template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::exists(
	DataAccessRegion const &region,
	std::function<bool(iterator)> condition
) {
	VERIFY_MAP();
	iterator it = BaseType::lower_bound(region.getStartAddress());

	if (it != BaseType::begin()) {
		if ((it == BaseType::end()) || (it->getAccessRegion().getStartAddress() > region.getStartAddress())) {
			it--;
		}
	}
	VERIFY_MAP();


	while ((it != BaseType::end()) && (it->getAccessRegion().getStartAddress() < region.getEndAddress())) {
		VERIFY_MAP();
		if (!region.intersect(it->getAccessRegion()).empty()) {
			VERIFY_MAP();
			bool found = condition(it); // NOTE: an error here indicates that the lambda is missing the "bool" return type
			VERIFY_MAP();
			if (found) {
				return true;
			}
		}
		it++;
	}
	VERIFY_MAP();

	return false;
}


template <typename ContentType, class Hook>
bool IntrusiveLinearRegionMap<ContentType, Hook>::contains(DataAccessRegion const &region)
{
	VERIFY_MAP();
	iterator it = BaseType::lower_bound(region.getStartAddress());

	VERIFY_MAP();
	if (it != BaseType::begin()) {
		if ((it == BaseType::end()) || (it->getAccessRegion().getStartAddress() > region.getStartAddress())) {
			it--;
		}
	}
	VERIFY_MAP();


	while ((it != BaseType::end()) && (it->getAccessRegion().getStartAddress() < region.getEndAddress())) {
		VERIFY_MAP();
		if (!region.intersect(it->getAccessRegion()).empty()) {
			return true;
		}
		it++;
	}
	VERIFY_MAP();

	return false;
}


template <typename ContentType, class Hook>
typename IntrusiveLinearRegionMap<ContentType, Hook>::iterator
IntrusiveLinearRegionMap<ContentType, Hook>::fragmentByIntersection(
	typename IntrusiveLinearRegionMap<ContentType, Hook>::iterator position,
	DataAccessRegion const &fragmenterRegion,
	std::function<ContentType *(ContentType &)> duplicator,
	std::function<void(ContentType *, ContentType *)> postprocessor
) {
	iterator intersectionPosition = BaseType::end();
	DataAccessRegion originalRegion = position->getAccessRegion();
	ContentType &contents = *position;

	VERIFY_MAP();
	originalRegion.processIntersectingFragments(
		fragmenterRegion,
		/* originalRegion only */
		[&](DataAccessRegion const &region) {
			VERIFY_MAP();
			ContentType *newContents = duplicator(contents); // An error here indicates that the duplicator is missing the "ContentType *" return type
			newContents->setAccessRegion(region);
			BaseType::insert(*newContents);
			postprocessor(newContents, &(*position));
			VERIFY_MAP();
		},
		/* intersection */
		[&](DataAccessRegion const &region) {
			VERIFY_MAP();
			assert(region == originalRegion.intersect(fragmenterRegion));
			position->setAccessRegion(region);
			intersectionPosition = position;
			assert(intersectionPosition->getAccessRegion() == region);
			postprocessor(&(*position), &(*position));
			assert(intersectionPosition->getAccessRegion() == region);
			VERIFY_MAP();
		},
		/* fragmeterRegion only */
		[&](__attribute__((unused)) DataAccessRegion const &region) {
			VERIFY_MAP();
		}
	);

	assert((intersectionPosition == BaseType::end()) || (intersectionPosition->getAccessRegion() == originalRegion.intersect(fragmenterRegion)));
	return intersectionPosition;
}


template <typename ContentType, class Hook>
typename IntrusiveLinearRegionMap<ContentType, Hook>::iterator
IntrusiveLinearRegionMap<ContentType, Hook>::fragmentByIntersection(
	typename IntrusiveLinearRegionMap<ContentType, Hook>::iterator position,
	DataAccessRegion const &fragmenterRegion,
	std::function<ContentType *(ContentType &)> duplicator
) {
	DataAccessRegion originalRegion = position->getAccessRegion();
	ContentType &contents = *position;

	VERIFY_MAP();
	const char *originalStart = (const char *) originalRegion.getStartAddress();
	const char *originalEnd = (const char *) originalRegion.getEndAddress();
	const char *secondStart = (const char *) fragmenterRegion.getStartAddress();
	const char *secondEnd = (const char *) fragmenterRegion.getEndAddress();

	const char *intersectionStart = std::max(originalStart, secondStart);
	const char *intersectionEnd = std::min(originalEnd, secondEnd);

	// There must be an intersection
	assert(intersectionStart < intersectionEnd);

	// Intersection
	DataAccessRegion intersection(intersectionStart, intersectionEnd);
	VERIFY_MAP();
	assert(intersection == originalRegion.intersect(fragmenterRegion));
	position->setAccessRegion(intersection);
	iterator intersectionPosition = position;
	assert(intersectionPosition->getAccessRegion() == intersection);
	VERIFY_MAP();

	// Left of intersection
	if (originalStart < intersectionStart) {
		DataAccessRegion leftOfIntersection(originalStart, intersectionStart);
		VERIFY_MAP();
		ContentType *newContents = duplicator(contents); // An error here indicates that the duplicator is missing the "ContentType *" return type
		newContents->setAccessRegion(leftOfIntersection);
		iterator pos = BaseType::insert(intersectionPosition, *newContents);
		pos++;
		assert(pos == intersectionPosition);
		VERIFY_MAP();
	}

	// Right of intersection
	if (intersectionEnd < originalEnd) {
		DataAccessRegion rightOfIntersection(intersectionEnd, originalEnd);
		VERIFY_MAP();
		ContentType *newContents = duplicator(contents); // An error here indicates that the duplicator is missing the "ContentType *" return type
		newContents->setAccessRegion(rightOfIntersection);
		iterator rightPosition = intersectionPosition;
		rightPosition++;
		iterator pos = BaseType::insert(rightPosition, *newContents);
		pos++;
		assert(pos == rightPosition);
		VERIFY_MAP();
	}

	assert((intersectionPosition == BaseType::end()) || (intersectionPosition->getAccessRegion() == originalRegion.intersect(fragmenterRegion)));
	return intersectionPosition;
}


template <typename ContentType, class Hook>
void IntrusiveLinearRegionMap<ContentType, Hook>::fragmentIntersecting(
	DataAccessRegion const &region,
	std::function<ContentType *(ContentType &)> duplicator,
	std::function<void(ContentType *, ContentType *)> postprocessor
) {
	processIntersecting(
		region,
		[&](iterator position) -> bool {
			VERIFY_MAP();
			fragmentByIntersection(position, region, duplicator, postprocessor);
			VERIFY_MAP();
			return true;
		}
	);
}


#endif // INTRUSIVE_LINEAR_REGION_MAP_IMPLEMENTATION_HPP

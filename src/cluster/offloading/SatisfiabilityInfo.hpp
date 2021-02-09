/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019--2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef SATISFIABILITY_INFO_HPP
#define SATISFIABILITY_INFO_HPP

#include <map>
#include <vector>
#include "WriteID.hpp"
#include "OffloadedTaskId.hpp"

class ClusterNode;

namespace TaskOffloading {

	struct SatisfiabilityInfo {
		//! The region related with the satisfiability info
		DataAccessRegion _region;

		//! node index of the current location
		//! -42 means the directory
		//! -1 means nullptr (only if sending write satisfiability before read satisfiability: very rare)
		int _src;

		//! makes access read/write satisfied
		bool _readSat, _writeSat;

		// The unique writeID
		WriteID _writeID;

		//! predecessor remote task ID expected for remote namespace propagation
		//! this is will play two roles.
		//! a) When used in a tasknew it will be the namespacePredecessor
		//! b) When used in a satisfiability message it will be the task id (access originator)
		OffloadedTaskId _id;

		//! Namespace reader num, to ensure propagation among concurrent readers respects
		//! program's sequential order
		int _namespaceReaderNum;

		// 0, or eager weak send tag
		int _eagerSendTag;


		SatisfiabilityInfo(
			DataAccessRegion const &region, int src,
			bool read, bool write,
			WriteID writeID, OffloadedTaskId id, int namespaceReaderNum, int eagerSendTag
		) : _region(region), _src(src),
			_readSat(read), _writeSat(write),
			_writeID(writeID), _id(id), _namespaceReaderNum(namespaceReaderNum), _eagerSendTag(eagerSendTag)
		{
			// std::cout << "construct SatisfiabilityInfo with nrp = " << namespacePredecessor << "\n";
		}

		// TODO: Add code to differentiate when in a tasknew or satisfiability-message
		friend std::ostream& operator<<(std::ostream &o, SatisfiabilityInfo const &satInfo)
		{
			return o << "[SatReg:" << satInfo._region
				<< " r:" << satInfo._readSat << " w:" << satInfo._writeSat
				<< " loc:" << satInfo._src << " task: " << satInfo._id << "]";
		}
	};

	typedef std::vector<SatisfiabilityInfo> SatisfiabilityInfoVector;
	typedef std::map<ClusterNode *, std::vector<SatisfiabilityInfo>> SatisfiabilityInfoMap;
}


#endif /* SATISFIABILITY_INFO_HPP */

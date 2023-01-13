/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <cstdlib>
#include <vector>
#include <algorithm>

#include "InstrumentCluster.hpp"
#include "MPIDataTransfer.hpp"
#include "MPIMessenger.hpp"
#include "cluster/messages/Message.hpp"
#include "cluster/polling-services/ClusterServicesPolling.hpp"
#include "cluster/polling-services/ClusterServicesTask.hpp"
#include "lowlevel/FatalErrorHandler.hpp"
#include "MPIErrorHandler.hpp"
#include "MessageId.hpp"

#include <ClusterManager.hpp>
#include <ClusterNode.hpp>
#include <MemoryAllocator.hpp>
#include <alloca.h>

#pragma GCC visibility push(default)
#include <mpi.h>
#pragma GCC visibility pop

// Split a data transfer by the max message size
void MPIMessenger::forEachDataPart(
	void *startAddress,
	size_t size,
	int messageId,
	std::function<void(void*, size_t, int)> processor
) {
	char *currAddress = (char *) startAddress;
	const char *endAddress = currAddress + size;
	int ofs = 0;

#ifndef NDEBUG
	const int nFragments = ClusterManager::getMPIFragments(DataAccessRegion(startAddress, size));
#endif

	while (currAddress < endAddress) {
		assert(ofs < nFragments);
		const size_t currSize = std::min<size_t>(endAddress - currAddress, _messageMaxSize);
		const int currMessageId = MessageId::messageIdInGroup(messageId, ofs);
		processor(currAddress, currSize, currMessageId);
		currAddress += currSize;
		ofs++;
	}
}

// Given the upper bound on the MPI tag, return the largest value that can be
// used as a bitmask for the message tag. The returned value will be used as a
// bitmask inside createTag, effectively as a substitute for the module operator.
// For this reason, we need to take the largest power of two minus one that is no
// larger than the maximum permissible message tag.
int MPIMessenger::convertToBitMask(int mpi_ub_tag) const
{
	// Do it the simple way. No need to try to be clever.
	int curr = INT_MAX;
	while (curr && curr > mpi_ub_tag) {
		curr >>= 1; // Too big: divide it by two
	}
	assert((curr & 0xff) == 0xff); // Need bottom eight bits set to extract the message type
	return curr;
}

/*
 * Parse cluster.hybrid.split passed in the argument
 * to determine the apprank number of this instance
 */
void MPIMessenger::setApprankNumber(const std::string &clusterSplit, int &internalRank)
{
	std::stringstream ss(clusterSplit);
	int countInstancesThisPhysicalNode = 0;   // Number of instances so far on the current node
	int countInstances = 0;           // Total number of instances so far
	bool done = false;

	// This instance's apprank number
	_apprankNum = -1;

	int apprank;
	for (apprank=0; !done; apprank++) {
		for (int intRank = 0; !done; intRank++) {
			int physicalNodeNum;
			ss >> physicalNodeNum;

			FatalErrorHandler::failIf( physicalNodeNum < 0 || physicalNodeNum >= _numNodes,
									   "node ", physicalNodeNum, " invalid in cluster.hybrid.split configuration");

			if (physicalNodeNum == _physicalNodeNum) {
				// My node
				if (countInstancesThisPhysicalNode == _indexThisPhysicalNode) {
					// Apprank number for this instance
					assert(_apprankNum == -1);
					internalRank = intRank;
					_apprankNum = apprank;
					_instrumentationRank = countInstances;
				}
				countInstancesThisPhysicalNode ++;
				_isMasterThisNode.push_back(intRank == 0); // make a note of whether it's a master or not
			}
			countInstances ++;

			// Get separator and continue
			char sep = '\0';
			ss >> sep;
			if (sep == '\0')
				done = true;
			if (sep == ';')
				break;  // next apprank
		}
	}
	_numAppranks = apprank;
	_numInstancesThisNode = countInstancesThisPhysicalNode;
	FatalErrorHandler::failIf( countInstances != _numExternalRanks,
							   "Wrong number of instances in cluster.hybrid.split configuration");
	FatalErrorHandler::failIf( _apprankNum == -1,
							   "Node ", _physicalNodeNum, " index ", _indexThisPhysicalNode, " not found in cluster.hybrid.split configuration");
}

/*
 * Environment variables to check to determine the node number
 * (should be set to an integer from 0 to <num_nodes>-1)
 */
static std::vector<const char *> node_num_envvars
{
	"_NANOS6_NODEID",    // _NANOS6_NODEID overrides all other sources
	"SLURM_NODEID"      // SLURM
};

void MPIMessenger::getNodeNumber()
{
	// Find physical node number from the environment variable
	_physicalNodeNum = -1;
	for (auto envVarName : node_num_envvars) {
		EnvironmentVariable<std::string> envVar(envVarName, "");
		if (envVar.isPresent()) {
			const std::string &value = envVar.getValue();
			_physicalNodeNum = std::stoi(value);
		}
	}

	// If the node number couldn't be determined, then silently change the
	// node number to zero. It may be running on a login node.
	if (_physicalNodeNum == -1) {
		_physicalNodeNum = 0;
	}

	// Find and broadcast number of nodes
	int maxNodeNum;
	MPI_Reduce(&_physicalNodeNum, &maxNodeNum, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
	_numNodes = maxNodeNum + 1;  // only valid on rank 0 of MPI_COMM_WORLD
	MPI_Bcast(&_numNodes, 1, MPI_INT, 0, MPI_COMM_WORLD);

	// Find index among instances on the same node
	MPI_Comm comm_within_node;
	MPI_Comm_split(MPI_COMM_WORLD, /* color */ _physicalNodeNum, /* key */_externalRank, &comm_within_node);
	MPI_Comm_rank(comm_within_node, &_indexThisPhysicalNode);
}

void MPIMessenger::splitCommunicator(const std::string &clusterSplit)
{
	// Used for splitting the communicator. It ought to be the same as the actual internal
	// rank, but the definitive value of the internalRank will be taken from INTRA_COMM,
	// which will be created by MPI_Comm_split.
	int internalRank = 0;

	// Find apprank number by parsing cluster.hybrid.split configuration
	setApprankNumber(clusterSplit, /* out */ internalRank);

	// Create intra communicator for this apprank (_wrank and _wsize will be determined from this)
	MPI_Comm_split(MPI_COMM_WORLD, /* color */ _apprankNum, /* key */ internalRank, &INTRA_COMM);
}

MPIMessenger::MPIMessenger(int argc, char **argv) : Messenger(argc, argv)
{
	int support, ret;

	ret = MPI_Init_thread(&_argc, &_argv, MPI_THREAD_MULTIPLE, &support);
	MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	if (support != MPI_THREAD_MULTIPLE) {
		std::cerr << "Could not initialize multithreaded MPI" << std::endl;
		abort();
	}

	//! Get the upper-bound tag supported by current MPI implementation
	int ubIsSetFlag = 0, *mpi_ub_tag = nullptr;
	ret = MPI_Comm_get_attr(MPI_COMM_WORLD, MPI_TAG_UB, &mpi_ub_tag, &ubIsSetFlag);
	MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	assert(mpi_ub_tag != nullptr);
	assert(ubIsSetFlag != 0);

	_mpi_ub_tag = convertToBitMask(*mpi_ub_tag);
	assert(_mpi_ub_tag > 0);

	// Set the error handler to MPI_ERRORS_RETURN so we can use a check latter.  The error handler
	// is inherited by any created messenger latter, so we don't need to set it again.  TODO:
	// Instead of checking on every MPI call we must define a propper MPI error handler.  The
	// problem with this is that some parameters are implementation specific...
	ret = MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
	MPIErrorHandler::handle(ret, MPI_COMM_WORLD);

	//! Save the parent communicator
	ret = MPI_Comm_get_parent(&PARENT_COMM);
	MPIErrorHandler::handle(ret, MPI_COMM_WORLD);

	//! Create a new communicator
	ConfigVariable<std::string> clusterSplitEnv("cluster.hybrid.split");
	if (PARENT_COMM == MPI_COMM_NULL) {

		//! Calculate number of nodes and node number
		getNodeNumber();

		//! Get external rank (in MPI_COMM_WORLD)
		ret = MPI_Comm_size(MPI_COMM_WORLD, &_numExternalRanks);
		ret = MPI_Comm_rank(MPI_COMM_WORLD, &_externalRank);

		// When there is not parent it is part of the initial communicator.
		if (clusterSplitEnv.isPresent() && clusterSplitEnv.getValue().length() > 0) {
			// Run in hybrid MPI + OmpSs-2@Cluster mode
			std::string clusterSplit = clusterSplitEnv.getValue();
			splitCommunicator(clusterSplit);
		} else {
			// Run in pure OmpSs-2@Cluster mode
			_apprankNum = 0;
			_numAppranks = 1;
			int n = _numExternalRanks / _numNodes;
			if (_physicalNodeNum < (_numExternalRanks % _numNodes)) {
				n++;
			}
			_numInstancesThisNode = n;

			for (int i=0; i<_numInstancesThisNode; i++) {
				bool isMasterInstance = (i==0) && (_physicalNodeNum==0); /* first instance on first node */
				_isMasterThisNode.push_back(isMasterInstance);
			}
			_instrumentationRank = _externalRank;

			//! Create a new communicator
			ret = MPI_Comm_dup(MPI_COMM_WORLD, &INTRA_COMM);
		}
	} else {
		// This is a spawned process.
		_apprankNum = 0;
		_numAppranks = 1;
		_numInstancesThisNode = 1; // Not used
		_instrumentationRank = _externalRank;
		ret = MPI_Intercomm_merge(PARENT_COMM, true,  &INTRA_COMM);
		FatalErrorHandler::failIf(clusterSplitEnv.isPresent(), "Malleability doesn't work with hybrid MPI+OmpSs-2@Cluster");
	}
	MPIErrorHandler::handle(ret, MPI_COMM_WORLD);

	// Get the user config to use a different communicator for data_raw.
	ConfigVariable<bool> mpi_comm_data_raw("cluster.mpi.comm_data_raw");
	_mpi_comm_data_raw = mpi_comm_data_raw.getValue();

	if (_mpi_comm_data_raw) {
		ret = MPI_Comm_dup(INTRA_COMM, &INTRA_COMM_DATA_RAW);
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	} else {
		INTRA_COMM_DATA_RAW = INTRA_COMM;
	}

	ret = MPI_Comm_rank(INTRA_COMM, &_wrank);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(_wrank >= 0);

	ret = MPI_Comm_size(INTRA_COMM, &_wsize);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(_wsize > 0);

}


void MPIMessenger::shutdown()
{
	int ret;

	if (_mpi_comm_data_raw ==  true) {
#ifndef NDEBUG
		int compare = 0;
		ret = MPI_Comm_compare(INTRA_COMM_DATA_RAW, INTRA_COMM, &compare);
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
		assert(compare !=  MPI_IDENT);
#endif // NDEBUG

		//! Release the INTRA_COMM_DATA_RAW
		ret = MPI_Comm_free(&INTRA_COMM_DATA_RAW);
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	}

	//! Release the intra-communicator
	ret = MPI_Comm_free(&INTRA_COMM);
	MPIErrorHandler::handle(ret, MPI_COMM_WORLD);

	ret = MPI_Finalize();
	MPIErrorHandler::handle(ret, MPI_COMM_WORLD);

	// After MPI finalization there shouldn't be any pending message. But we don't actually know if
	// some delayed message has arrived from a third remote node. So we clear here and the latter
	// assert may be fine.
	RequestContainer<Message>::clear();
	RequestContainer<DataTransfer>::clear();
}


MPIMessenger::~MPIMessenger()
{
#ifndef NDEBUG
#ifndef EXTRAE_ENABLED
	int finalized = 0;
	int ret = MPI_Finalized(&finalized);
	assert(ret == MPI_SUCCESS);
	assert(finalized == 1);
#endif // EXTRAE_ENABLED

	assert(RequestContainer<Message>::isCleared());
	assert(RequestContainer<DataTransfer>::isCleared());
#endif // NDEBUG
}

void MPIMessenger::shareDLBInfo()
{
	// Note: If the All gather pattern continues, then maybe it worth considering to use
	// MPI_Iallgather with a waitall at the end of all to create a single synchronization point.

	// Create the application communicator (Hybrid MPI+OmpSs applications)
	int ret = MPI_Comm_split(MPI_COMM_WORLD, _wrank, _apprankNum, &APP_COMM);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	if (_wrank > 0) {
		//! Invalid to use application communicator on slave nodes
		APP_COMM = MPI_COMM_NULL;
	}

	// Create map from internal rank to external rank
	_internalRankToExternalRank.resize(_wsize);
	ret = MPI_Allgather(
		&_externalRank, 1, MPI_INT,
		_internalRankToExternalRank.data(), 1, MPI_INT, INTRA_COMM
	);
	MPIErrorHandler::handle(ret, INTRA_COMM);

	// This node to external rank
	_instanceThisNodeToExternalRank.resize(_numInstancesThisNode);
	MPI_Comm tempNodeComm;
	ret = MPI_Comm_split(
		MPI_COMM_WORLD,
		/* color */ _physicalNodeNum,
		/* key */ _indexThisPhysicalNode,
		&tempNodeComm
	);
	MPIErrorHandler::handle(ret, INTRA_COMM);

#ifndef NDEBUG
	// This is a defensive check to assert that the next operation does not produce overflow...
	// you know there are never not enough assertions
	int checksize = 0;
	ret = MPI_Comm_size(tempNodeComm, &checksize);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(checksize == _numInstancesThisNode);
#endif // NDEBUG

	// External Rank
	ret = MPI_Allgather(
		&_externalRank, 1, MPI_INT,
		_instanceThisNodeToExternalRank.data(), 1, MPI_INT, tempNodeComm
	);
	MPIErrorHandler::handle(ret, INTRA_COMM);

	// Internal rank
	_internalRankToInstrumentationRank.resize(_wsize);
	ret = MPI_Allgather(
		&_instrumentationRank, 1, MPI_INT,
		_internalRankToInstrumentationRank.data(), 1, MPI_INT, INTRA_COMM
	);

	ret = MPI_Comm_free(&tempNodeComm);
	MPIErrorHandler::handle(ret, INTRA_COMM);
}

void MPIMessenger::sendMessage(Message *msg, ClusterNode const *toNode, bool block)
{
	int ret;
	Message::Deliverable *delv = msg->getDeliverable();
	const int mpiDst = toNode->getCommIndex();
	const size_t msgSize = sizeof(delv->header) + delv->header.size;

	//! At the moment we use the Message id and the Message type to create
	//! the MPI tag of the communication
	int tag = createTag(delv);

	assert(mpiDst < _wsize && mpiDst != _wrank);
	assert(delv->header.size != 0);

	Instrument::clusterSendMessage(msg, mpiDst);

	if (block) {
		Instrument::MPILock();
		ret = MPI_Send((void *)delv, msgSize, MPI_BYTE, mpiDst, tag, INTRA_COMM);
		Instrument::MPIUnLock();
		MPIErrorHandler::handle(ret, INTRA_COMM);

		// Note: instrument before mark as completed, otherwise possible use-after-free
		Instrument::clusterSendMessage(msg, -1);
		msg->markAsCompleted();
		return;
	}

	MPI_Request *request = (MPI_Request *)MemoryAllocator::alloc(sizeof(MPI_Request));
	FatalErrorHandler::failIf(request == nullptr, "Could not allocate memory for MPI_Request");

	Instrument::MPILock();
	ret = MPI_Isend((void *)delv, msgSize, MPI_BYTE, mpiDst, tag, INTRA_COMM, request);
	Instrument::MPIUnLock();

	MPIErrorHandler::handle(ret, INTRA_COMM);

	msg->setMessengerData((void *)request);

	// Note instrument before add as pending, otherwise can be processed and freed => use-after-free
	Instrument::clusterSendMessage(msg, -1);

	ClusterPollingServices::PendingQueue<Message>::addPending(msg);
}

DataTransfer *MPIMessenger::sendData(
	const DataAccessRegion &region,
	const ClusterNode *to,
	int messageId,
	bool block,
	bool instrument
) {
	int ret;
	const int mpiDst = to->getCommIndex();
	void *address = region.getStartAddress();

	const size_t size = region.getSize();

	assert(mpiDst < _wsize && mpiDst != _wrank);

	if (instrument) {
		Instrument::clusterDataSend(address, size, mpiDst, messageId);
	}

	if (block) {
		Instrument::MPILock();
		forEachDataPart(
			address,
			size,
			messageId,
			[&](void *currAddress, size_t currSize, int currMessageId) {
				int tag = getTag(currMessageId);
				ret = MPI_Send(currAddress, currSize, MPI_BYTE, mpiDst, tag, INTRA_COMM_DATA_RAW);
				MPIErrorHandler::handle(ret, INTRA_COMM_DATA_RAW);
			}
		);
		Instrument::MPIUnLock();

		return nullptr;
	}

	MPI_Request *request = (MPI_Request *)MemoryAllocator::alloc(sizeof(MPI_Request));

	FatalErrorHandler::failIf(request == nullptr, "Could not allocate memory for MPI_Request");

	// TODO: This code overwrites the request, so the previous request may become missing or cause
	// some problems as it is not set release. Some implementations may handle differently this
	// situation.
	Instrument::MPILock();
	forEachDataPart(
		address,
		size,
		messageId,
		[&](void *currAddress, size_t currSize, int currMessageId) {
			int tag = getTag(currMessageId);
			ret = MPI_Isend(currAddress, currSize, MPI_BYTE, mpiDst, tag, INTRA_COMM_DATA_RAW, request);
			MPIErrorHandler::handle(ret, INTRA_COMM_DATA_RAW);
		}
	);
	Instrument::MPIUnLock();

	if (instrument) {
		Instrument::clusterDataSend(NULL, 0, mpiDst, -1);
	}

	return new MPIDataTransfer(region, ClusterManager::getCurrentMemoryNode(),
		to->getMemoryNode(), request, mpiDst, messageId, /* isFetch */ false);
}

DataTransfer *MPIMessenger::fetchData(
	const DataAccessRegion &region,
	const ClusterNode *from,
	int messageId,
	bool block,
	bool instrument
) {
	int ret;
	const int mpiSrc = from->getCommIndex();
	void *address = region.getStartAddress();
	size_t size = region.getSize();

	assert(mpiSrc < _wsize && mpiSrc != _wrank);

	if (block) {
		Instrument::MPILock();
		forEachDataPart(
			address,
			size,
			messageId,
			[&](void *currAddress, size_t currSize, int currMessageId) {
				int tag = getTag(currMessageId);
				ret = MPI_Recv(currAddress, currSize, MPI_BYTE, mpiSrc, tag, INTRA_COMM_DATA_RAW, MPI_STATUS_IGNORE);
				MPIErrorHandler::handle(ret, INTRA_COMM_DATA_RAW);
			}
		);
		Instrument::MPIUnLock();
		if (instrument) {
			Instrument::clusterDataReceived(address, size, mpiSrc, messageId);
		}

		return nullptr;
	}

	MPI_Request *request = (MPI_Request *)MemoryAllocator::alloc(sizeof(MPI_Request));

	FatalErrorHandler::failIf(request == nullptr, "Could not allocate memory for MPI_Request");

	Instrument::MPILock();
	forEachDataPart(
		address,
		size,
		messageId,
		[&](void *currAddress, size_t currSize, int currMessageId) {
			int tag = getTag(currMessageId);
			ret = MPI_Irecv(currAddress, currSize, MPI_BYTE, mpiSrc, tag, INTRA_COMM_DATA_RAW, request);
			MPIErrorHandler::handle(ret, INTRA_COMM_DATA_RAW);
		}
	);
	Instrument::MPIUnLock();

	return new MPIDataTransfer(region, from->getMemoryNode(),
		ClusterManager::getCurrentMemoryNode(), request, mpiSrc, messageId, /* isFetch */ true);
}

void MPIMessenger::synchronizeWorld(void)
{
	int ret = MPI_Barrier(MPI_COMM_WORLD);
	MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
}

void MPIMessenger::synchronizeAll(void)
{
	Instrument::MPILock();
	int ret = MPI_Barrier(INTRA_COMM);
	Instrument::MPIUnLock();
	MPIErrorHandler::handle(ret, INTRA_COMM);
}

Message *MPIMessenger::checkMail(void)
{
	int ret, flag, count;
	MPI_Status status;

	Instrument::MPILock();
	ret = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, INTRA_COMM, &flag, &status);
	Instrument::MPIUnLock();
	MPIErrorHandler::handle(ret, INTRA_COMM);

	if (!flag) {
		return nullptr;
	}

	//! DATA_RAW type of messages will be received by matching 'fetchData' methods
	const int type = status.MPI_TAG & 0xff;

	// INIT_SPAWNED messages are received ONLY before the polling services were started; so we never
	// receive this message in the check-mail unless there is an error.
	assert(type != DATA_RAW); // DATA_RAW is sent on INTRA_COMM_DATA_RAW

	ret = MPI_Get_count(&status, MPI_BYTE, &count);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(count != 0);

	Message::Deliverable *dlv = (Message::Deliverable *) malloc(count);
	FatalErrorHandler::failIf(dlv == nullptr, "malloc for message returned null");

	Instrument::clusterReceiveMessage(type, nullptr);

	Instrument::MPILock();
	ret = MPI_Recv((void *)dlv, count, MPI_BYTE, status.MPI_SOURCE,
		status.MPI_TAG, INTRA_COMM, MPI_STATUS_IGNORE);
	Instrument::MPIUnLock();
	MPIErrorHandler::handle(ret, INTRA_COMM);

	Message *msg
		= GenericFactory<int, Message*, Message::Deliverable*>::getInstance().create(type, dlv);
	assert(msg->getType() == type);

	Instrument::clusterReceiveMessage(type, msg);

	return msg;
}


template <typename T>
void MPIMessenger::testCompletionInternal(std::vector<T *> &pendings)
{
	const size_t msgCount = pendings.size();
	assert(msgCount > 0);

	int completedCount;

	RequestContainer<T>::reserve(msgCount);

	for (size_t i = 0; i < msgCount; ++i) {
		T *msg = pendings[i];
		assert(msg != nullptr);

		MPI_Request *req = (MPI_Request *)msg->getMessengerData();
		assert(req != nullptr);

		RequestContainer<T>::requests[i] = *req;
	}

	Instrument::MPILock();
	const int ret = MPI_Testsome(
		(int) msgCount,
		RequestContainer<T>::requests,
		&completedCount,
		RequestContainer<T>::finished,
		RequestContainer<T>::status
	);
	Instrument::MPIUnLock();

	MPIErrorHandler::handleErrorInStatus(
		ret, INTRA_COMM,
		completedCount, RequestContainer<T>::status
	);

	for (int i = 0; i < completedCount; ++i) {
		const int index = RequestContainer<T>::finished[i];
		TransferBase *msg = pendings[index];

		msg->markAsCompleted();
		MPI_Request *req = (MPI_Request *) msg->getMessengerData();
		MemoryAllocator::free(req, sizeof(MPI_Request));
	}
}

void MPIMessenger::waitAllCompletion(std::vector<TransferBase *> &pendings)
{
	const size_t total = (int) pendings.size();
	assert(total > 0);

	// This function is not intended to be used for a huge number of requests, so we can use alloca
	// and live with it and will be faster.
	MPI_Request *requests = (MPI_Request *) alloca(total * sizeof(MPI_Request));
	MPI_Status *statuses = (MPI_Status *) alloca(total * sizeof(MPI_Status));
	assert(requests != nullptr);
	assert(statuses != nullptr);

	for (size_t i = 0; i < total; ++i) {
		assert(pendings[i] != nullptr);

		MPI_Request *req = (MPI_Request *) pendings[i]->getMessengerData();
		assert(req != nullptr);

		requests[i] = *req;
	}

	Instrument::MPILock();
	const int ret = MPI_Waitall(total, requests, statuses);
	Instrument::MPIUnLock();
	MPIErrorHandler::handleErrorInStatus(ret, INTRA_COMM, total, statuses);

	for (size_t i = 0; i < total; ++i) {
		TransferBase *msg = pendings[i];
		assert(msg != nullptr);

		msg->markAsCompleted();
		MPI_Request *req = (MPI_Request *) msg->getMessengerData();
		MemoryAllocator::free(req, sizeof(MPI_Request));

		assert(msg->isCompleted());
		delete msg;
	}
}

void MPIMessenger::summarizeSplit() const
{
	Instrument::summarizeSplit(_externalRank, _physicalNodeNum, _apprankNum);
}


int MPIMessenger::nanos6Spawn(int delta)
{
	assert(delta > 0);
	MPI_Comm newintra = MPI_COMM_NULL;               // Variable for intracomm
	MPI_Comm newinter = MPI_COMM_NULL;               // Temporal intercomm

	MPI_Info info;
	MPI_Info_create(&info);
	MPI_Info_set(info, "host", "s08r2b24");

	int errcode = 0;

	int ret = MPI_Comm_spawn(_argv[0], &_argv[1], delta, info, 0, INTRA_COMM, &newinter, &errcode);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	FatalErrorHandler::failIf(errcode != MPI_SUCCESS, "New process returned error code: ", errcode);

	MPI_Comm_free(&INTRA_COMM);                      // Free old intracomm before.
	MPI_Intercomm_merge(newinter, false, &newintra); // Create new intra

	INTRA_COMM = newintra;                           // Reassign the intra to the new one
	MPI_Comm_free(&newinter);                        // Free the created intercomm

	MPI_Info_free(&info);

	//! make sure the new communicator returns errors
	if (_mpi_comm_data_raw) {
		MPI_Comm_free(&INTRA_COMM_DATA_RAW);
		ret = MPI_Comm_dup(INTRA_COMM, &INTRA_COMM_DATA_RAW);
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	} else {
		INTRA_COMM_DATA_RAW = INTRA_COMM;
	}

	int newrank = -1, newsize = -1;;
	ret = MPI_Comm_rank(INTRA_COMM, &newrank);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(newrank == _wrank);

	ret = MPI_Comm_size(INTRA_COMM, &newsize);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(newsize = _wsize + delta);
	_wsize = newsize;

	return delta;
}

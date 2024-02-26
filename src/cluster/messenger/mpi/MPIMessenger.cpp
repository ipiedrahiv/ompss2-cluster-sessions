/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include <dlfcn.h>
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

#include <SlurmAPI.hpp>
#include <ClusterManager.hpp>
#include <ClusterNode.hpp>
#include <MemoryAllocator.hpp>
#include <alloca.h>

#pragma GCC visibility push(default)
#include <mpi.h>
#pragma GCC visibility pop

#include "messenger/mpi/TAMPIInteroperability.hpp"

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
	const int nFragments = this->getMessageFragments(DataAccessRegion(startAddress, size));
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
	#ifdef HAVE_SESSIONS
	MPI_Reduce(&_physicalNodeNum, &maxNodeNum, 1, MPI_INT, MPI_MAX, 0, MPI_SESSION_WORLD_COMM);
	_numNodes = maxNodeNum + 1;  // only valid on rank 0 of MPI_SESSION_WORLD_COMM
	MPI_Bcast(&_numNodes, 1, MPI_INT, 0, MPI_SESSION_WORLD_COMM);

	// Find index among instances on the same node
	MPI_Comm comm_within_node;
	MPI_Comm_split(MPI_SESSION_WORLD_COMM, /* color */ _physicalNodeNum, /* key */_externalRank, &comm_within_node);
	MPI_Comm_rank(comm_within_node, &_indexThisPhysicalNode);
	#else
	MPI_Reduce(&_physicalNodeNum, &maxNodeNum, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
	_numNodes = maxNodeNum + 1;  // only valid on rank 0 of MPI_COMM_WORLD
	MPI_Bcast(&_numNodes, 1, MPI_INT, 0, MPI_COMM_WORLD);

	// Find index among instances on the same node
	MPI_Comm comm_within_node;
	MPI_Comm_split(MPI_COMM_WORLD, /* color */ _physicalNodeNum, /* key */_externalRank, &comm_within_node);
	MPI_Comm_rank(comm_within_node, &_indexThisPhysicalNode);
	#endif
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
	#ifdef HAVE_SESSIONS
		MPI_Comm_split(MPI_SESSION_WORLD_COMM, /* color */ _apprankNum, /* key */ internalRank, &INTRA_COMM);
	#else
		MPI_Comm_split(MPI_COMM_WORLD, /* color */ _apprankNum, /* key */ internalRank, &INTRA_COMM);
	#endif
}

MPIMessenger::MPIMessenger(int argc, char **argv) : Messenger(argc, argv)
{
	int support, ret;

#if HAVE_TAMPI
	// By default, TAMPI is initialized by the user's call to MPI_Init_thread.
	// It normally creates a task, which will periodically poll for completion
	// of its MPI requests. Right now, it is far too early for TAMPI to create
	// a task, as the runtime hasn't yet been fully initialized. Moreover,
	// TAMPI must be initialized within a task context, not at an arbitrary
	// point in the runtime outside a task. We set TAMPI_PROPERTY_AUTO_INIT to
	// false, and will call TAMPI_Init later when it is safe to do so, inside
	// the main or namespace task.
	ret = TAMPI_Property_set(TAMPI_PROPERTY_AUTO_INIT, 0);
	assert(ret == MPI_SUCCESS);
#else
	// Check whether the user linked the application with TAMPI. If so, Nanos6 has to be
	// built with TAMPI, but it isn't (HAVE_TAMPI is 0). Calling MPI_Init_thread would
	// initialize TAMPI and cause an obscure failure when it tries to use the Nanos6 API
	// before the runtime has fully initialized. Raise an error instead.
	void *symbol = dlsym(RTLD_DEFAULT, "TAMPI_Blocking_enabled");
	FatalErrorHandler::failIf(symbol != nullptr, "To use OmpSs-2@Cluster with TAMPI, build Nanos6 with --with-tampi=<prefix>");
#endif
	

	#ifdef HAVE_SESSIONS
		// Session based:
		//MPI_Session session;
		ret = MPI_Session_init(MPI_INFO_NULL, MPI_ERRORS_RETURN, &sessionHandle);
		//MPI_Group group = MPI_GROUP_NULL;
		ret = MPI_Group_from_session_pset(sessionHandle, "mpi://WORLD", &sessionWorldGroup);
		//MPI_Comm MPI_SESSION_WORLD_COMM = MPI_COMM_NULL;
		ret = MPI_Comm_create_from_group(sessionWorldGroup, "stringtag", MPI_INFO_NULL, MPI_ERRORS_RETURN, &MPI_SESSION_WORLD_COMM);
	#else
		// World model:
		ret = MPI_Init_thread(&_argc, &_argv, MPI_THREAD_MULTIPLE, &support);
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
		if (support != MPI_THREAD_MULTIPLE) {
			std::cerr << "Could not initialize multithreaded MPI" << std::endl;
			abort();
	}
	#endif
	int groupSize = 0;
	#ifdef HAVE_SESSIONS
		ret = MPI_Comm_size(MPI_SESSION_WORLD_COMM, &groupSize);
	#else
		ret = MPI_Comm_size(MPI_COMM_WORLD, &groupSize);
	#endif
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(groupSize > 0);

	// Set the error handler to MPI_ERRORS_RETURN so we can use a check latter.  The error handler
	// is inherited by any created messenger latter, so we don't need to set it again.
	// TODO: Instead of checking on every MPI call we must define a propper MPI error handler.  The
	// problem with this is that some parameters are implementation specific...
	/*ret = MPI_Comm_set_errhandler(MPI_SESSION_WORLD_COMM, MPI_ERRORS_RETURN);
	MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);*/

	//! Save the parent communicator
	ret = MPI_Comm_get_parent(&PARENT_COMM);
	#ifdef HAVE_SESSIONS
		MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
	#else
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	#endif
	ConfigVariable<std::string> clusterSplitEnv("cluster.hybrid.split");
	if (!clusterSplitEnv.isPresent()) {
		// When there is not parent this is part of the initial communicator.
		#ifdef HAVE_SESSIONS
			ret = MPI_Comm_dup(MPI_SESSION_WORLD_COMM, &INTRA_COMM);
			MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
		#else
			ret = MPI_Comm_dup(MPI_COMM_WORLD, &INTRA_COMM);
			MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
		#endif
		char commname[MPI_MAX_OBJECT_NAME];
		snprintf(commname, MPI_MAX_OBJECT_NAME - 1, "INTRA_WORLD_%s",
			(PARENT_COMM != MPI_COMM_NULL ? "spawned" : "nospawned"));

		ret = MPI_Comm_set_name(INTRA_COMM, commname);
		MPIErrorHandler::handle(ret, INTRA_COMM);
	}
	// The first element in the vector is information about the current INTRA_COMM before any merge.
	// WHen the process is not spawned then PARENT_COMM is MPI_COMM_NULL
	commInfo info = {.intraComm = INTRA_COMM, .interComm = PARENT_COMM, .groupSize = groupSize};
	_spawnedCommInfoStack.push(info);

	//! Create a new communicator
	if (PARENT_COMM == MPI_COMM_NULL) {

		//! Calculate number of nodes and node number
		getNodeNumber();
		
		//! Get external rank (in MPI_SESSION_WORLD_COMM)
		#ifdef HAVE_SESSIONS
			ret = MPI_Comm_size(MPI_SESSION_WORLD_COMM, &_numExternalRanks);
			ret = MPI_Comm_rank(MPI_SESSION_WORLD_COMM, &_externalRank);
		#else
			ret = MPI_Comm_size(MPI_COMM_WORLD, &_numExternalRanks);
			ret = MPI_Comm_rank(MPI_COMM_WORLD, &_externalRank);
		#endif
		// When there is not parent it is part of the initial communicator.
		if (clusterSplitEnv.isPresent() && clusterSplitEnv.getValue().length() > 0) {
			// Run in hybrid MPI + OmpSs-2@Cluster mode
			std::string clusterSplit = clusterSplitEnv.getValue();
			splitCommunicator(clusterSplit);
		} else {
			// Run in pure OmpSs-2@Cluster mode
			_apprankNum = 0;
			_numAppranks = 1;
			_numInstancesThisNode = calcNumInstancesThisNode();

			for (int i = 0; i < _numInstancesThisNode; ++i) {
				bool isMasterInstance = (i==0) && (_physicalNodeNum==0); /* first instance on first node */
				_isMasterThisNode.push_back(isMasterInstance);
			}
			_instrumentationRank = _externalRank;
		}
	} else {
		FatalErrorHandler::failIf(
			clusterSplitEnv.isPresent(),
			"Malleability doesn't work with hybrid MPI+OmpSs-2@Cluster"
		);

		// This is a spawned process.
		_apprankNum = 0;
		_numAppranks = 1;
		_numInstancesThisNode = 1; // Not used
		_instrumentationRank = _externalRank;

		// This is a spawned process, so merge with the parent communicator..
		ret = MPI_Intercomm_merge(PARENT_COMM, true,  &INTRA_COMM);
		#ifdef HAVE_SESSIONS
			MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
		#else
			MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
		#endif
	}

	//! Get the upper-bound tag supported by current MPI implementation
	int ubIsSetFlag = 0, *mpi_ub_tag = nullptr;
	ret = MPI_Comm_get_attr(INTRA_COMM, MPI_TAG_UB, &mpi_ub_tag, &ubIsSetFlag);
	#ifdef HAVE_SESSIONS
		MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
	#else
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	#endif
	assert(mpi_ub_tag != nullptr);
	assert(ubIsSetFlag != 0);

	_mpi_ub_tag = convertToBitMask(*mpi_ub_tag);
	assert(_mpi_ub_tag > 0);

	// Get the user config to use a different communicator for data_raw.
	ConfigVariable<bool> mpi_comm_data_raw("cluster.mpi.comm_data_raw");
	_mpi_comm_data_raw = mpi_comm_data_raw.getValue();

	if (_mpi_comm_data_raw) {
		ret = MPI_Comm_dup(INTRA_COMM, &INTRA_COMM_DATA_RAW);
		MPIErrorHandler::handle(ret, INTRA_COMM_DATA_RAW);
	} else {
		INTRA_COMM_DATA_RAW = INTRA_COMM;
	}

	ret = MPI_Comm_rank(INTRA_COMM, &_wrank);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(_wrank >= 0);

	ret = MPI_Comm_size(INTRA_COMM, &_wsize);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(_wsize > 0);

	// The parent class already has a copy of _argc and _argv, but as MPI spawn has extra
	// requirements, we use our local and keep the others untouched in case we implement other
	// messengers in the future
	EnvironmentVariable<std::string> envWrapper("NANOS6_WRAPPER");
	if (envWrapper.isPresent()) {
		spawnArgc = envWrapper.getValue();
		spawnArgv = argv;
	} else {
		spawnArgc = argv[0];
		spawnArgv = &argv[1];
	}

	this->shareDLBInfo();
}


void MPIMessenger::shutdown()
{
	int ret;

	if (_mpi_comm_data_raw ==  true) {
#ifndef NDEBUG
		int compare = 0;
		ret = MPI_Comm_compare(INTRA_COMM_DATA_RAW, INTRA_COMM, &compare);
		#ifdef HAVE_SESSIONS
			MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
		#else
			MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
		#endif
		assert(compare !=  MPI_IDENT);
#endif // NDEBUG

		//! Release the INTRA_COMM_DATA_RAW
		ret = MPI_Comm_free(&INTRA_COMM_DATA_RAW);
		#ifdef HAVE_SESSIONS
			MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
		#else
			MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
		#endif
	}

	//! Release the intra-communicator
	ret = MPI_Comm_free(&INTRA_COMM);
	#ifdef HAVE_SESSIONS
		MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
	#else
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	#endif

	
	//ret = MPI_Finalize();
	//ret = MPI_Session_finalize(&sessionHandle);
	ret = MPI_Finalize();
	#ifdef HAVE_SESSIONS
		MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
	#else
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	#endif

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

// Only needed in pure OmpSs-2@Cluster mode: check how many processes there are on
// the current node.
size_t MPIMessenger::calcNumInstancesThisNode()
{
	// Make a temporary communicator of all ranks with the same _physicalNodeNum
	MPI_Comm tempNodeComm;
	#ifdef HAVE_SESSIONS
	int ret = MPI_Comm_split(
		MPI_SESSION_WORLD_COMM,
		/* color */ _physicalNodeNum,
		/* key */ _externalRank,
		&tempNodeComm
	);
	#else
	int ret = MPI_Comm_split(
		MPI_COMM_WORLD,
		/* color */ _physicalNodeNum,
		/* key */ _externalRank,
		&tempNodeComm
	);
	#endif
	int numInstancesThisNode = 0;
	ret = MPI_Comm_size(tempNodeComm, &numInstancesThisNode);
	ret = MPI_Comm_free(&tempNodeComm);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	return numInstancesThisNode;
}

void MPIMessenger::shareDLBInfo()
{
	// Note: If the All gather pattern continues, then maybe it worth considering to use
	// MPI_Iallgather with a waitall at the end of all to create a single synchronization point.

	// Create the application communicator (Hybrid MPI+OmpSs applications)
	#ifdef HAVE_SESSIONS
		int ret = MPI_Comm_split(MPI_SESSION_WORLD_COMM, _wrank, _apprankNum, &APP_COMM);
	#else
		int ret = MPI_Comm_split(MPI_COMM_WORLD, _wrank, _apprankNum, &APP_COMM);
	#endif
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
	#ifdef HAVE_SESSIONS
	ret = MPI_Comm_split(
		MPI_SESSION_WORLD_COMM,
		/* color */ _physicalNodeNum,
		/* key */ _indexThisPhysicalNode,
		&tempNodeComm
	);
	#else
	ret = MPI_Comm_split(
		MPI_COMM_WORLD,
		/* color */ _physicalNodeNum,
		/* key */ _indexThisPhysicalNode,
		&tempNodeComm
	);
	#endif
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
		disableTAMPITaskAwareness();
		ret = MPI_Send((void *)delv, msgSize, MPI_BYTE, mpiDst, tag, INTRA_COMM);
		enableTAMPITaskAwareness();
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
				disableTAMPITaskAwareness();
				ret = MPI_Send(currAddress, currSize, MPI_BYTE, mpiDst, tag, INTRA_COMM_DATA_RAW);
				enableTAMPITaskAwareness();
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

	assert(mpiSrc < _wsize);
	assert(mpiSrc != _wrank);

	if (block) {
		Instrument::MPILock();
		forEachDataPart(
			address,
			size,
			messageId,
			[&](void *currAddress, size_t currSize, int currMessageId) {
				int tag = getTag(currMessageId);
				disableTAMPITaskAwareness();
				ret = MPI_Recv(currAddress, currSize, MPI_BYTE, mpiSrc, tag, INTRA_COMM_DATA_RAW, MPI_STATUS_IGNORE);
				enableTAMPITaskAwareness();
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
	disableTAMPITaskAwareness();
	#ifdef HAVE_SESSIONS
		int ret = MPI_Barrier(MPI_SESSION_WORLD_COMM);
		enableTAMPITaskAwareness();
		MPIErrorHandler::handle(ret, MPI_SESSION_WORLD_COMM);
	#else
		int ret = MPI_Barrier(MPI_COMM_WORLD);
		enableTAMPITaskAwareness();
		MPIErrorHandler::handle(ret, MPI_COMM_WORLD);
	#endif
}

void MPIMessenger::synchronizeAll(void)
{
	Instrument::MPILock();
	disableTAMPITaskAwareness();
	int ret = MPI_Barrier(INTRA_COMM);
	enableTAMPITaskAwareness();
	Instrument::MPIUnLock();
	MPIErrorHandler::handle(ret, INTRA_COMM);
}

void MPIMessenger::abortAll(int errcode)
{
	// TODO: This is a partial solution to reduce the real issue. If this failure happens in a
	// remote process (not master) then the job is not killed because only master knows about the
	// allocations and master will die with MPI_Abort. 
	if (SlurmAPI::isEnabled()) {
		SlurmAPI::killJobRequestIfPending();
	}

	std::lock_guard<SpinLock> guard(_abortLock);
	MPI_Abort(INTRA_COMM, errcode);
}


Message *MPIMessenger::checkMail(void)
{
	int ret, flag, count;
	MPI_Status status;

	Instrument::MPILock();
	ret = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, INTRA_COMM, &flag, &status);
	Instrument::MPIUnLock();
	MPIErrorHandler::handle(ret, INTRA_COMM, "Pooling MPI_Iprobe");

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
	disableTAMPITaskAwareness();
	ret = MPI_Recv((void *)dlv, count, MPI_BYTE, status.MPI_SOURCE,
		status.MPI_TAG, INTRA_COMM, MPI_STATUS_IGNORE);
	enableTAMPITaskAwareness();
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

void MPIMessenger::messengerReinitialize(bool willdie)
{
	int ret;
	//! make sure the new communicator returns errors
	if (_mpi_comm_data_raw) {
		MPI_Comm_free(&INTRA_COMM_DATA_RAW);
		ret = MPI_Comm_dup(INTRA_COMM, &INTRA_COMM_DATA_RAW);
		MPIErrorHandler::handle(ret, INTRA_COMM);
	} else {
		INTRA_COMM_DATA_RAW = INTRA_COMM;
	}

	if (willdie) {
		return;
	}

	int newrank = -1, newsize = -1;;
	ret = MPI_Comm_rank(INTRA_COMM, &newrank);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	assert(newrank == _wrank);

	ret = MPI_Comm_size(INTRA_COMM, &newsize);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	_wsize = newsize;
}

int MPIMessenger::messengerSpawn(int delta, std::string hostname)
{
	assert(delta > 0);
	MPI_Comm newinter = MPI_COMM_NULL;               // Temporal intercomm
	MPI_Comm newintra = MPI_COMM_NULL;               // Temporal intracomm

	int ret = 0;

	MPI_Info info = MPI_INFO_NULL;
	if (!hostname.empty() && _wrank == 0) {
		ret = MPI_Info_create(&info);
		MPIErrorHandler::handle(ret, INTRA_COMM);

		ret = MPI_Info_set(info, "host", hostname.c_str());
		MPIErrorHandler::handle(ret, INTRA_COMM);
	}

	int errcode = 0;
	ret = MPI_Comm_spawn(
		spawnArgc.c_str(), spawnArgv, delta, info, 0, INTRA_COMM, &newinter, &errcode
	);
	MPIErrorHandler::handle(ret, INTRA_COMM);
	FatalErrorHandler::failIf(errcode != MPI_SUCCESS, "New process returned error code: ", errcode);

	ret = MPI_Intercomm_merge(newinter, false, &newintra); // Create new intra
	MPIErrorHandler::handle(ret, INTRA_COMM);

	commInfo comm_info = {.intraComm = INTRA_COMM, .interComm = newinter, .groupSize = delta};
	_spawnedCommInfoStack.push(comm_info);

	INTRA_COMM = newintra;

	// This is commented to go around the Intel issue with MPI_Comm_spawn; it seems like intel frees
	// the info internal content in the MPI_Comm_spawn; somehow the info cannot be used after the
	// spawn either to access or to release.
	// if (info != MPI_INFO_NULL) {
	// 	ret = MPI_Info_free(&info);
	// 	MPIErrorHandler::handle(ret, INTRA_COMM);
	// }

	__attribute__((unused)) const int oldsize = _wsize;
	messengerReinitialize(false);
	assert(_wsize == oldsize + delta);                       // New size needs to be bigger

	char commname[MPI_MAX_OBJECT_NAME];
	snprintf(commname, MPI_MAX_OBJECT_NAME - 1, "INTRA_%d_%d", _wrank, _wsize);

	ret = MPI_Comm_set_name(INTRA_COMM, commname);
	MPIErrorHandler::handle(ret, INTRA_COMM);

	return _wsize;
}

int MPIMessenger::messengerShrink(int delta)
{
	assert(delta < 0);

	const int newsize = _wsize + delta;
	FatalErrorHandler::failIf(newsize < 1, "Can't resize below current number of nodes.");

	const int willdie = (_wrank >= newsize);
	FatalErrorHandler::failIf(willdie && !isSpawned(), "Can't shrink a non-spawned process.");

	int ret, currsize = _wsize;

	while (currsize > newsize && _spawnedCommInfoStack.size() > 0) {

#ifndef NDEBUG
		{   // Assert we don't have any pending message in the communicator.
			int flag;
			MPI_Status status;

			ret = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, INTRA_COMM, &flag, &status);
			MPIErrorHandler::handle(ret, INTRA_COMM, "Debug MPI_Iprobe");

			FatalErrorHandler::failIf(flag,
				"Pending messages in a communicator that will be removed: type ",
				MessageTypeStr[status.MPI_TAG & 0xff], " from ", status.MPI_SOURCE);
		}
#endif // NDEBUG
		struct commInfo &spawnedCommInfo = _spawnedCommInfoStack.top();

		FatalErrorHandler::failIf(_spawnedCommInfoStack.size() == 1
			&& spawnedCommInfo.interComm == MPI_COMM_NULL,
			"Can't shrink when there are no spawned processes.");

		FatalErrorHandler::failIf(currsize - newsize < spawnedCommInfo.groupSize,
			"Can't shrink incomplete communicator.");

		ret = MPI_Comm_free(&INTRA_COMM);                      // Free old intracomm before.
		INTRA_COMM = spawnedCommInfo.intraComm;                // Use the next in the stack
		MPIErrorHandler::handle(ret, INTRA_COMM);

		ret = MPI_Comm_disconnect(&spawnedCommInfo.interComm); // disconnect the inter communicator
		MPIErrorHandler::handle(ret, INTRA_COMM);

		currsize -= spawnedCommInfo.groupSize;

		// We can remove the entire group and continue iterating.
		_spawnedCommInfoStack.pop();
	}

	messengerReinitialize(willdie);

	if (willdie) {
		assert(_spawnedCommInfoStack.empty());
		return 0;
	}

	assert(newsize == _wsize);
	return _wsize;
}

void MPIMessenger::TAMPIInit()
{
#if HAVE_TAMPI
	ConfigVariable<bool> tampiEnabled("tampi.enabled");
	int requested = tampiEnabled ? MPI_TASK_MULTIPLE : MPI_THREAD_MULTIPLE;
	int support, ret;

	// Note: if TAMPI_Init gives the error "Intializing TAMPI before MPI is invalid"
	// or similar, it is likely due to the library linking order. TAMPI must be
	// linked before Extrae and the MPI library.
	//
	// If you LD_PRELOAD Extrae, then LD_PRELOAD both TAMPI and Extrae, with TAMPI first.
	// If the application is linked with MPI, then either also link it with TAMPI or
	// LD_PRELOAD TAMPI.

	ret = TAMPI_Init(requested, &support);
	if (ret != MPI_SUCCESS || support != requested) {
		std::cerr << "Could not initialize TAMPI" << std::endl;
		abort();
	}
	ClusterManager::synchronizeAll();
	_tampiInitialized = true;
#endif
}

void MPIMessenger::TAMPIFinalize()
{
#if HAVE_TAMPI
	__attribute__((unused)) int ret;
	assert(_tampiInitialized);
	ret = TAMPI_Finalize();
	assert(ret == MPI_SUCCESS);
#endif
}

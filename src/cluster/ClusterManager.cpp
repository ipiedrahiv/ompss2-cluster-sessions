/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include <vector>

#include "ClusterManager.hpp"
#include "ClusterHybridManager.hpp"
#include "ClusterMemoryManagement.hpp"
#include "messages/MessageSysFinish.hpp"
#include "messages/MessageDataFetch.hpp"
#include "messages/MessageDmalloc.hpp"

#include "messenger/Messenger.hpp"
#include "polling-services/ClusterServicesPolling.hpp"
#include "polling-services/ClusterServicesTask.hpp"
#include "system/RuntimeInfo.hpp"

#include <NodeNamespace.hpp>
#include <RemoteTasksInfoMap.hpp>
#include <OffloadedTaskId.hpp>
#include <OffloadedTasksInfoMap.hpp>
#include <ClusterNode.hpp>
#include "ClusterUtil.hpp"
#include "WriteID.hpp"
#include "MessageId.hpp"
#include "tasks/Task.hpp"

#include "executors/workflow/cluster/ExecutionWorkflowCluster.hpp"
#include "executors/threads/WorkerThread.hpp"

#include "executors/threads/CPUManager.hpp"

#include "SlurmAPI.hpp"

TaskOffloading::RemoteTasksInfoMap *TaskOffloading::RemoteTasksInfoMap::_singleton = nullptr;
TaskOffloading::OffloadedTasksInfoMap *TaskOffloading::OffloadedTasksInfoMap::_singleton = nullptr;
ClusterManager *ClusterManager::_singleton = nullptr;

MessageId *MessageId::_singleton = nullptr;
WriteIDManager *WriteIDManager::_singleton = nullptr;
OffloadedTaskIdManager *OffloadedTaskIdManager::_singleton = nullptr;

std::atomic<size_t> ClusterServicesPolling::_activeClusterPollingServices(0);
std::atomic<size_t> ClusterServicesTask::_activeClusterTaskServices(0);

ClusterManager::ClusterManager()
	: _clusterRequested(false),
	_clusterNodes(1),
	_thisNode(new ClusterNode(0, 0, 0, false, 0)),
	_masterNode(_thisNode),
	_msn(nullptr)
{
	assert(_singleton == nullptr);
	_clusterNodes[0] = _thisNode;
	MessageId::initialize(0, 1);
	WriteIDManager::initialize(0,1);
	OffloadedTaskIdManager::initialize(0,1);

	_dataInit._numMinNodes = 1;
	_dataInit._numMaxNodes = 1;
}

ClusterManager::ClusterManager(std::string const &commType, int argc, char **argv)
	: _clusterRequested(true),
	_msn(GenericFactory<std::string,Messenger*,int,char**>::getInstance().create(commType, argc, argv))
{
	assert(_msn != nullptr);
	TaskOffloading::RemoteTasksInfoMap::init();
	TaskOffloading::OffloadedTasksInfoMap::init();

	const size_t clusterSize = _msn->getClusterSize();
	const int apprankNum = _msn->getApprankNum();
	const int externalRank = _msn->getExternalRank();
	const int internalRank = _msn->getNodeIndex();  /* internal rank */
	const int physicalNodeNum = _msn->getPhysicalNodeNum();
	const int indexThisPhysicalNode = _msn->getIndexThisPhysicalNode();
	const int masterIndex = _msn->getMasterIndex();

	const int numAppranks = _msn->getNumAppranks();
	const bool inHybridMode = numAppranks > 1;

	// Initialize the DLB stuff
	const std::vector<int> &internalRankToExternalRank = _msn->getInternalRankToExternalRank();
	const std::vector<int> &instanceThisNodeToExternalRank = _msn->getInstanceThisNodeToExternalRank();

	ClusterHybridManager::preinitialize(
		inHybridMode, externalRank, apprankNum, internalRank, physicalNodeNum, indexThisPhysicalNode,
		clusterSize, internalRankToExternalRank, instanceThisNodeToExternalRank
	);

	// Called from constructor the first time
	this->_clusterNodes.resize(clusterSize);

	for (size_t i = 0; i < clusterSize; ++i) {
		_clusterNodes[i] = new ClusterNode(i, i, apprankNum, inHybridMode, _msn->internalRankToInstrumentationRank(i));
	}

	_thisNode = _clusterNodes[internalRank];
	_masterNode = _clusterNodes[masterIndex];

	assert(_thisNode != nullptr);
	assert(_masterNode != nullptr);
	assert(_thisNode->getCommIndex() == internalRank);

	ConfigVariable<bool> disableRemote("cluster.disable_remote");
	_disableRemote = disableRemote.getValue();

	ConfigVariable<bool> disableRemoteConnect("cluster.disable_remote_connect");
	_disableRemoteConnect = disableRemoteConnect.getValue();

	ConfigVariable<bool> disableAutowait("cluster.disable_autowait");
	_disableAutowait = disableAutowait.getValue();

	ConfigVariable<bool> eagerWeakFetch("cluster.eager_weak_fetch");
	_eagerWeakFetch = eagerWeakFetch.getValue();

	ConfigVariable<bool> mergeReleaseAndFinish("cluster.merge_release_and_finish");
	_mergeReleaseAndFinish = mergeReleaseAndFinish.getValue();

	ConfigVariable<bool> autoOptimizeNonAccessed("cluster.auto.optimize_nonaccessed");
	_autoOptimizeNonAccessed = autoOptimizeNonAccessed.getValue();

	ConfigVariable<bool> autoOptimizeReadOnly("cluster.auto.optimize_readonly");
	_autoOptimizeReadOnly = autoOptimizeReadOnly.getValue();

	ConfigVariable<int> numMessageHandlerWorkers("cluster.num_message_handler_workers");
	_numMessageHandlerWorkers = numMessageHandlerWorkers.getValue();

	ConfigVariable<size_t> numMaxNodes("cluster.num_max_nodes");
	_numMaxNodes = (numMaxNodes.getValue() > clusterSize ? numMaxNodes.getValue() : clusterSize);

	ConfigVariable<bool> groupMessages("cluster.group_messages");
	_groupMessages = groupMessages.getValue();

	MessageId::initialize(internalRank, _numMaxNodes);
	WriteIDManager::initialize(internalRank, _numMaxNodes);
	OffloadedTaskIdManager::initialize(internalRank, _numMaxNodes);
}

ClusterManager::~ClusterManager()
{
	OffloadedTaskIdManager::finalize();
	WriteIDManager::finalize();
	MessageId::finalize();

#if HAVE_SLURM
	if (SlurmAPI::isEnabled()) {
		assert(ClusterManager::isMasterNode());
		assert(!ClusterManager::isSpawned());
		assert(ClusterManager::getInitData().clusterMalleabilityEnabled());
		SlurmAPI::finalize();
	}
#endif // HAVE_SLURM

	for (auto &node : _clusterNodes) {
		delete node;
	}
	_clusterNodes.clear();

	delete _msn;
	_msn = nullptr;
}

// Static
void ClusterManager::initClusterNamespace(void (*func)(void *), void *args)
{
	assert(_singleton != nullptr);
	NodeNamespace::init(func, args);
}


// Cluster is initialized before the memory allocator.
void ClusterManager::initialize(int argc, char **argv)
{
	assert(_singleton == nullptr);
	ConfigVariable<std::string> commType("cluster.communication");

	RuntimeInfo::addEntry("cluster_communication", "Cluster Communication Implementation", commType);

	if (commType.getValue() == "disabled") {
		_singleton = new ClusterManager();
		assert(_singleton != nullptr);
	} else {
		/** If a communicator has not been specified through the cluster.communication config
		 * variable we will not initialize the cluster support of Nanos6 */
		assert(argc > 0);
		assert(argv != nullptr);
		_singleton = new ClusterManager(commType.getValue(), argc, argv);
		assert(_singleton != nullptr);

		_singleton->initializeMalleabilityVars();
		FatalErrorHandler::failIf(
			ClusterManager::isSpawned(), "Can spawn process without malleability support."
		);
	}
}

// This needs to be called AFTER initializing the memory allocator
void ClusterManager::postinitialize()
{
	assert(_singleton != nullptr);
	assert(MemoryAllocator::isInitialized());

	/* For (verbose) instrumentation, summarize the splitting of external ranks
	 * into appranks and instances. Always do this, even if in non-cluster mode,
	 * as useful for the "per-node" instrumentation of DLB (using num_cores).
	 */
	if (_singleton->_msn != nullptr) {
		_singleton->_msn->summarizeSplit();
	}

	int allocCores = getCurrentClusterNode()->getCurrentAllocCores();
	Instrument::emitClusterEvent(Instrument::ClusterEventType::AllocCores, allocCores);
	Instrument::emitClusterEvent(Instrument::ClusterEventType::OwnedCPUs, ClusterHybridManager::getCurrentOwnedCPUs());

	/*
	 * Synchronization before starting polling services. This is needed only for the hybrid
	 * polling service. We do not want the hybrid polling service to take free cores that
	 * have not yet been claimed by their owner at startup, which would cause an error from
	 * DLB. This synchronizes MPI_COMM_WORLD, but it would be sufficient to synchronize only
	 * among the instances on the same node.
	 */
	if (_singleton->_msn) {
#if HAVE_TAMPI
		// TAMPI is initialized either before running main (on rank 0) or at the start of the
		// namespace task (on the other ranks). In either case this proceeds concurrently with
		// postinitialize. Initialization of TAMPI has a barrier to ensure a well-defined point
		// at which TAMPI has been enabled, that is known to be before main starts executing. We
		// need to wait for this before doing the below barrier, otherwise the two barriers will
		// get mixed up.
		while(!TAMPIInitialized()) {
			usleep(1000);
		}
#endif
		_singleton->_msn->synchronizeWorld();
	}

	if (ClusterManager::inClusterMode()) {
		ClusterServicesPolling::initialize();
		ClusterServicesTask::initializeWorkers(_singleton->_numMessageHandlerWorkers);
	} else {
#if HAVE_DLB
		/* Enable polling services for LeWI + DROM integration even if not in clusters mode.
		 * Ideally DROM support could be disconnected from the cluster support as it may
		 * be useful among processes on the same node, even without clusters.
		 */
		ClusterServicesPolling::initialize(/* hybridOnly */ true);
#endif
	}
}

void ClusterManager::shutdownPhase1()
{
	assert(NodeNamespace::isEnabled() || ClusterManager::getMessenger() == nullptr);
	assert(_singleton != nullptr);
	assert(MemoryAllocator::isInitialized());

	if (ClusterManager::getMessenger() != nullptr && ClusterManager::isMasterNode()) {
		assert(!ClusterManager::isSpawned());
		MessageSysFinish msg;
		ClusterManager::sendMessageToAll(&msg, true);

		// Master needs to do the same than others
		msg.handleMessage();
	}

	if (ClusterManager::inClusterMode()) {
		if (ClusterServicesPolling::count() > 0) {
			ClusterServicesPolling::shutdown();
		}

		if (ClusterServicesTask::count() > 0) {
			ClusterServicesTask::shutdownWorkers(_singleton->_numMessageHandlerWorkers);
		}

		TaskOffloading::RemoteTasksInfoMap::shutdown();
		TaskOffloading::OffloadedTasksInfoMap::shutdown();
	} else {
#if HAVE_DLB
		ClusterServicesPolling::shutdown(/* hybridOnly */ true);
#endif
		assert(ClusterServicesPolling::count() == 0);
		assert(ClusterServicesTask::count() == 0);
	}

	if (_singleton->_msn != nullptr) {
		// Finalize MPI BEFORE the instrumentation because the extrae finalization accesses to some
		// data structures throw extrae_nanos6_get_thread_id when finalizing MPI.
		_singleton->_msn->shutdown();
	}
}

void ClusterManager::shutdownPhase2()
{
	if (ClusterManager::getMessenger() != nullptr) {
		// To avoid some issues with the instrumentation shutdown this must be called after
		// finalizing the instrumentation. The extrae instrumentation accesses to the
		// taskInfo->implementations[0] during finalization so if the taskinfo is deleted the access
		// may be corrupt.
		NodeNamespace::deallocate();
	}

	assert(!NodeNamespace::isEnabled());
	assert(_singleton != nullptr);

	delete _singleton;
	_singleton = nullptr;
}

void ClusterManager::fetchVector(
	size_t nFragments,
	std::vector<ExecutionWorkflow::ClusterDataCopyStep *> const &copySteps,
	MemoryPlace const *from
) {
	assert(_singleton->_msn != nullptr);
	assert(from != nullptr);
	assert(from->getType() == nanos6_cluster_device);
	assert((size_t)from->getIndex() < _singleton->_clusterNodes.size());

	ClusterNode const *remoteNode = getClusterNode(from->getIndex());

	assert(remoteNode != _singleton->_thisNode);

	//! At the moment we do not translate addresses on remote
	//! nodes, so the region we are fetching, on the remote node is
	//! the same as the local one
	MessageDataFetch *msg = new MessageDataFetch(nFragments, copySteps);

	__attribute__((unused)) MessageDataFetch::DataFetchMessageContent *content = msg->getContent();

	size_t index = 0;

	std::vector<DataTransfer *> temporal(nFragments, nullptr);

	for (ExecutionWorkflow::ClusterDataCopyStep const *step : copySteps) {

		const std::vector<ExecutionWorkflow::FragmentInfo> &fragments = step->getFragments();

		for (ExecutionWorkflow::FragmentInfo const &fragment : fragments) {
			assert(index < nFragments);
			assert(content->_remoteRegionInfo[index]._remoteRegion == fragment._region);
			temporal[index] = fragment._dataTransfer;
			++index;
		}
	}

	assert(index == nFragments);
	ClusterPollingServices::PendingQueue<DataTransfer>::addPendingVector(temporal);
	ClusterManager::sendMessage(msg, remoteNode);
}

int ClusterManager::nanos6GetInfo(nanos6_cluster_info_t *info)
{
	assert(_singleton != nullptr);
	assert(info != nullptr);

	info->spawn_policy = _singleton->_dataInit.defaultSpawnPolicy;
	info->transfer_policy = _singleton->_dataInit.defaultShrinkTransferPolicy;
	info->cluster_num_min_nodes = _singleton->_dataInit._numMinNodes;
	info->cluster_num_max_nodes = _singleton->_dataInit._numMaxNodes;
	info->malleability_enabled = _singleton->_dataInit.clusterMalleabilityEnabled();

	info->cluster_num_nodes = (unsigned long) ClusterManager::clusterSize();
	info->num_message_handler_workers = ClusterManager::getNumMessageHandlerWorkers();
	info->namespace_enabled = !ClusterManager::getDisableRemote();
	info->disable_remote_connect = ClusterManager::getDisableRemoteConnect();
	info->disable_autowait = ClusterManager::getDisableAutowait();
	info->eager_weak_fetch = ClusterManager::getEagerWeakFetch();
	info->eager_send = false;
	info->merge_release_and_finish = ClusterManager::getMergeReleaseAndFinish();
	info->reserved_leader_thread = CPUManager::hasReservedCPUforLeaderThread();
	info->group_messages_enabled = ClusterManager::getGroupMessagesEnabled();
	info->write_id_enabled = WriteIDManager::isEnabled();

	info->virtual_region_start = _singleton->_dataInit._virtualAllocation.getStartAddress();
	info->virtual_region_size = _singleton->_dataInit._virtualAllocation.getSize();

	info->distributed_region_start = _singleton->_dataInit._virtualDistributedRegion.getStartAddress();
	info->distributed_region_size = _singleton->_dataInit._virtualDistributedRegion.getSize();

	return 0;
}

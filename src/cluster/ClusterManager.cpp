/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include "ClusterManager.hpp"
#include "ClusterHybridManager.hpp"
#include "messages/MessageSysFinish.hpp"
#include "messages/MessageDataFetch.hpp"
#include "messages/MessageResize.hpp"
#include "messages/DataInitSpawn.hpp"

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

#include "system/ompss/TaskWait.hpp"

#include "executors/workflow/cluster/ExecutionWorkflowCluster.hpp"
#include "executors/threads/WorkerThread.hpp"

#if HAVE_SLURM
#include "SlurmAPI.hpp"
#endif // HAVE_SLURM

TaskOffloading::RemoteTasksInfoMap *TaskOffloading::RemoteTasksInfoMap::_singleton = nullptr;
TaskOffloading::OffloadedTasksInfoMap *TaskOffloading::OffloadedTasksInfoMap::_singleton = nullptr;
ClusterManager *ClusterManager::_singleton = nullptr;

MessageId *MessageId::_singleton = nullptr;
WriteIDManager *WriteIDManager::_singleton = nullptr;
OffloadedTaskIdManager *OffloadedTaskIdManager::_singleton = nullptr;

std::atomic<size_t> ClusterServicesPolling::_activeClusterPollingServices(0);
std::atomic<bool> ClusterServicesPolling::_pausedServices(false);

std::atomic<size_t> ClusterServicesTask::_activeClusterTaskServices(0);
std::atomic<bool> ClusterServicesTask::_pausedServices(false);

ClusterManager::ClusterManager()
	: _clusterRequested(false),
	_clusterNodes(1), _hostnames(), _numMinNodes(-1), _numMaxNodes(-1),
	_thisNode(new ClusterNode(0, 0, 0, false, 0)),
	_masterNode(_thisNode),
	_msn(nullptr),
	_disableRemote(false), _disableRemoteConnect(false), _disableAutowait(false),
	_dataInit(nullptr)
{
	assert(_singleton == nullptr);
	_clusterNodes[0] = _thisNode;
	MessageId::initialize(0, 1);
	WriteIDManager::initialize(0,1);
	OffloadedTaskIdManager::initialize(0,1);
}

ClusterManager::ClusterManager(std::string const &commType, int argc, char **argv)
	: _clusterRequested(true), _hostnames(), _numMinNodes(-1), _numMaxNodes(-1),
	_msn(GenericFactory<std::string,Messenger*,int,char**>::getInstance().create(commType, argc, argv)),
	_disableRemote(false), _disableRemoteConnect(false), _disableAutowait(false),
	_dataInit(nullptr)
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

	MessageId::initialize(internalRank, clusterSize);
	WriteIDManager::initialize(internalRank, clusterSize);
	OffloadedTaskIdManager::initialize(internalRank, clusterSize);

	const int numAppranks = _msn->getNumAppranks();
	const bool inHybridMode = numAppranks > 1;

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

	ConfigVariable<bool> disableRemote("cluster.disable_remote");
	_disableRemote = disableRemote.getValue();

	ConfigVariable<bool> disableRemoteConnect("cluster.disable_remote_connect");
	_disableRemoteConnect = disableRemoteConnect.getValue();

	ConfigVariable<bool> disableAutowait("cluster.disable_autowait");
	_disableAutowait = disableAutowait.getValue();

	ConfigVariable<bool> eagerWeakFetch("cluster.eager_weak_fetch");
	_eagerWeakFetch = eagerWeakFetch.getValue();

	ConfigVariable<bool> eagerSend("cluster.eager_send");
	_eagerSend = eagerSend.getValue();

	ConfigVariable<bool> mergeReleaseAndFinish("cluster.merge_release_and_finish");
	_mergeReleaseAndFinish = mergeReleaseAndFinish.getValue();

	ConfigVariable<bool> autoOptimizeNonAccessed("cluster.auto.optimize_nonaccessed");
	_autoOptimizeNonAccessed = autoOptimizeNonAccessed.getValue();

	ConfigVariable<bool> autoOptimizeReadOnly("cluster.auto.optimize_readonly");
	_autoOptimizeReadOnly = autoOptimizeReadOnly.getValue();

	ConfigVariable<int> numMessageHandlerWorkers("cluster.num_message_handler_workers");
	_numMessageHandlerWorkers = numMessageHandlerWorkers.getValue();
}

ClusterManager::~ClusterManager()
{
	OffloadedTaskIdManager::finalize();
	WriteIDManager::finalize();
	MessageId::finalize();

	for (auto &node : _clusterNodes) {
		delete node;
	}
	_clusterNodes.clear();

	delete _msn;
	_msn = nullptr;

	if (_dataInit != nullptr) {
		free(_dataInit);
	}
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

	/** If a communicator has not been specified through the
	 * cluster.communication config variable we will not
	 * initialize the cluster support of Nanos6 */
	if (commType.getValue() != "disabled") {
		assert(argc > 0);
		assert(argv != nullptr);
		_singleton = new ClusterManager(commType.getValue(), argc, argv);
		assert(_singleton != nullptr);

#if HAVE_SLURM
		ConfigVariable<int> numMaxNodes("cluster.num_max_nodes");
		_singleton->_numMaxNodes = numMaxNodes.getValue();

		if (clusterMalleableMaxSize() != 0 && isMasterNode()) {
			assert(_singleton->_msn->isSpawned() == false); // Master node can't be spawned

			_singleton->_hostnames = clusterHostManager::getNodeList();

			FatalErrorHandler::failIf(
				_singleton->_hostnames.empty(),
				"Hostnames list is empty. Check the SlurmAPI.");

			FatalErrorHandler::warnIf(
				_singleton->_hostnames.size() > (size_t)clusterMalleableMaxSize(),
				"There are more nodes than num_max_nodes setting.");
		}
#endif // HAVE_SLURM

		if (_singleton->_msn->isSpawned()) {
			// We could use an if-else but this way it is more defensive to detect errors in other places.
			assert(!isMasterNode());
			ClusterManager::synchronizeAll();

			_singleton->_dataInit = (DataInitSpawn *) malloc(sizeof(DataInitSpawn));
			assert(_singleton->_dataInit != nullptr);

			DataAccessRegion region(_singleton->_dataInit, sizeof(DataInitSpawn));
			fetchDataRaw(region, getMasterNode()->getMemoryNode(), 1, true, false);
		}

	} else {
		_singleton = new ClusterManager();
		assert(_singleton != nullptr);
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

	if (inClusterMode()) {
		ClusterServicesPolling::waitUntilFinished();
	}

	if (ClusterManager::getMessenger() != nullptr && ClusterManager::isMasterNode()) {
		MessageSysFinish msg;
		ClusterManager::sendMessageToAll(&msg, true);

		// Master needs to do the same than others
		msg.handleMessage();
	}

	if (ClusterManager::inClusterMode()) {
		ClusterServicesPolling::shutdown();

		ClusterServicesTask::shutdownWorkers(_singleton->_numMessageHandlerWorkers);

		TaskOffloading::RemoteTasksInfoMap::shutdown();
		TaskOffloading::OffloadedTasksInfoMap::shutdown();
	} else {
#if HAVE_DLB
		ClusterServicesPolling::shutdown(/* hybridOnly */ true);
#endif
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

		for (__attribute__((unused)) ExecutionWorkflow::FragmentInfo const &fragment : fragments) {
			assert(index < nFragments);
			assert(content->_remoteRegionInfo[index]._remoteRegion == fragment._region);
			temporal[index] = fragment._dataTransfer;

			++index;
		}
	}

	assert(index == nFragments);

	ClusterPollingServices::PendingQueue<DataTransfer>::addPendingVector(temporal);

	_singleton->_msn->sendMessage(msg, remoteNode);
}

int ClusterManager::nanos6Spawn(const MessageResize *msg_spawn)
{
	assert(_singleton != nullptr);
	assert(_singleton->_msn != nullptr);

	const int delta = msg_spawn->getDelta();
	const std::string hostname = msg_spawn->getHostname();

	assert(delta > 0);
	const __attribute__((unused)) int oldIndex = _singleton->_msn->getNodeIndex();
	const int oldSize = ClusterManager::clusterSize();

	// Stop polling services.
	if (oldSize > 1) {
		ClusterServicesTask::setPauseStatus(true);
		ClusterServicesPolling::setPauseStatus(true);
	}
	// messenger calls spawn and merge
	const int newSize = _singleton->_msn->nanos6Spawn(delta, hostname);
	assert(newSize == oldSize + delta);
	const int newIndex = _singleton->_msn->getNodeIndex();
	assert(newIndex == oldIndex);

	// Register the new nodes and their memory
	for (int i = oldSize; i < newSize; ++i) {
		ClusterNode *node = new ClusterNode(i, i, 0, false, i);
		_singleton->_clusterNodes.push_back(node);
		VirtualMemoryManagement::registerNodeLocalRegion(node);
	}
	assert(ClusterManager::clusterSize() == newSize);

	MessageId::reset(newIndex, newSize);
	WriteIDManager::reset(newIndex, newSize);
	OffloadedTaskIdManager::reset(newIndex, newSize);

	ClusterManager::synchronizeAll();

	if (oldSize == 1) { // When we started with a single node polling services didn't start.
		ClusterServicesPolling::initialize();
		ClusterServicesTask::initializeWorkers(_singleton->_numMessageHandlerWorkers);
	} else {            // Restart the polling services
		assert(oldSize > 1);
		ClusterServicesPolling::setPauseStatus(false);
		ClusterServicesTask::setPauseStatus(false);
	}

	return newSize;
}


int ClusterManager::nanos6Resize(int delta)
{
	assert(delta != 0);
	assert(ClusterManager::isMasterNode());
	assert(_singleton != nullptr);
	assert(_singleton->_msn != nullptr);
	assert(_singleton->_numMaxNodes != -1); // Manager initialized
	// There is more than one host... This will be corrected with multiple processes/node
	assert(_singleton->_hostnames.size() > 1);

	TaskWait::taskWait("nanos6Resize");

	const size_t oldSize = ClusterManager::clusterSize();

	if (delta == 0) {
		return oldSize;
	}

	assert(oldSize == (size_t)_singleton->_msn->getClusterSize());

	FatalErrorHandler::failIf(oldSize < 1, "Old size can't be less than 1");
	FatalErrorHandler::failIf(_singleton->_numMaxNodes == 0,
		"Can't resize cluster, malleability is disabled (num_max_nodes == 0)");

	if (delta > 0) {
		assert(oldSize < _singleton->_hostnames.size());
		assert(oldSize < (size_t)_singleton->_numMaxNodes);

		// Master sends spawn messages to all the OLD world
		MessageResize msg_spawn(delta, _singleton->_hostnames[oldSize]);

		ClusterManager::sendMessageToAll(&msg_spawn, true);

		// this is the same call that message handler does. So any improvement in resize will be
		// done in nanos6Spawn not here because that will be executed by all the processes.
		const int newSize = ClusterManager::nanos6Spawn(&msg_spawn);
		assert((size_t)newSize == oldSize + delta);

		// Then master sends the init message to the new processes.
		DataInitSpawn data_init;
		DataAccessRegion msg_region((void *)&data_init, sizeof(DataInitSpawn));
		for (int i = oldSize; i < newSize; ++i) {

			ClusterNode *target = ClusterManager::getClusterNode(i);
			assert(target != nullptr);
			assert(target->getMemoryNode() != nullptr);

			ClusterManager::sendDataRaw(msg_region, target->getMemoryNode(), 1, true);
		}

		return newSize;
	} else if (delta < 0) {
		assert(oldSize < (size_t)_singleton->_numMinNodes);
	}

	return -1;
}

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
#include "messages/MessageResize.hpp"

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

#include "dependencies/linear-regions-fragmented/TaskDataAccesses.hpp"

#include "system/ompss/TaskWait.hpp"

#include "executors/workflow/cluster/ExecutionWorkflowCluster.hpp"
#include "executors/threads/WorkerThread.hpp"

#if HAVE_SLURM
#include "SlurmAPI.hpp"
SlurmAPI *SlurmAPI::_singleton = nullptr;
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

#if HAVE_SLURM
	if (_msn->isSpawned()) {
		// We could use an if-else but this way it is more defensive to detect errors in other places.
		assert(_thisNode != _masterNode);
		_msn->synchronizeAll();

		DataAccessRegion region(&_dataInit, sizeof(DataInitSpawn));
		_msn->fetchData(region, _masterNode, 1, true, false);
	} else {
		_dataInit._numMinNodes = clusterSize;
		_dataInit._numMaxNodes = clusterSize;

		ConfigVariable<int> numMaxNodes("cluster.num_max_nodes");
		if (numMaxNodes.getValue() != 0) {
			_dataInit._numMaxNodes = numMaxNodes.getValue();
		}

		if (_thisNode == _masterNode && _dataInit.clusterMalleabilityEnabled()) {
			SlurmAPI::initialize();
		}
	}
#else // HAVE_SLURM
	FatalErrorHandler::failIf(_msn->isSpawned(), "Can spawn process without malleability support.")
#endif // HAVE_SLURM
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

	/** If a communicator has not been specified through the
	 * cluster.communication config variable we will not
	 * initialize the cluster support of Nanos6 */
	if (commType.getValue() != "disabled") {
		assert(argc > 0);
		assert(argv != nullptr);
		_singleton = new ClusterManager(commType.getValue(), argc, argv);
		assert(_singleton != nullptr);
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
	assert(NodeNamespace::isEnabled());
	assert(MemoryAllocator::isInitialized());

	if (ClusterManager::inClusterMode()) {
		ClusterServicesPolling::waitUntilFinished();
	}

	if (ClusterManager::getMessenger() != nullptr && ClusterManager::isMasterNode()) {
		assert(!ClusterManager::isSpawned());
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

int ClusterManager::nanos6Spawn(const MessageSpawn *msg_spawn)
{
	assert(_singleton != nullptr);
	assert(_singleton->_msn != nullptr);

	const __attribute__((unused)) int oldIndex = _singleton->_msn->getNodeIndex();
	const int oldSize = ClusterManager::clusterSize();
	int newSize = oldSize;

	// Stop polling services.
	if (oldSize > 1) {
		ClusterServicesTask::setPauseStatus(true);
		ClusterServicesPolling::setPauseStatus(true);
	}

	const int delta = msg_spawn->getDeltaNodes();
	assert(delta > 0);
	const size_t nEntries = msg_spawn->getNEntries();

	int spawned = 0;
	for (size_t ent = 0; ent < nEntries; ++ent) {
		// Spawn entries are basically the following spawn steps. As we send a single message with
		// all the spawn steps to perform. Apart form that the new processes will be informed to
		// spawn also in case they are created as intermediate processes.  This method is the only
		// one fully compatible with all the mpi implementations and with the efficiency of
		// performing everything with a minimal number of messages.
		const MessageSpawnHostInfo &info = msg_spawn->getEntries()[ent];

		const std::string hostname(info.hostname);
		const size_t nprocs = info.nprocs;
		const int lastSize = ClusterManager::clusterSize();
		assert(lastSize == oldSize + spawned);

		// messenger calls spawn and merge
		newSize = _singleton->_msn->messengerSpawn(delta, hostname);
		assert(_singleton->_msn->getNodeIndex() == oldIndex);

		// Register the new nodes and their memory
		for (int i = oldSize; i < newSize; ++i) {
			ClusterNode *node = new ClusterNode(i, i, 0, false, i);
			_singleton->_clusterNodes.push_back(node);
			VirtualMemoryManagement::registerNodeLocalRegion(node);
		}
		assert(ClusterManager::clusterSize() == newSize);
		spawned += nprocs;
		ClusterManager::synchronizeAll();

		if (ClusterManager::isMasterNode()) {
			// Then master sends the init message to the new processes. We need to send a separate
			// message to inform the new processes that the spawn is not finished because the
			// ClusterManager::sendDataRaw and its counter part expect a fixed size message and the
			// polling services on the new nodes are not running yet either.
			MessageSpawn *msg_spawn_i = nullptr;

			// create a temporal message with spawn instructions from ent + 1 -> nEntries for the
			// new processes created in this iteration only.
			const size_t nextEnt = ent + 1;
			const int pending = delta - spawned;
			assert(pending >= 0);         // We never spawn more processes than delta
			if (nextEnt < nEntries) {
				assert(pending > 0);      // We still have processes to spawn
				msg_spawn_i = new MessageSpawn(
					pending, nEntries - nextEnt, &msg_spawn->getEntries()[nextEnt]
				);
			}

			// TODO: All these messages are blocking; we may improve this code to make it async. The
			// eassiest way may be to modify the messages to have a reference counter and
			// ClusterPollingServices::PendingQueue<Message>::addPending to search in the pending
			// queue for this message.
			// We could also make the setMessengerData to have a list
			DataAccessRegion init_region((void *)&_singleton->_dataInit, sizeof(DataInitSpawn));

			for (int i = lastSize; i < newSize; ++i) {

				ClusterNode *target = ClusterManager::getClusterNode(i);
				assert(target != nullptr);
				assert(target->getMemoryNode() != nullptr);

				ClusterManager::sendDataRaw(init_region, target->getMemoryNode(), 1, true);

				// After the init we need to send the rest of the spawn to the new processes created
				// in this iteration, so we can continue to the next iteration.
				if (msg_spawn_i != nullptr) {
					ClusterManager::sendMessage(msg_spawn_i, target, true);
				}

			}
			delete msg_spawn_i;
		}
	}

	const int newIndex = _singleton->_msn->getNodeIndex();
	assert(newIndex == oldIndex);         // In the current implementation the index never changes.
	assert(newSize == oldSize + delta);

	MessageId::reset(newIndex, newSize);
	WriteIDManager::reset(newIndex, newSize);
	OffloadedTaskIdManager::reset(newIndex, newSize);

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

int ClusterManager::nanos6Shrink(const MessageShrink *msg_shrink)
{
	assert(_singleton != nullptr);
	assert(_singleton->_msn != nullptr);

	const int delta = msg_shrink->getDeltaNodes();
	assert(delta < 0);

	// Index actually never changes, only size. We prefix these OLD meaning that it was obtained
	// BEFORE any resize step.
	const int oldIndex = _singleton->_msn->getNodeIndex();
	const int oldSize = ClusterManager::clusterSize();

	ClusterServicesTask::setPauseStatus(true);
	ClusterServicesPolling::setPauseStatus(true);

	const MessageShrinkDataInfo *dataInfos = msg_shrink->getEntries();
	std::vector<DataTransfer *> transferList;

	for (size_t i = 0; i < msg_shrink->getNEntries(); ++i) {
		// When we receive the message, the first we need to do is to process all the transfers.
		// When a transfer is required and the process is involved (source or target) then we create
		// the all the data transfers in a non blocking way (otherwise an evident deadlock will be
		// created). All this needs to be done BEFORE the shrink, so it will be executed over the
		// initial communicator.
		const MessageShrinkDataInfo &info = dataInfos[i];

		if (info.oldLocationIdx != info.newLocationIdx) {
			// transfer required.
			assert(info.tag > 0);

			if (info.oldLocationIdx == oldIndex) {
				DataTransfer *tmp = ClusterManager::sendDataRaw(
					info.region,
					ClusterManager::getMemoryNode(info.newLocationIdx),
					info.tag,
					false,
					false
				);
				transferList.push_back(tmp);

			} else if (info.newLocationIdx == oldIndex) {
				DataTransfer *tmp = ClusterManager::fetchDataRaw(
					info.region,
					ClusterManager::getMemoryNode(info.oldLocationIdx),
					info.tag,
					false,
					false
				);
				transferList.push_back(tmp);
			}
			// TODO: We can add any else condition here for madvise on the other regions or to
			// cleanup regions based on writeId.
		}
	}
	ClusterManager::waitAllCompletion(transferList); // Block wait all the transfers finish

	// messenger calls spawn and merge, returns zero on the dying nodes.
	const int newSize = _singleton->_msn->messengerShrink(delta);

	if (newSize > 0) { // Condition for surviving nodes
		assert(newSize == oldSize + delta);
		const int newIndex = _singleton->_msn->getNodeIndex();
		assert(newIndex == oldIndex);

		// Surviving nodes
		for (int i = oldSize - 1; i >= newSize; --i) {
			ClusterNode *node = _singleton->_clusterNodes[i];
			VirtualMemoryManagement::unregisterNodeLocalRegion(node);
			delete node;
		}
		_singleton->_clusterNodes.resize(newSize);

		MessageId::reset(newIndex, newSize);
		WriteIDManager::reset(newIndex, newSize);
		OffloadedTaskIdManager::reset(newIndex, newSize);

		ClusterManager::synchronizeAll();

	} else {           // newSize is zero when this is a dying rank.
		MessageSysFinish msg;
		msg.handleMessage();
	}

	if (newSize == 1) { // When we started with a single node polling services didn't start.
		ClusterServicesTask::shutdownWorkers(_singleton->_numMessageHandlerWorkers);
		ClusterServicesPolling::shutdown();
	} else {            // Restart the polling services
		//assert(newSize > 1);
		ClusterServicesPolling::setPauseStatus(false);
		ClusterServicesTask::setPauseStatus(false);
	}

	return newSize;
}


int ClusterManager::nanos6Resize(int delta)
{
	assert(ClusterManager::isMasterNode());
	assert(ClusterManager::getInitData().clusterMalleabilityEnabled());
	// There is more than one host... This will be corrected with multiple processes/node
	assert(SlurmAPI::isEnabled());
	assert(SlurmAPI::getHostnameVector().size() > 1);

	TaskWait::taskWait("nanos6Resize");

	// Just some checks
	const int oldSize = _singleton->_msn->getClusterSize();
	assert(ClusterManager::clusterSize() == oldSize);
	if (delta == 0) {
		return oldSize;
	}

	const int expectedSize = oldSize + delta;

	// Some of these conditions may be substituted with assertions
	FatalErrorHandler::failIf((size_t)oldSize < _singleton->_dataInit._numMinNodes,
		"Old size can't be less than initial size: ", _singleton->_dataInit._numMinNodes);
	FatalErrorHandler::failIf((size_t)oldSize > _singleton->_dataInit._numMaxNodes,
		"Old size can't be bigger than numMaxNodes: ", _singleton->_dataInit._numMaxNodes);

	FatalErrorHandler::failIf(
		(size_t) oldSize < _singleton->_dataInit._numMinNodes
		|| (size_t) oldSize > _singleton->_dataInit._numMaxNodes,
		"Can't resize processes, current nodes: ", oldSize,
		" but the valid range is: [", _singleton->_dataInit._numMinNodes,
		";", _singleton->_dataInit._numMaxNodes, "]"
	);

	if ((size_t)expectedSize > _singleton->_dataInit._numMaxNodes
		|| (size_t)expectedSize < _singleton->_dataInit._numMinNodes) {
		FatalErrorHandler::warn(
			"Can't resize ", delta,
			" processes, current nodes: ", oldSize,
			" but the valid range is: [", _singleton->_dataInit._numMinNodes,
			";", _singleton->_dataInit._numMaxNodes, "]"
		);
		return oldSize;
	}

	if (delta > 0) {
		assert((size_t)oldSize < SlurmAPI::getHostnameVector().size());

		// TODO: Any spawn policy to implement may be done here in the hostInfos.
		std::vector<MessageSpawnHostInfo> hostInfos;
		hostInfos.push_back(
			MessageSpawnHostInfo(SlurmAPI::getHostnameVector()[oldSize].hostname, (size_t) delta)
		);

		// Master sends spawn messages to all the OLD world
		MessageSpawn msgSpawn(delta, hostInfos);
		ClusterManager::sendMessageToAll(&msgSpawn, true);

		// this is the same call that message handler does. So any improvement in resize will be
		// done in nanos6Spawn not here because that will be executed by all the processes.
		const int newSize = ClusterManager::nanos6Spawn(&msgSpawn);

		FatalErrorHandler::failIf(
			newSize != expectedSize,
			"Couldn't spawn ", expectedSize, " new processes; only ", newSize, " were created;"
		);

		// Share the existing dmallocs with the new processes.
		const ClusterMemoryManagement::dmalloc_container_t &mallocsList
			= ClusterMemoryManagement::getMallocsList();

		if (mallocsList.size() > 0) {
			MessageDmalloc msgDmallocInfo(mallocsList);

			for (size_t i = oldSize; i < (size_t)newSize; ++i) {
				ClusterNode *target = ClusterManager::getClusterNode(i);
				ClusterManager::sendMessage(&msgDmallocInfo, target, true);
			}
		}


	} else if (delta < 0) {

		ClusterMemoryManagement::redistributeDmallocs(expectedSize);

		// Process accesses fragments
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		assert(currentThread != nullptr);

		Task *currentTask = currentThread->getTask();
		assert(currentTask != nullptr);
		assert(currentTask->isMainTask());

		const ClusterMemoryNode *thisLocation = ClusterManager::getCurrentMemoryNode();

		TaskDataAccesses &accessStructures = currentTask->getDataAccesses();

		// Here we must construct the accesses
		std::vector<MessageShrinkDataInfo> shrinkDataInfo(oldSize);
		int tag = 1;

		accessStructures._accesses.processAll(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *dataAccess = &(*position);
				assert(dataAccess != nullptr);

				std::cout << *dataAccess << std::endl;

				const MemoryPlace *oldLocation = nullptr;
				const MemoryPlace *newLocation = nullptr;

				const nanos6_device_t accessDeviceType = dataAccess->getLocation()->getType();

				const DataAccessRegion &region = dataAccess->getAccessRegion();

				if (accessDeviceType == nanos6_cluster_device) {

					// By default we don't migrate the accesses
					oldLocation = dataAccess->getLocation();
					newLocation = dataAccess->getLocation();

					const ClusterMemoryNode *oldClusterLocation
						= dynamic_cast<const ClusterMemoryNode*>(oldLocation);

					assert(oldClusterLocation != nullptr);
					assert(oldClusterLocation != thisLocation);
					assert(oldClusterLocation->getCommIndex() < ClusterManager::clusterSize());

					if (oldClusterLocation->getCommIndex() >= expectedSize) {
						// Come here only if the current location will die after shrinking; so the
						// policy to migrate can be implemented inside this scope.

						std::vector<size_t> bytes((size_t)expectedSize);
						const Directory::HomeNodesArray *homeNodes = Directory::find(region);

						// The current policy is to find all the new home nodes and try to find the
						// one containing the bigger region and move there the whole region.
						for (const auto &entry : *homeNodes) {
							const MemoryPlace *location = entry->getHomeNode();
							assert(location->getType() == nanos6_host_device);

							// New location should be in the expected size range.
							const size_t homeNodeId = location->getIndex();
							assert(homeNodeId < (size_t)expectedSize);

							const DataAccessRegion subregion = region.intersect(entry->getAccessRegion());
							assert(subregion.getSize() != 0);
							bytes[homeNodeId] += subregion.getSize();
						}
						delete homeNodes;

						const size_t destinationIndex
							= std::distance(bytes.begin(), std::max_element(bytes.begin(), bytes.end()));

						newLocation = ClusterManager::getMemoryNode(destinationIndex);

						dataAccess->setLocation(newLocation);
					}
				} else if (accessDeviceType == nanos6_host_device) {
					if (VirtualMemoryManagement::isDistributedRegion(region)) {
						// This information is only for the madvise purposes. Because the accesses
						// on root never need to migrate.
						oldLocation = thisLocation;
						newLocation = thisLocation;
					}

				} else {
					FatalErrorHandler::fail(
						"Access of type: ", accessDeviceType, " is unsupported for malleability."
					);
				}

				if (oldLocation != nullptr) {
					// oldLocation is set only for the accesses we want to share. The other accesses
					// are ignored.
					MessageShrinkDataInfo shrinkInfo = {
						.region = region,
						.oldLocationIdx = oldLocation->getIndex(),
						.newLocationIdx = newLocation->getIndex(),
						.writeId = dataAccess->getWriteID(),
						.tag = tag++  // TODO: Get the right tag here...
					};

					shrinkDataInfo.push_back(shrinkInfo);
				}

				return true; // continue, to process all access fragments
			});

		MessageShrink msgShrink(delta, shrinkDataInfo);

		ClusterManager::sendMessageToAll(&msgShrink, true);

		__attribute__((unused)) const int newSize = ClusterManager::nanos6Shrink(&msgShrink);
		assert(newSize == expectedSize || newSize == 0); // zero for dying nodes
	}

	return expectedSize;
}

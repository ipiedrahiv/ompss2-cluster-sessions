/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include <api/nanos6/cluster.h>

#include <ClusterManager.hpp>

#include <ClusterMemoryManagement.hpp>
#include <VirtualMemoryManagement.hpp>

#include "messages/MessageDmalloc.hpp"
#include "messages/MessageSysFinish.hpp"
#include "messages/MessageResize.hpp"
#include "messages/MessageResizeImplementation.hpp"

#include "dependencies/linear-regions-fragmented/TaskDataAccesses.hpp"
#include "polling-services/ClusterServicesPolling.hpp"
#include "polling-services/ClusterServicesTask.hpp"

#include "ClusterUtil.hpp"
#include "WriteID.hpp"

#include "system/ompss/TaskWait.hpp"



#if HAVE_SLURM
#include "SlurmAPI.hpp"
#endif // HAVE_SLURM

int ClusterManager::nanos6Resize(int delta, nanos6_spawn_policy_t policy)
{
	assert(ClusterManager::isMasterNode());
	assert(ClusterManager::getInitData().clusterMalleabilityEnabled());
	assert(SlurmAPI::isEnabled());

	// Do some checks
	const int oldSize = _singleton->_msn->getClusterSize();
	assert(ClusterManager::clusterSize() == oldSize);        // Let's be a bit paranoiac
	if (delta == 0) {
		return oldSize;
	}

	// Some of these conditions may be substituted with assertions
	FatalErrorHandler::failIf((size_t)oldSize < _singleton->_dataInit._numMinNodes,
		"Old size can't be less than initial size: ", _singleton->_dataInit._numMinNodes);
	FatalErrorHandler::failIf((size_t)oldSize > _singleton->_dataInit._numMaxNodes,
		"Old size can't be bigger than numMaxNodes: ", _singleton->_dataInit._numMaxNodes);

	const int expectedSize = oldSize + delta;

	// Some of these conditions may be substituted with assertions
	FatalErrorHandler::failIf((size_t)expectedSize < _singleton->_dataInit._numMinNodes,
		"Can't resize expected:", expectedSize,
		" smaller than _numMinNodes: ", _singleton->_dataInit._numMinNodes);

	FatalErrorHandler::failIf((size_t)expectedSize > _singleton->_dataInit._numMaxNodes,
		"Can't resize expected:", expectedSize,
		" bigger than _numMaxNodes: ", _singleton->_dataInit._numMaxNodes);

	const int neededNewHosts
		= SlurmAPI::permitsExpansion()
		? SlurmAPI::requestHostsForNRanks(expectedSize)
		: 0;

	// TODO: Manage correctly the error cases: ==0; <0; or >0;
	FatalErrorHandler::failIf(
		neededNewHosts < 0, "Request hosts for:", expectedSize," ranks; returned:", neededNewHosts
	);

	TaskWait::taskWait("nanos6Resize");

	int newSize = -1;

	if (delta > 0) {
		// Check slurm for allocations.
		if (neededNewHosts > 0) {
			// TODO: We can make a policy here. We could either wait for the allocation, with a loop
			// and a timeout or fail or continue as nothing happened. We can either add an extra
			// parameter to decide what to do.
			const int allocatedNewHosts = SlurmAPI::checkAllocationRequest();

			// TODO: Manage correctly the error cases: ==0; <0; or >0;
			FatalErrorHandler::failIf(
				allocatedNewHosts < 0,
				"Error allocating more hosts, the request check returned an error"
			);

			FatalErrorHandler::failIf(
				allocatedNewHosts < neededNewHosts,
				"Needed ", neededNewHosts, " jobs but only ", allocatedNewHosts, "received"
			);
		}

		// TODO: Any spawn policy to implement may be done here in the hostInfos.
		std::vector<MessageSpawnHostInfo> hostInfos = SlurmAPI::getSpawnHostInfoVector(delta);
		if (hostInfos.empty()){
			FatalErrorHandler::warn("There are not hosts/spots to spawn more processes");
			return oldSize;
		}

		// Master sends spawn messages to all the OLD world
		MessageSpawn msgSpawn(policy, delta, hostInfos);
		ClusterManager::sendMessageToAll(&msgSpawn, true);

		// this is the same call that message handler does. So any improvement in resize will be
		// done in nanos6Spawn not here because that will be executed by all the processes.
		newSize = ClusterManager::handleResizeMessage(&msgSpawn);

		// Share the dmallocs with the new processes... there is a potential issue here. It may be
		// too late as the remote processes may have even performed some spawns... but no dmallocs
		// are performed during that process.
		// Redistribution in master was made with all the other initial processes at the end of
		// handleResizeMessage
		const ClusterMemoryManagement::dmalloc_container_t &mallocsList
			= ClusterMemoryManagement::getMallocsList();

		if (mallocsList.size() > 0) {
			MessageDmalloc msgDmallocInfo(mallocsList);
			ClusterManager::sendMessageToAll(&msgDmallocInfo, true, oldSize, newSize);
		}


	} else if (delta < 0) {
		// This needs to take place before because we use the new home node information to
		// redistribute data in the migration. the other processes will do this as soon as they get
		// into the handle message
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
		std::vector<MessageShrinkDataInfo> shrinkDataInfo;
		int tag = 1;

		accessStructures._accesses.processAll(
			[&](TaskDataAccesses::accesses_t::iterator position) -> bool {
				DataAccess *dataAccess = &(*position);
				assert(dataAccess != nullptr);

				const MemoryPlace *oldLocation = nullptr;
				const MemoryPlace *newLocation = nullptr;

				const MemoryPlace *accessLocation = dataAccess->getLocation();
				assert(accessLocation != nullptr);

				const DataAccessRegion &region = dataAccess->getAccessRegion();
				assert(!region.empty());

				const nanos6_device_t accessDeviceType = accessLocation->getType();

				if (accessDeviceType == nanos6_cluster_device) {

					// By default we don't migrate the accesses
					oldLocation = accessLocation;
					newLocation = accessLocation;

					const ClusterMemoryNode *oldClusterLocation
						= dynamic_cast<const ClusterMemoryNode*>(oldLocation);

					assert(oldClusterLocation != nullptr);
					assert(oldClusterLocation->getCommIndex() < oldSize);

					if (oldClusterLocation->getCommIndex() >= expectedSize) {
						// Come here only if the current location will die after shrinking; so the
						// policy to migrate can be implemented inside this scope.

						std::vector<size_t> bytes((size_t)expectedSize);
						const Directory::HomeNodesArray *homeNodes = Directory::find(region);

						// The current policy is to find all the new home nodes and try to find the
						// one containing the bigger region and move there the whole region.
						for (const auto &entry : *homeNodes) {
							const MemoryPlace *location = entry->getHomeNode();
							assert(location->getType() == nanos6_host_device
								|| location->getType() == nanos6_cluster_device);

							const size_t homeNodeId
								= (location->getType() == nanos6_host_device)
								? thisLocation->getIndex()
								: location->getIndex();

							// New location should be in the expected size range because the dmalloc
							// redistribution already took place here.
							assert(homeNodeId < (size_t)expectedSize);

							const DataAccessRegion subregion
								= region.intersect(entry->getAccessRegion());
							assert(subregion.getSize() != 0);
							bytes[homeNodeId] += subregion.getSize();
						}
						delete homeNodes;

						const size_t destinationIndex = std::distance(
							bytes.begin(), std::max_element(bytes.begin(), bytes.end())
						);

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
						"Region: ", region, " has unsupported access type: ", accessDeviceType
					);
				}

				const WriteID oldWriteId = dataAccess->getWriteID();
				const int writeIDNode = WriteIDManager::getWriteIDNode(oldWriteId);

				if (writeIDNode >= expectedSize) {
					dataAccess->setNewWriteID();
				}

				if (oldLocation != nullptr) {
					// oldLocation is set only for the accesses we want to share. The other accesses
					// are ignored.
					MessageShrinkDataInfo shrinkInfo = {
						.region = region,
						.oldLocationIdx = oldLocation->getIndex(),
						.newLocationIdx = newLocation->getIndex(),
						.oldWriteId = oldWriteId,
						.newWriteId = dataAccess->getWriteID(),
						.tag = tag++  // TODO: Get the right tag here...
					};

					shrinkDataInfo.push_back(shrinkInfo);
				}

				return true; // continue, to process all access fragments
			});

		MessageShrink msgShrink(policy, delta, shrinkDataInfo);

		ClusterManager::sendMessageToAll(&msgShrink, true);

		newSize = ClusterManager::handleResizeMessage(&msgShrink);

		FatalErrorHandler::failIf(
			newSize != expectedSize,
			"Couldn't spawn: ", expectedSize, " new processes; only: ", newSize, " were created."
		);

		if (SlurmAPI::permitsExpansion()) {
			// We don't want to (really) release the hosts back to slurm IF permitsExpansion is
			// disabled, because it may be impossible to reallocate them back in the future.
			const int releasedHosts = SlurmAPI::releaseUnusedHosts();
			FatalErrorHandler::failIf(releasedHosts < 0, "Error releasing hosts with SlurmAPI");
		}

	}

	FatalErrorHandler::failIf(
		newSize != expectedSize,
		"Couldn't spawn: ", expectedSize, " new processes; only: ", newSize, " were created."
	);

	return expectedSize;
}


// SPAWN

int ClusterManager::resizeFull(
	int delta, size_t nEntries, const MessageSpawnHostInfo *entries
) {
	const int oldSize = ClusterManager::clusterSize();
	const int newSize = _singleton->_msn->messengerSpawn(delta, "");
	FatalErrorHandler::failIf(newSize != oldSize + delta,
		"Group spawned to:", newSize, " but expected:", oldSize, "+", delta);

	int spawned = 0;

	for (size_t ent = 0; ent < nEntries; ++ent) {
		assert(ClusterManager::clusterSize() == oldSize + spawned);

		const MessageSpawnHostInfo &info = entries[ent];

		for (size_t step = 0; step < info.nprocs; ++step) {

			const int newindex = oldSize + spawned;

			// Register the new nodes and their memory
			ClusterNode *node = new ClusterNode(newindex, newindex, 0, false, newindex);
			assert(node != nullptr);
			node->setHostName(info.hostname);

			_singleton->_clusterNodes.push_back(node);
			VirtualMemoryManagement::registerNodeLocalRegion(node);

			if (ClusterManager::isMasterNode()) {
				DataAccessRegion init_region((void *)&_singleton->_dataInit, sizeof(DataInitSpawn));
				assert(node->getMemoryNode() != nullptr);

				// Send init message
				ClusterManager::sendDataRaw(
					init_region, node->getMemoryNode(), std::numeric_limits<int>::max(), true
				);
			}
			++spawned;
		}
		if (SlurmAPI::isEnabled()) {
			// This update is wrong because we don't know exactly where srun sets the new processes.
			// To do it properly we need to do a gather from master to get accurate hostname.  At
			// the moment this is not an issue as this policy is not recommended. I only use it for
			// local benchmarks
			SlurmAPI::deltaProcessToHostname(info.hostname, info.nprocs);
		}
	}
	assert(spawned == delta);

	return spawned;
}


int ClusterManager::resizeByPolicy(
	nanos6_spawn_policy_t policy, int delta, size_t nEntries, const MessageSpawnHostInfo *entries
) {
	int pending = delta;

	std::vector<MessageSpawnHostInfo> spawnInfos(entries, entries + nEntries);

	const bool isMaster = ClusterManager::isMasterNode();

	while(spawnInfos.size() > 0) {
		assert(pending > 0);

		MessageSpawnHostInfo &info = spawnInfos.front();
		assert(info.nprocs > 0);

		const int deltaStep = (policy == nanos6_spawn_by_host) ? info.nprocs
			: (policy == nanos6_spawn_by_one) ? 1
			: 0;

		FatalErrorHandler::failIf(deltaStep <= 0, "Wrong spawn policy in resizeByPolicy");

		if (pending != delta) {
			assert(pending < delta);
			// This to match with new processes just coming and stopping polling services
			ClusterManager::synchronizeAll();
		}

		const int newSize = _singleton->_msn->messengerSpawn(deltaStep, info.hostname);
		const int oldSize = ClusterManager::clusterSize();

		assert(newSize == oldSize + deltaStep);
		assert(info.nprocs >= (size_t) deltaStep);

		// Create new nodes and sent init information.
		for (int it = oldSize; it < newSize; ++it) {

			// Register the new nodes and their memory
			ClusterNode *node = new ClusterNode(it, it, 0, false, it);
			assert(node != nullptr);
			node->setHostName(info.hostname);

			_singleton->_clusterNodes.push_back(node);
			VirtualMemoryManagement::registerNodeLocalRegion(node);

			if (isMaster) {
				DataAccessRegion init_region((void *)&_singleton->_dataInit, sizeof(DataInitSpawn));
				assert(node->getMemoryNode() != nullptr);

				// Send init message
				ClusterManager::sendDataRaw(
					init_region, node->getMemoryNode(), std::numeric_limits<int>::max(), true
				);
			}
		}

		// Update hostname counter in Slurm API
		if (SlurmAPI::isEnabled()) {
			SlurmAPI::deltaProcessToHostname(info.hostname, deltaStep);
		}

		// Update the spawnInfos list and remove empty entry
		info.nprocs -= deltaStep;
		pending -= deltaStep;
		if (info.nprocs == 0) {
			spawnInfos.erase(spawnInfos.begin());
		}

		// Sent the resize messages to the new processes.
		if (isMaster && spawnInfos.size() > 0) {
			assert(pending > 0);
			MessageSpawn msgSpawn_i(policy, pending, spawnInfos);
			ClusterManager::sendMessageToAll(&msgSpawn_i, true, oldSize, newSize);
		}
	}
	assert(pending == 0);

	return delta;
}

int ClusterManager::handleResizeMessage(const MessageResize<MessageSpawnHostInfo> *msgSpawn)
{
	assert(_singleton != nullptr);
	assert(_singleton->_msn != nullptr);

	const int oldIndex = _singleton->_msn->getNodeIndex();
	const int oldSize = ClusterManager::clusterSize();

	ClusterManager::synchronizeAll(); // do this BEFORE stopping the polling services

	if (oldSize > 1) {                // Stop polling services.
		ClusterServicesPolling::shutdown();
		ClusterServicesTask::shutdownWorkers(_singleton->_numMessageHandlerWorkers);
	}

	const int delta = msgSpawn->getDeltaNodes();
	assert(delta > 0);
	const size_t nEntries = msgSpawn->getNEntries();
	assert(nEntries > 0);

	const MessageSpawnHostInfo *entries = msgSpawn->getEntries();
	assert(entries != nullptr);

	int spawned = 0;

	const nanos6_spawn_policy_t policy = msgSpawn->getPolicy();

	if (policy == nanos6_spawn_by_group) {
		spawned = _singleton->resizeFull(delta, nEntries, entries);
	} else if (policy == nanos6_spawn_by_host || policy == nanos6_spawn_by_one) {
		spawned = _singleton->resizeByPolicy(policy, delta, nEntries, entries);
	} else {
		FatalErrorHandler::fail("Wrong policy in: ", __func__);
	}

	FatalErrorHandler::failIf(
		oldIndex != _singleton->_msn->getNodeIndex(),
		"Index changed after spawn"
	);

	FatalErrorHandler::failIf(
		spawned != delta,
		"Spawned size is wrong value:", spawned, " expected:", delta
	);

	assert(spawned > 0);

	const int newSize = ClusterManager::clusterSize();
	assert(oldSize + delta == newSize);

	// Will redistribute at the end because we need to have all the new hosts.  Only the old
	// processes will do something useful here. The just spawned ones will receive an updated
	// dmalloc info messages latter from master.
	ClusterMemoryManagement::redistributeDmallocs(newSize);

	// Restart the services
	ClusterServicesPolling::initialize();
	ClusterServicesTask::initializeWorkers(_singleton->_numMessageHandlerWorkers);

	return newSize;
}


// SHRINK
int ClusterManager::handleResizeMessage(const MessageResize<MessageShrinkDataInfo> *msgShrink)
{
	assert(_singleton != nullptr);
	assert(_singleton->_msn != nullptr);

	// We don't stop the services until everybody is here...
	ClusterManager::synchronizeAll();

	ClusterServicesPolling::shutdown();
	ClusterServicesTask::shutdownWorkers(_singleton->_numMessageHandlerWorkers);

	const int delta = msgShrink->getDeltaNodes();
	assert(delta < 0);

	// Index actually never changes, only size. We prefix these OLD meaning that it was obtained
	// BEFORE any resize step.
	const int oldIndex = _singleton->_msn->getNodeIndex();
	const int oldSize = ClusterManager::clusterSize();
	assert(oldSize + delta > 0);

	const MessageShrinkDataInfo *dataInfos = msgShrink->getEntries();
	std::vector<DataTransfer *> transferList;

	struct timespec startRes;
	if (ClusterManager::isMasterNode()) {
		clock_gettime(CLOCK_MONOTONIC, &startRes);
	}

	for (size_t i = 0; i < msgShrink->getNEntries(); ++i) {
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
				WriteIDManager::registerWriteIDasLocal(info.newWriteId, info.region);
			} else if (info.newWriteId != info.oldWriteId) {
				WriteIDManager::updateExistingWriteID(info.oldWriteId, info.newWriteId, info.region);
			}
			// TODO: We can add any else condition here for madvise on the other regions or to
			// cleanup regions based on writeId.
		}
	}

	ClusterManager::waitAllCompletion(transferList); // Block wait all the transfers finish

	ClusterManager::synchronizeAll();

	struct timespec endRes;
	if (ClusterManager::isMasterNode()) {
		clock_gettime(CLOCK_MONOTONIC, &endRes);
	}

	// messenger shrink returns zero on the dying nodes.
	const int newSize = _singleton->_msn->messengerShrink(delta);
	assert(newSize >= 0); // negative means error

	if (ClusterManager::isMasterNode()) {
		struct timespec deltaRes = clusterDiffTime(&startRes, &endRes);

		printf("# Resize_transfer:%d->%d: %lg\n",
			oldSize, newSize, deltaRes.tv_sec * 1.0E9 + deltaRes.tv_nsec);
	}

	if (newSize > 0) { // Condition for surviving nodes
		WriteIDManager::limitWriteIDToMaxNodes(newSize);

		assert(newSize == oldSize + delta);
		const int newIndex = _singleton->_msn->getNodeIndex();

		FatalErrorHandler::failIf(newIndex != oldIndex,
			"Index changed after shrink: ", oldIndex, " -> ", newIndex);

		// Surviving nodes
		for (int i = oldSize - 1; i >= newSize; --i) {
			ClusterNode *node = _singleton->_clusterNodes[i];
			VirtualMemoryManagement::unregisterNodeLocalRegion(node);

			if (SlurmAPI::isEnabled()) {
				// Discount the process from host counter.
				assert(ClusterManager::isMasterNode());
				assert(!node->getHostName().empty());
				SlurmAPI::deltaProcessToHostname(node->getHostName(), -1);
			}

			delete node;
		}
		_singleton->_clusterNodes.resize(newSize);

		if (!ClusterManager::isMasterNode) {
			ClusterMemoryManagement::redistributeDmallocs(newSize);
		}

		ClusterManager::synchronizeAll();

	} else {           // newSize is zero when this is a dying rank.
		MessageSysFinish msg;
		msg.handleMessage();
	}

	if (newSize > 1) { // When we started with a single node polling services didn't start.
		ClusterServicesPolling::initialize();
		ClusterServicesTask::initializeWorkers(_singleton->_numMessageHandlerWorkers);
	}

	return newSize;
}

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#include <nanos6/cluster.h>

#include <ClusterManager.hpp>
#include <ClusterMemoryManagement.hpp>
#include <ClusterNode.hpp>
#include <Serialize.hpp>
#include "tasks/Task.hpp"
#include "executors/threads/WorkerThread.hpp"
#include "lowlevel/FatalErrorHandler.hpp"

extern "C" {

	// Compatibility in pure OmpSs-2@Cluster, "node" means irank
	int nanos6_is_master_node(void)
	{
		return nanos6_is_master_irank();
	}

	// Compatibility: in pure OmpSs-2@Cluster, "node" means irank
	int nanos6_get_cluster_node_id(void)
	{
		return nanos6_get_cluster_irank_id();
	}

	// Compatibility: in pure OmpSs-2@Cluster, "node" means irank
	int nanos6_get_num_cluster_nodes(void)
	{
		return nanos6_get_num_cluster_iranks();
	}

	int nanos6_is_master_irank(void)
	{
		return ClusterManager::isMasterNode();
	}

	int nanos6_get_cluster_physical_node_id(void)
	{
		return ClusterManager::getPhysicalNodeNum();
	}

	int nanos6_get_num_cluster_physical_nodes(void)
	{
		return ClusterManager::getNumNodes();
	}

	int nanos6_get_cluster_irank_id(void)
	{
		return ClusterManager::getCurrentClusterNode()->getIndex();
	}

	int nanos6_get_num_cluster_iranks(void)
	{
		return ClusterManager::clusterSize();
	}

	int nanos6_get_namespace_is_enabled(void)
	{
		// Invert this value.
		return ClusterManager::getDisableRemote() == false;
	}


	void *nanos6_dmalloc(
		size_t size,
		nanos6_data_distribution_t policy,
		size_t num_dimensions,
		size_t *dimensions
	) {
		if (size == 0) {
			return nullptr;
		}

		return ClusterMemoryManagement::dmalloc(size, policy, num_dimensions, dimensions);
	}

	void nanos6_dfree(void *ptr, size_t size)
	{
		ClusterMemoryManagement::dfree(ptr, size);
	}

	void *nanos6_lmalloc(size_t size)
	{
		if (size == 0) {
			return nullptr;
		}

		return ClusterMemoryManagement::lmalloc(size);
	}

	void nanos6_lfree(void *ptr, size_t size)
	{
		ClusterMemoryManagement::lfree(ptr, size);
	}

	void nanos6_set_early_release(nanos6_early_release_t early_release)
	{
		WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
		Task *task = currentThread->getTask();
		assert(task != nullptr);
		task->setEarlyRelease(early_release);
	}

	int nanos6_cluster_resize_with_policy(int delta, nanos6_spawn_policy_t policy)
	{
#if HAVE_SLURM
		return ClusterManager::nanos6Resize(delta, policy);
#else
		FatalErrorHandler::warn("Nanos6 is compiled without malleability support, ignore resize");
		return ClusterManager::clusterSize();
#endif
	}

	int nanos6_cluster_resize(int delta)
	{
		return nanos6_cluster_resize_with_policy(delta, nanos6_spawn_default);
	}


	int nanos6_get_app_communicator(void *appcomm)
	{
#ifndef USE_CLUSTER
		// If the runtime doesn't have support for cluster, then it hasn't been built with
		// MPI. So we cannot return an MPI communicator of any sort.
		FatalErrorHandler::fail(
			"nanos6_get_app_communicator() should only be called if Nanos6 is built with cluster support.");
#endif
		MPI_Comm *appcomm_ptr = (MPI_Comm *)appcomm;
		*appcomm_ptr = ClusterManager::getAppCommunicator();
		return 0;
	}

	int nanos6_serialize(void *start, size_t nbytes, size_t process, size_t id, int checkpoint)
	{
		DataAccessRegion region(start, nbytes);

		return Serialize::serializeRegion(region, process, id, (bool) checkpoint);
	}

	void nanos6_fail(const char message[])
	{
		if (message != nullptr && message[0] != '\0') {
			std::string reason = message;
			FatalErrorHandler::fail(reason);
		} else {
			FatalErrorHandler::nanos6Abort();
		}
	}

	int nanos6_get_cluster_info(nanos6_cluster_info_t *info)
	{
		return ClusterManager::nanos6GetInfo(info);
	}

	int nanos6_in_cluster_mode(void)
	{
		return ClusterManager::inClusterMode();
	}
}

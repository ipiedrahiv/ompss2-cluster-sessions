/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef SLURMAPI_HPP
#define SLURMAPI_HPP

#include <cstdlib>
#include <ctime>
#include <string>
#include <signal.h>
#include <unistd.h>
#include <slurm/slurm.h>

#include <MessageResize.hpp>

// Part of this information may be handled by the Hardware info too; but we need to get it from
// slurm in order to distribute the work properly.
class SlurmAPI {
public:
	struct SlurmHostInfo {
		std::string _hostname;
		size_t _nProcesses;

		SlurmHostInfo(const std::string &hostname)
			: _hostname(hostname), _nProcesses(0)
		{
		}

		void changeProcesses(int delta)
		{
			const int expected = _nProcesses + delta;
			assert(expected >= 0);
			assert((size_t)expected <= SlurmAPI::getTasksPerNode());
			_nProcesses = expected;
		}

		friend std::ostream &operator<<(std::ostream &out, const SlurmHostInfo &info)
		{
			out << "Host:" << info._hostname << "processes:" << info._nProcesses;
			return out;
		}
	};

private:
	template<typename... TS>
	static inline void failIf(bool cond, TS... reasonParts)
	{
		FatalErrorHandler::failIf(cond, reasonParts..., "Slurm Error:", slurm_strerror(errno));
	}

	const uint32_t _slurmJobId;
	size_t _cpusPerTask, _tasksPerNode, _partitionMaxNodes;
	uint32_t _user_id, _group_id;
	time_t _end_time;
	std::string _partition;
	bool _permitsExpansion;

	std::vector<SlurmHostInfo> _hostInfoVector;


	// We may have only one of these as we expect to have only one request at the time. We may have
	// multiple requests, but then we need a much complex code to handle all pending requests and
	// update the host list.
	resource_allocation_response_msg_t *_slurmPendingMsgPtr = nullptr;

	static SlurmAPI *_singleton;

	std::vector<SlurmHostInfo>::iterator getHostinfoByHostname(const std::string &hostname)
	{
		std::vector<SlurmHostInfo>::iterator hostInfoIt
			= std::find_if_not(   // The only one in C++-11
				_hostInfoVector.begin(),
				_hostInfoVector.end(),
				[&](const SlurmHostInfo &hostInfo) -> bool {
					return (hostInfo._hostname != hostname);
				}
			);

		FatalErrorHandler::failIf(
			hostInfoIt == _hostInfoVector.end(),
			"Hostname: ", hostname, " is not in the HostnameList."
		);

		return hostInfoIt;
	}

	static std::vector<std::string> hostListToHostinfoVector(const std::string &hostString)
	{
		std::vector<std::string> hostInfoVector; // for return

		hostlist_t hostlist = slurm_hostlist_create(hostString.c_str());
		SlurmAPI::failIf(hostlist == NULL, "slurm_hostlist_create returned NULL");

		// Initialize the node list.
		char *host;
		while ((host = slurm_hostlist_shift(hostlist))) {
			hostInfoVector.push_back(host);
		}
		slurm_hostlist_destroy(hostlist);

		// This will use a move operator.
		return hostInfoVector;
	}

	// Hopefully this is not needed, but in case the user accesses the slurm variables later it may
	// be useful (and correct) to keep these updated.
	// TODO: At the moment only master updated the environment. If needed we need to update all the
	// hosts environments... but lets keep it simple for now as we don't need that for now.
	void updateEnvironment()
	{
		resource_allocation_response_msg_t *updatedInfo;
		int rc = slurm_allocation_lookup(_slurmJobId, &updatedInfo);
	    SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_allocation_lookup after release returned:", rc);

		if (getenv("SLURM_NODELIST")) {
			setenv("SLURM_NODELIST", updatedInfo->node_list, 1);
		}

		if (getenv("SLURM_JOB_NODELIST")) {
			setenv("SLURM_JOB_NODELIST", updatedInfo->node_list, 1);
		}

		std::string node_cnt = std::to_string(updatedInfo->node_cnt);

		if (getenv("SLURM_NNODES")) {
			setenv("SLURM_NNODES", node_cnt.c_str(), 1);
		}

		if (getenv("SLURM_JOB_NUM_NODES")) {
			setenv("SLURM_JOB_NUM_NODES", node_cnt.c_str(), 1);
		}

		if (getenv("SLURM_JOB_CPUS_PER_NODE")) {
			//to support more than one we need uint32_compressed_to_str from slurm code
			assert(updatedInfo->num_cpu_groups == 1);

			std::string job_cpus_per_node
				= std::to_string(updatedInfo->cpus_per_node[0])
				+ "(x" + std::to_string(updatedInfo->cpu_count_reps[0]) + ")";

			setenv("SLURM_JOB_CPUS_PER_NODE", job_cpus_per_node.c_str(), 1);
		}

		if (getenv("SLURM_TASKS_PER_NODE")) {
			// We have the information to update these, but in different moments.
			unsetenv("SLURM_NPROCS");
			unsetenv("SLURM_NTASKS");
		}

		slurm_free_resource_allocation_response_msg(updatedInfo);
	}

public:
	SlurmAPI()
		: _slurmJobId(EnvironmentVariable<uint32_t>("SLURM_JOBID").getValue()),
		  _partitionMaxNodes(std::numeric_limits<size_t>::max())
	{
		// Get the JOB information ======================
		job_info_msg_t *jobInfoMsg = nullptr;
		int rc = slurm_load_job(&jobInfoMsg, _slurmJobId, 0);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_load_job returned: ", rc);
		SlurmAPI::failIf(jobInfoMsg == NULL, "slurm_load_job set jobInfoMsg to NULL");
		SlurmAPI::failIf(
			jobInfoMsg->record_count != 1,
			"slurm_load_job returned ", jobInfoMsg->record_count, " record_count entries"
		);

		// We shouldn't store this permanently because some values change in time. So we need to
		// follow these steps and call slurm_load_job/slurm_free_job_info_msg every time we need
		// them. We will store some values that we may need latter if they are not intended to
		// change during execution.
		const slurm_job_info_t &jobInfo = jobInfoMsg->job_array[0];

		std::vector<std::string> hostnamesVector = SlurmAPI::hostListToHostinfoVector(jobInfo.nodes);

		const std::string nodelist = EnvironmentVariable<std::string>("SLURM_NODELIST").getValue();
		FatalErrorHandler::failIf(nodelist.empty(), "Couldn't get SLURM_NODELIST.");

		// Set job vars
		_cpusPerTask = jobInfo.cpus_per_task;
		_tasksPerNode = jobInfo.ntasks_per_node;
		_user_id = jobInfo.user_id;
		_group_id = jobInfo.group_id;
		_end_time = jobInfo.end_time;
		_partition = jobInfo.partition;

		FatalErrorHandler::failIf(_cpusPerTask == 0, "Couldn't get cpus_per_task.");
		FatalErrorHandler::failIf(_tasksPerNode == 0, "Couldn't get ntasks_per_node.");

		for (const std::string &hostname : hostnamesVector) {
			_hostInfoVector.emplace_back(hostname);
		}
		FatalErrorHandler::failIf(_hostInfoVector.empty(), "Error nodelist_vector is empty.");

		// Get the config information ======================
		slurm_conf_t  *slurmCtlConf = NULL;

		rc = slurm_load_ctl_conf ((time_t) NULL, &slurmCtlConf);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_load_ctl_conf returned: ", rc);
		SlurmAPI::failIf(slurmCtlConf == NULL, "slurm_load_ctl_conf set slurmConf to NULL");

		// set config vars.
		// Check that permit_expansion is enabled.
		const std::string schedParams = slurmCtlConf->sched_params;
		_permitsExpansion = (schedParams.find("permit_job_expansion") != std::string::npos);

		// Get the partition information ======================
		partition_info_msg_t *partitionInfoMsg = NULL;
		rc = slurm_load_partitions((time_t) NULL, &partitionInfoMsg, (uint16_t) 0);
		SlurmAPI::failIf(rc != 0, "slurm_load_partitions returned: ", rc);
		SlurmAPI::failIf(
			partitionInfoMsg == NULL, "slurm_load_partitions set partitionInfoMsg to NULL"
		);
		SlurmAPI::failIf(
			partitionInfoMsg->record_count == 0, "slurm_load_partitions no partition information"
		);

		for (uint32_t i = 0; i < partitionInfoMsg->record_count; ++i) {
			partition_info_t &info = partitionInfoMsg->partition_array[i];

			if (_partition == info.name) {
				_partitionMaxNodes = info.total_nodes;
				break;
			}
		}

		// Cleanup everything
		slurm_free_partition_info_msg(partitionInfoMsg);
		slurm_free_ctl_conf(slurmCtlConf);
		slurm_free_job_info_msg(jobInfoMsg);

	}

	~SlurmAPI()
	{
		if (_singleton->_slurmPendingMsgPtr != nullptr) {
			// This should never happen, but you know... never say never to a paranoiac
			FatalErrorHandler::warn("There is a Slurm allocation pending, needed cancelation");

			int rc = slurm_kill_job(_singleton->_slurmPendingMsgPtr->job_id, SIGKILL, 0);
			SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_kill_job returned: ", rc);

			slurm_free_resource_allocation_response_msg(_singleton->_slurmPendingMsgPtr);
		}
	}

	static void initialize()
	{
		assert(_singleton == nullptr);
		_singleton = new SlurmAPI();
		assert(_singleton != nullptr);

		const std::string masterHost = ClusterManager::getMasterNode()->getHostName();
		// Move the master Hostname to the first position (rotating).
		if (masterHost != _singleton->_hostInfoVector[0]._hostname) {
			std::rotate(
				_singleton->_hostInfoVector.begin(),
				_singleton->getHostinfoByHostname(masterHost),
				_singleton->_hostInfoVector.end()
			);
		}

		// Now count the number of processes per host.
		for (ClusterNode *it : ClusterManager::getClusterNodes()) {
			SlurmAPI::deltaProcessToHostname(it->getHostName(), 1);
		}
	}

	static void finalize()
	{
		assert(_singleton != nullptr);
		delete _singleton;
		_singleton = nullptr;
	}

	static bool isEnabled()
	{
		return _singleton != nullptr;
	}

	static size_t getTasksPerNode()
	{
		assert(_singleton != nullptr);
		return _singleton->_tasksPerNode;
	}

	static void deltaProcessToHostname(const std::string &hostname, int delta)
	{
		assert(_singleton != nullptr);
		std::vector<SlurmHostInfo>::iterator info = _singleton->getHostinfoByHostname(hostname);
		info->changeProcesses(delta);
	}

	// This performs a resources request if N processes does not fit in the current number of hosts.
	// The request returns immediately (either with the resources or not; we don't care at this
	// point).  Some other function may check the allocation latter.  This call can/may be done
	// before the taskwait implicit in the resize function to request in advance and the latter
	// check after the taskwait will have more probabilities that the new resources granted on that
	// point.
	// 0 no request needed; >0 request made (number of extra hosts); <0 error
	static int requestHostsForNRanks(size_t N)
	{
		assert(_singleton != nullptr);
		assert(_singleton->_slurmPendingMsgPtr == nullptr);

		const size_t ppn = SlurmAPI::getTasksPerNode();
		assert(ppn > 0);

		const size_t requiredHosts = (N + ppn - 1) / ppn;

		FatalErrorHandler::failIf(
			requiredHosts > _singleton->_partitionMaxNodes,
			"Require more hosts to expand to: ", N,
			" processes than partition: ", _singleton->_partitionMaxNodes
		);

		const int deltaHosts =  requiredHosts - _singleton->_hostInfoVector.size();

		if (deltaHosts <= 0) {
			return 0;
		}

		FatalErrorHandler::failIf(
			_singleton->_permitsExpansion == false,
			"Cant request: ", deltaHosts, " new hosts; permit_job_expansion is not set"
		);

		// Being strict the new allocation job will not last more than the current one, so it is
		// better to limit the timer assuming that in the worst case both will end together. In a
		// realistic application the time_limit is the default from the partition, which may be too
		// long; delaying the process in the queue. Time is in minutes for slurm.
		const time_t now = std::time(NULL);
		const uint32_t time_limit = std::difftime(_singleton->_end_time, now) / 60;

		// We require more hosts that what we currently have. So we make an allocation request. In
		// general this is just a request, we don't need to fail if there is an error or wait until
		// the allocation is done. That will be handled latter.
		job_desc_msg_t job;
		slurm_init_job_desc_msg(&job);

		char jobname[] = "temp_job ";

		job.name = jobname;
		job.time_limit = time_limit;
		job.min_nodes = deltaHosts;
		job.user_id = _singleton->_user_id;
		job.group_id = _singleton->_group_id;

		// Expand current job
		std::string dependency = "expand:" + std::to_string(_singleton->_slurmJobId);
		job.dependency = (char *) alloca(dependency.size() + 1);
		dependency.copy(job.dependency, dependency.size(), 0);

		// Request in same partition
		job.partition = (char *) alloca(_singleton->_partition.size() + 1);
		_singleton->_partition.copy(job.partition, _singleton->_partition.size(), 0);

		int rc = slurm_allocate_resources(&job, &_singleton->_slurmPendingMsgPtr);

		// TODO: This may become a warning and do some cleanup without failing. As this is just a
		// request not failure we can continue if the system fails to give us more resources. But at
		// the moment we have a controlled environment and we need to test it works.
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_allocate_resources returned: ", rc);

		return deltaHosts;
	}


	// > 0 success (number of new hosts); 0 job still in the queue; < 0 error (no pending request)
	static int checkAllocationRequest()
	{
		int rc;

		if (_singleton->_slurmPendingMsgPtr == nullptr) {
			// No pending allocation (no previous request or request just failed)
			return -1;
		}

		if (_singleton->_slurmPendingMsgPtr->node_cnt == 0) {
			// There is a pending allocation but it is not running yet; the struct should contain
			// the jobid of the queued process, but nothing else useful for us (at the moment).
			// We use that ID to check if the job is running now.

			const uint32_t job_id = _singleton->_slurmPendingMsgPtr->job_id;
			slurm_free_resource_allocation_response_msg(_singleton->_slurmPendingMsgPtr);

			rc = slurm_allocation_lookup(job_id, &_singleton->_slurmPendingMsgPtr);
			SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_allocation_lookup returned: ", rc);

			if (_singleton->_slurmPendingMsgPtr->node_cnt == 0) {
				// The allocation is still in the queue; not running yet
				return 0;
			}
		}

		// If we are here it means that the allocation has some nodes... so it is already running
		// and we can update the job.
		SlurmAPI::failIf(
			_singleton->_slurmPendingMsgPtr->node_list == NULL,
			"slurm_allocation didn't return a list."
		);

		std::vector<std::string> newHostnameVector =
			SlurmAPI::hostListToHostinfoVector(_singleton->_slurmPendingMsgPtr->node_list);

		assert(!newHostnameVector.empty());

#ifndef NDEBUG
		// Assert we don't receive a host we already have; that means that the exclusive flag is
		// not set in the slurm config which may be problematic to detect
		const std::vector<std::string>::iterator firstIt =
			std::find_first_of(
				newHostnameVector.begin(), newHostnameVector.end(),
				_singleton->_hostInfoVector.begin(), _singleton->_hostInfoVector.end(),
				[](const std::string &newHost, const SlurmHostInfo &infoInfo) -> bool {
					return newHost == infoInfo._hostname;
				}
			);

		FatalErrorHandler::failIf(
			firstIt != newHostnameVector.end(), "We received a host we already have.", *firstIt
		);
#endif // NDEBUG

		// Update the new job to have zero resources and kill
		job_desc_msg_t jobUpdate;
		slurm_init_job_desc_msg(&jobUpdate);
		jobUpdate.job_id = _singleton->_slurmPendingMsgPtr->job_id;
		jobUpdate.min_nodes = 0;
		rc = slurm_update_job(&jobUpdate);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "From slurm_update_job reducing new Slurm Job.");

		rc = slurm_kill_job(_singleton->_slurmPendingMsgPtr->job_id, 9, 0);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "From slurm_kill_job killing allocated Job.");

		// Update myself to take the new resources (nodes)
		slurm_init_job_desc_msg(&jobUpdate);
		jobUpdate.job_id = _singleton->_slurmJobId;
		jobUpdate.min_nodes = _singleton->_hostInfoVector.size() + newHostnameVector.size();
		rc = slurm_update_job(&jobUpdate);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "From slurm_update_job spawning Slurm Job.");

		// If we are here it means that the spawn succeded, so we add the new hosts to our list.
		for (const std::string &it : newHostnameVector) {
			_singleton->_hostInfoVector.emplace_back(it);
		}

		_singleton->updateEnvironment();

		// Now we cleanup the request
		slurm_free_resource_allocation_response_msg(_singleton->_slurmPendingMsgPtr);
		_singleton->_slurmPendingMsgPtr = nullptr;

		return newHostnameVector.size();
	}


	// This is a policy to spawn the new processes. Distributions may be implemented here. We
	// receive here the number of extra nodes we want to spawn and the function returns a vector of 
	// MessageSpawnHostInfo with information about how many process may be spawned in every
	// host. This is the vector that will be shared with the other nodes to iterate over them.
	static std::vector<MessageSpawnHostInfo> getSpawnHostInfoVector(size_t delta)
	{
		const size_t ppn = SlurmAPI::getTasksPerNode();
		std::vector<MessageSpawnHostInfo> ret;
		size_t pendingToSet = delta;

		for (SlurmHostInfo &it : _singleton->_hostInfoVector ) {

			if (ppn <= it._nProcesses) {
				// This should never ever happen.
				FatalErrorHandler::warnIf(
					ppn < it._nProcesses,
					"Host ", it._hostname, " has ", it._nProcesses, " and ppn is: ", ppn
				);
				continue;
			}

			const size_t spacesInHost = ppn - it._nProcesses;

			const size_t forThatHost = std::min(spacesInHost, pendingToSet);

			ret.emplace_back(it._hostname, forThatHost);
			pendingToSet -= forThatHost;

			// If all the requested hosts were allocated, then we can return immediately
			if (pendingToSet == 0) {
				return ret;
			}
		}

		// If we arrive here it means that there are not enough hosts.
		FatalErrorHandler::fail("There are not enough hosts to spawn all processes.");

		return ret;
	}

	static void releaseUnusedHosts()
	{
		// This works by creating a hostlist with the hosts containing a process and updating the
		// current job hostlist. After an update we need to update the environment to be consistent.
		hostlist_t hl = slurm_hostlist_create(NULL);

		for (SlurmHostInfo &it : _singleton->_hostInfoVector ) {
			if (it._nProcesses > 0) {
				slurm_hostlist_push_host(hl, it._hostname.c_str());
			}
		}

		// Remove duplicated and sort (we use it to assert sort, we shouldn't have duplication)
		slurm_hostlist_uniq(hl);

		job_desc_msg_t jobUpdate;
		slurm_init_job_desc_msg(&jobUpdate);
		jobUpdate.job_id = _singleton->_slurmJobId;
		jobUpdate.user_id = _singleton->_user_id;
		jobUpdate.group_id = _singleton->_group_id;
		jobUpdate.req_nodes = slurm_hostlist_ranged_string_malloc(hl); // this calls malloc

		int rc = slurm_update_job(&jobUpdate);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_update_job releasing Job returned:", rc);

		_singleton->updateEnvironment();

		free(jobUpdate.req_nodes);
		slurm_hostlist_destroy(hl);
	}

};

#endif // SLURMAPI_HPP

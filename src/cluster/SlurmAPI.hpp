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

	template<typename... TS>
	static inline void warnIf(bool cond, TS... reasonParts)
	{
		FatalErrorHandler::warnIf(cond, reasonParts..., "Slurm Warning:", slurm_strerror(errno));
	}


	const uint32_t _slurmJobId;

	// These two need to be updated together
	job_info_msg *_jobInfoMsg = nullptr;
	slurm_job_info_t *_jobInfo;

	// from environment
	size_t _tasksPerNode;

	// from partition info
	size_t _partitionMaxNodes;

	// from configuration info
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

	static std::vector<SlurmHostInfo> hostListToHostinfoVector(const std::string &hostString)
	{
		std::vector<SlurmHostInfo> hostInfoVector; // for return

		hostlist_t hostlist = slurm_hostlist_create(hostString.c_str());
		SlurmAPI::failIf(hostlist == NULL, "slurm_hostlist_create returned NULL");

		// Initialize the node list.
		char *host;
		while ((host = slurm_hostlist_shift(hostlist))) {
			hostInfoVector.emplace_back(host);
		}
		slurm_hostlist_destroy(hostlist);

		// This will use a move operator.
		return hostInfoVector;
	}

	void updateInternalJobInfo()
	{
		if (_jobInfoMsg) {
			slurm_free_job_info_msg(_jobInfoMsg);
		}

		int rc = slurm_load_job(&_jobInfoMsg, _slurmJobId, 0);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_load_job returned: ", rc);
		SlurmAPI::failIf(_jobInfoMsg == NULL, "slurm_load_job set jobInfoMsg to NULL");
		SlurmAPI::failIf(
			_jobInfoMsg->record_count != 1,
			"slurm_load_job returned ", _jobInfoMsg->record_count, " record_count entries"
		);

		_jobInfo = &_jobInfoMsg->job_array[0];

		// Update the nodes list and refresh the counts.
		_hostInfoVector = SlurmAPI::hostListToHostinfoVector(_jobInfo->nodes);
		FatalErrorHandler::failIf(_hostInfoVector.empty(), "Error nodelist_vector is empty.");

		// Now count the number of active processes per host.
		for (ClusterNode *it : ClusterManager::getClusterNodes()) {
			SlurmAPI::deltaProcessToHostname(it->getHostName(), 1);
		}
	}

	// Hopefully this is not needed, but in case the user accesses the slurm variables later it may
	// be useful (and correct) to keep these updated.
	// TODO: At the moment only master updated the environment. If needed we need to update all the
	// hosts environments... but lets keep it simple for now as we don't need that for now.
	void updateEnvironment()
	{
		if (getenv("SLURM_NODELIST")) {
			setenv("SLURM_NODELIST", _jobInfo->nodes, 1);
		}

		if (getenv("SLURM_JOB_NODELIST")) {
			setenv("SLURM_JOB_NODELIST", _jobInfo->nodes, 1);
		}

		std::string node_cnt = std::to_string(_jobInfo->num_nodes);

		if (getenv("SLURM_NNODES")) {
			setenv("SLURM_NNODES", node_cnt.c_str(), 1);
		}

		if (getenv("SLURM_JOB_NUM_NODES")) {
			setenv("SLURM_JOB_NUM_NODES", node_cnt.c_str(), 1);
		}

		if (getenv("SLURM_JOB_CPUS_PER_NODE")) {
			//to support more than one we need uint32_compressed_to_str from slurm code
			assert(_jobInfo->num_cpus % _jobInfo->num_nodes == 0);

			const size_t cpus_per_node = _jobInfo->num_cpus / _jobInfo->num_nodes;

			std::string job_cpus_per_node
				= std::to_string(cpus_per_node) + "(x" + std::to_string(_jobInfo->num_nodes) + ")";

			setenv("SLURM_JOB_CPUS_PER_NODE", job_cpus_per_node.c_str(), 1);
		}

		if (getenv("SLURM_TASKS_PER_NODE")) {
			// We have the information to update these, but in different moments.
			unsetenv("SLURM_NPROCS");
			unsetenv("SLURM_NTASKS");
		}
	}

public:
	SlurmAPI()
		: _slurmJobId(EnvironmentVariable<uint32_t>("SLURM_JOBID").getValue()),
		  _partitionMaxNodes(std::numeric_limits<size_t>::max())
	{
		int rc;
		this->updateInternalJobInfo();

		// Set job vars
		_tasksPerNode = _jobInfo->ntasks_per_node;
		FatalErrorHandler::failIf(_tasksPerNode == 0, "Couldn't get ntasks_per_node.");


		// Get the config information ======================
		slurm_conf_t  *slurmCtlConf = NULL;

		rc = slurm_load_ctl_conf ((time_t) NULL, &slurmCtlConf);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_load_ctl_conf returned: ", rc);
		SlurmAPI::failIf(slurmCtlConf == NULL, "slurm_load_ctl_conf set slurmConf to NULL");

		// Check that permit_expansion is enabled.
		const std::string schedParams = slurmCtlConf->sched_params;
		_permitsExpansion = (schedParams.find("permit_job_expansion") != std::string::npos);

		// Cleanup config info
		slurm_free_ctl_conf(slurmCtlConf);

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
			// Compare job partition with this one.
			if (strcmp(_singleton->_jobInfo->partition, info.name) == 0) {
				_partitionMaxNodes = info.total_nodes;
				break;
			}
		}

		// Cleanup partition info
		slurm_free_partition_info_msg(partitionInfoMsg);
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

		assert(_jobInfoMsg != nullptr);
		slurm_free_job_info_msg(_jobInfoMsg);
	}

	static void initialize()
	{
		assert(_singleton == nullptr);
		_singleton = new SlurmAPI();
		assert(_singleton != nullptr);
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
	// 0 no request needed; >0 request made (number of new hosts requested); <0 error
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
			"Can't request: ", deltaHosts, " new hosts; permit_job_expansion is not set"
		);

		// As we require more hosts that what we currently have. So we make an allocation
		// request. In general this is just a request, we don't need to fail if there is an error or
		// wait until the allocation is done. That will be handled latter.

		// Being strict the new allocation job will not last more than the current one, so it is
		// better to limit the timer assuming that in the worst case both will end together. In a
		// realistic application the time_limit is the default from the partition, which may be too
		// long; delaying the process in the queue. Time is in minutes for slurm.
		uint32_t time_limit = std::difftime(_singleton->_jobInfo->end_time, std::time(NULL)) / 60;

		job_desc_msg_t job;
		slurm_init_job_desc_msg(&job);

		char jobname[] = "temp_job ";

		job.name = jobname;
		job.time_limit = time_limit; // min
		job.min_nodes = deltaHosts;
		job.user_id = _singleton->_jobInfo->user_id;
		job.group_id = _singleton->_jobInfo->group_id;
		job.partition = _singleton->_jobInfo->partition;

		// Expand current job
		std::string dependency = "expand:" + std::to_string(_singleton->_slurmJobId);
		job.dependency = (char *) alloca(dependency.size() + 1);
		dependency.copy(job.dependency, dependency.size(), 0);

		// This is the most important call
		int rc = slurm_allocate_resources(&job, &_singleton->_slurmPendingMsgPtr);
		if (rc != SLURM_SUCCESS) {
			SlurmAPI::warnIf(true, "slurm_allocate_resources returned: ", rc);
			return -1;
		}

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
			// the jobid of the queued process, but nothing else useful for us (at the moment).  We
			// use that ID to check if the job is running now.

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

		const int nNewNodes = _singleton->_slurmPendingMsgPtr->node_cnt;
		assert(nNewNodes > 0);

		// Update the new job to have zero resources and kill
		job_desc_msg_t jobUpdate;
		slurm_init_job_desc_msg(&jobUpdate);
		jobUpdate.job_id = _singleton->_slurmPendingMsgPtr->job_id;
		jobUpdate.min_nodes = 0;
		rc = slurm_update_job(&jobUpdate);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "From slurm_update_job reducing new Slurm Job.");

		rc = slurm_kill_job(_singleton->_slurmPendingMsgPtr->job_id, 9, 0);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "From slurm_kill_job killing allocated Job.");

		// Update myself to take the new resources (nodes) for myself
		slurm_init_job_desc_msg(&jobUpdate);
		jobUpdate.job_id = _singleton->_slurmJobId;
		jobUpdate.min_nodes = _singleton->_hostInfoVector.size() + nNewNodes;
		rc = slurm_update_job(&jobUpdate);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "From slurm_update_job spawning Slurm Job.");

		// If we are here it means that the spawn succeded, so we add the new hosts to our list and
		// recount.
		_singleton->updateInternalJobInfo();
		_singleton->updateEnvironment();

		// Now we cleanup the request
		slurm_free_resource_allocation_response_msg(_singleton->_slurmPendingMsgPtr);
		_singleton->_slurmPendingMsgPtr = nullptr;

		return nNewNodes;
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

		return std::vector<MessageSpawnHostInfo>();
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
		jobUpdate.req_nodes = slurm_hostlist_ranged_string_malloc(hl); // this calls malloc

		int rc = slurm_update_job(&jobUpdate);
		SlurmAPI::failIf(rc != SLURM_SUCCESS, "slurm_update_job releasing Job returned:", rc);

		_singleton->updateInternalJobInfo();
		_singleton->updateEnvironment();

		free(jobUpdate.req_nodes);
		slurm_hostlist_destroy(hl);
	}

};

#endif // SLURMAPI_HPP

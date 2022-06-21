/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef SLURMAPI_HPP
#define SLURMAPI_HPP

#include <cstdlib>
#include <string>
#include <unistd.h>
#include <slurm/slurm.h>

// Part of this information may be handled by the Hardware info too; but we need to get it from
// slurm in order to distribute the work properly.
class SlurmAPI {

	struct SlurmHostInfo {
		std::string hostname;
		size_t nProcesses;

		void changeProcesses(int delta)
		{
			const int expected = nProcesses + delta;
			assert(expected >= 0);
			assert((size_t)expected <= getProcessesPerNode());
			nProcesses = expected;
		}

		friend std::ostream &operator<<(std::ostream &out, const SlurmHostInfo &info)
		{
			out << "Host:" << info.hostname << "processes:" << info.nProcesses;
			return out;
		}
	};

	std::vector<SlurmHostInfo> _hostInfoVector;
	const size_t _cpusPerTask, _cpusPerNode;

	static SlurmAPI *_singleton;
private:

	std::vector<SlurmHostInfo>::iterator getHostinfoByHostname(std::string hostname)
	{
		std::vector<SlurmHostInfo>::iterator hostInfoIt
			= std::find_if_not(   // The only one in C++-11
				_hostInfoVector.begin(),
				_hostInfoVector.end(),
				[&](const SlurmHostInfo &hostInfo) -> bool {
					return (hostInfo.hostname != hostname);
				}
			);

		FatalErrorHandler::failIf(
			hostInfoIt == _hostInfoVector.end(),
			"Hostname: ", hostname, " is not in the HostnameList."
		);

		return hostInfoIt;
	}

public:
	SlurmAPI()
		: _cpusPerTask(EnvironmentVariable<size_t>("SLURM_CPUS_PER_TASK").getValue()),
		  _cpusPerNode(EnvironmentVariable<size_t>("SLURM_CPUS_ON_NODE").getValue())
	{
		assert(_cpusPerTask != _cpusPerNode);

		const std::string nodelist = EnvironmentVariable<std::string>("SLURM_NODELIST").getValue();
		FatalErrorHandler::failIf(nodelist.empty(), "Couldn't get SLURM_NODELIST.");

		FatalErrorHandler::failIf(_cpusPerTask == 0, "Couldn't get SLURM_CPUS_PER_TASK.");
		FatalErrorHandler::failIf(_cpusPerNode == 0, "Couldn't get SLURM_CPUS_ON_NODE.");
		FatalErrorHandler::failIf(_cpusPerNode < _cpusPerTask,
			"Require more than one process per node.");

		hostlist_t hostlist = slurm_hostlist_create(nodelist.c_str());
		FatalErrorHandler::failIf(hostlist == NULL,
			"slurm_hostlist_create returned NULL", std::string(slurm_strerror(slurm_get_errno())));

		// Initialize the node list.
		char *host;
		while ((host = slurm_hostlist_shift(hostlist))) {
			SlurmHostInfo info = {.hostname = host, .nProcesses = 0};
			_hostInfoVector.push_back(info);
		}
		slurm_hostlist_destroy(hostlist);

		FatalErrorHandler::failIf(_hostInfoVector.empty(), "Error nodelist_vector is empty.");
	}

	static void initialize(std::vector<ClusterNode *> &mpiNodes)
	{
		assert(_singleton == nullptr);
		_singleton = new SlurmAPI();
		assert(_singleton != nullptr);

		// Move the master Hostname to the first position.
		// TODO: Not hardcode the 0, but use the master_index variable from cluster manager...
		if (mpiNodes[0]->getHostName() != _singleton->_hostInfoVector[0].hostname) {
			std::rotate(
				_singleton->_hostInfoVector.begin(),
				_singleton->getHostinfoByHostname(mpiNodes[0]->getHostName()),
				_singleton->_hostInfoVector.end()
			);
		}

		// TODO: If the number of hosts grows too much this search is complexity N, it may be
		// useful to use a temporal map to reduce the search in getHostinfoByHostname to logN
		for (ClusterNode *it : mpiNodes) {
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

	static const std::vector<SlurmHostInfo> &getHostInfoVector()
	{
		assert(_singleton != nullptr);
		return _singleton->_hostInfoVector;
	}

	static size_t getCpusPerTask()
	{
		assert(_singleton != nullptr);
		return _singleton->_cpusPerTask;
	}

	static size_t getCpusPerNode()
	{
		assert(_singleton != nullptr);
		return _singleton->_cpusPerNode;
	}

	static size_t getProcessesPerNode()
	{
		assert(_singleton != nullptr);
		assert(_singleton->_cpusPerNode >= _singleton->_cpusPerTask);
		return _singleton->_cpusPerNode / _singleton->_cpusPerTask;
	}

	static void deltaProcessToHostname(std::string hostname, int delta)
	{
		assert(_singleton != nullptr);
		std::vector<SlurmHostInfo>::iterator info = _singleton->getHostinfoByHostname(hostname);
		info->changeProcesses(delta);
	}

};

#endif // SLURMAPI_HPP

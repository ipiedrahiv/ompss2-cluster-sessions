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
		size_t nprocesses;
	};

	std::vector<SlurmHostInfo> _nodelist_vector;
	const size_t _cpus_per_task, _cpus_per_node;

	static SlurmAPI *_singleton;

public:
	SlurmAPI()
		: _cpus_per_task(EnvironmentVariable<size_t>("SLURM_CPUS_PER_TASK").getValue()),
		  _cpus_per_node(EnvironmentVariable<size_t>("SLURM_CPUS_ON_NODE").getValue())
	{
		assert(_cpus_per_task != _cpus_per_node);

		const std::string nodelist = EnvironmentVariable<std::string>("SLURM_NODELIST").getValue();
		FatalErrorHandler::failIf(nodelist.empty(), "Couldn't get SLURM_NODELIST.");

		FatalErrorHandler::failIf(_cpus_per_task == 0, "Couldn't get SLURM_CPUS_PER_TASK.");
		FatalErrorHandler::failIf(_cpus_per_node == 0, "Couldn't get SLURM_CPUS_ON_NODE.");
		FatalErrorHandler::failIf(_cpus_per_node < _cpus_per_task,
			"Require more than one process per node.");

		hostlist_t hostlist = slurm_hostlist_create(nodelist.c_str());
		FatalErrorHandler::failIf(hostlist == NULL,
			"slurm_hostlist_create returned NULL", std::string(slurm_strerror(slurm_get_errno())));

		// Initialize the node list.
		char *host;
		while ((host = slurm_hostlist_shift(hostlist))) {
			SlurmHostInfo info = {.hostname = host, .nprocesses = 0};
			_nodelist_vector.push_back(info);
		}
		slurm_hostlist_destroy(hostlist);

		FatalErrorHandler::failIf(_nodelist_vector.empty(), "Error nodelist_vector is empty.");

#ifndef NDEBUG
		// Here the code can be to reorder the nodes on demand and check the nodes in use.
		char this_hostname[HOST_NAME_MAX];

		if (gethostname(this_hostname, HOST_NAME_MAX) == 0) {
			// TODO: Instead of failing we should move this_hostname to the first position in the
			// list As this function is used only on master, a more complete approach must be to use
			// the SLURM_JOB_NODELIST environment variable and remove the elements.
			FatalErrorHandler::failIf(
				_nodelist_vector[0].hostname != std::string(this_hostname),
				"The master's hostname is not the first in the list.");
		} else {
			FatalErrorHandler::warn("Couldn't get hostname to assert that the list is correct.");
		}
#endif // NDEBUG
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

	static const std::vector<SlurmHostInfo> &getHostnameVector()
	{
		assert(_singleton != nullptr);
		return _singleton->_nodelist_vector;
	}

	static size_t getCpusPerTask()
	{
		assert(_singleton != nullptr);
		return _singleton->_cpus_per_task;
	}

	static size_t getCpusPerNode()
	{
		assert(_singleton != nullptr);
		return _singleton->_cpus_per_node;
	}

	static size_t getProcessesPerNode()
	{
		assert(_singleton != nullptr);
		assert(_singleton->_cpus_per_node >= _singleton->_cpus_per_task);
		return _singleton->_cpus_per_node / _singleton->_cpus_per_task;
	}
};

#endif // SLURMAPI_HPP

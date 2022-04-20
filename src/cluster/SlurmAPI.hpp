/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef SLURMAPI_HPP
#define SLURMAPI_HPP

#include <string>
#include <unistd.h>
#include <slurm/slurm.h>

class clusterHostManager {

public:
	static std::vector<std::string> getNodeList()
	{
		// Only master should call this. Is not enforced, but current design assumes that.
		assert(ClusterManager::isMasterNode());

		std::vector<std::string> hostname_list;
		char *hostnames = getenv("SLURM_JOB_NODELIST");
		if (hostnames == NULL) {
			return hostname_list;
		}

		hostlist_t hostlist = slurm_hostlist_create(hostnames);
		if (hostlist == NULL) {
			return hostname_list;
		}

		char *host;
		while ( (host = slurm_hostlist_shift(hostlist)) ) {
			hostname_list.push_back(host);
		}

		slurm_hostlist_destroy(hostlist) ;

		assert(!hostname_list.empty());

#ifndef NDEBUG
		char this_hostname[HOST_NAME_MAX];
		const int fail = gethostname(this_hostname, HOST_NAME_MAX);

		if (fail == 0) {
			const int comparison = strncmp(this_hostname, hostname_list[0].c_str(), HOST_NAME_MAX);

			FatalErrorHandler::failIf(comparison != 0,
				"The master's hostname is not the first in the list.") ;
		} else {
			FatalErrorHandler::warn("Couldn't get hostname to assert that the list is correct.");
		}


		int strncmp(const char *s1, const char *s2, size_t n);
#endif // NDEBUG

		return hostname_list;
	}

};

#endif // SLURMAPI_HPP

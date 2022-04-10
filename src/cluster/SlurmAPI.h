/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef SLURMAPI_HPP
#define SLURMAPI_HPP

#include <string>
#include <slurm/slurm.h>

std::vector<std::string> getNodeList()
{
	std::vector<std::string> ret;
	char *hostnames = getenv("SLURM_JOB_NODELIST");
	if (hostnames == NULL) {
		return ret;
	}

	hostlist_t hostlist = slurm_hostlist_create(hostnames);
	if (hostlist == NULL) {
		return ret;
	}

	char *host;
	while ( (host = slurm_hostlist_shift(hostlist)) ) {
		ret.push_back(host);
	}

	slurm_hostlist_destroy(hostlist) ;
	return ret;
}


#endif

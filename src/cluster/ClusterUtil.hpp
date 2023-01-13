/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/


#ifndef CLUSTERUTIL_H
#define CLUSTERUTIL_H

#include <execinfo.h> // for backtrace
#include <dlfcn.h>    // for dladdr
#include <cxxabi.h>   // for __cxa_demangle

#include <cstdio>
#include <cstdlib>
#include <string>
#include <sstream>
#include <limits>
#include <cassert>

#include <ClusterManager.hpp>

#define bufsize 1024
#define stacksize 128

#ifdef NDEBUG
#define clusterAssert(expr) (static_cast<void>(0))
#else // NDEBUG

/* This prints an "Assertion failed" message and aborts.  */
inline void __cluster_assert_fail(
	const char *__assertion,
	const char *__file,
	unsigned int __line,
	const char *__function
) {
	// This is a small hack to attach the node idx to the filename.
	const int node = ClusterManager::isInitialized()
		? ClusterManager::getCurrentClusterNode()->getIndex()
		: 0;

	char tmp[bufsize];
	snprintf(tmp, bufsize, "Node(%d) %s", node, __file);

	__assert_fail(__assertion, tmp, __line, __function);
}

#define clusterAssert(expr)												\
	(static_cast <bool> (expr)											\
		? void (0)														\
		: __cluster_assert_fail(#expr, __FILE__, __LINE__, __ASSERT_FUNCTION))
#endif // NDEBUG

// In the verbose instrumentation there is the logMessage, but functional only when verbose
#define clusteFprintf(STREAM, FORMAT, ...)								\
	fprintf(STREAM,  "# Node:%s " FORMAT,				     	 \
			ClusterManager::getCurrentClusterNode()->getInstrumentationName().c_str(), \
			##__VA_ARGS__)

// Print Node [Rest]
#define clusterPrintf(FORMAT, ...) clusteFprintf(stdout, FORMAT, ##__VA_ARGS__)

// Print Node file:line [Rest]
#define clusterVPrintf(FORMAT, ...)										\
	clusteFprintf(stdout, "%s:%d " FORMAT, __FILE__, __LINE__,__VA_ARGS__)

#define clusterCout														\
	std::cout << "# Node:" << ClusterManager::getCurrentClusterNode()->getInstrumentationName()  << " "

// This function produces a stack backtrace with demangled function & method names.
inline std::string clusterBacktrace()
{
	void *callstack[stacksize];
	int nFrames = backtrace(callstack, stacksize);
	char **symbols = backtrace_symbols(callstack, nFrames);

	std::ostringstream trace_buf;
	char buffer[bufsize];

	for (int i = 1; i < nFrames; i++) { // Start in one to skip this function
		Dl_info info;

		if (dladdr(callstack[i], &info) != 0
			&& info.dli_sname != NULL
			&& info.dli_saddr != NULL) {

			char *demangled = NULL;
			int status = -1;

			if (info.dli_sname[0] == '_') {
				demangled = abi::__cxa_demangle(info.dli_sname, NULL, 0, &status);
			}

			snprintf(buffer, bufsize, "\t%-3d %*p %s + %zd\n",
				i,
				int(2 + sizeof(void*) * 2),
				callstack[i],
				status == 0 ? demangled : info.dli_sname == 0 ? symbols[i] : info.dli_sname,
				(char *)callstack[i] - (char *)info.dli_saddr
			);

			free(demangled);
		} else {
			snprintf(buffer, bufsize, "\t%-3d %*p %s\n",
				i,
				int(2 + sizeof(void*) * 2),
				callstack[i],
				symbols[i]
			);
		}
		trace_buf << buffer;
	}

	free(symbols);

	if (nFrames >= stacksize) {
		trace_buf << "[truncated]\n";
	}
	return trace_buf.str();
}

inline struct timespec clusterDiffTime(const struct timespec *t1, const struct timespec *t2)
{
	assert(t2->tv_sec >= t1->tv_sec);
	struct timespec ret;

	if (t2->tv_nsec < t1->tv_nsec) {
		ret.tv_nsec = 1000000000L + t2->tv_nsec - t1->tv_nsec;
		ret.tv_sec = (t2->tv_sec - 1 - t1->tv_sec);
	} else {
		ret.tv_nsec = (t2->tv_nsec - t1->tv_nsec);
		ret.tv_sec = (t2->tv_sec - t1->tv_sec);
	}
	return ret;
}


#endif /* CLUSTERUTIL_H */

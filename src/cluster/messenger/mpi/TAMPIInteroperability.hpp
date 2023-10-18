/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef TAMPI_INTEROPERABILITY_HPP
#define TAMPI_INTEROPERABILITY_HPP

#if HAVE_TAMPI
#include <TAMPI.h>

inline void disableTAMPITaskAwareness()
{
	__attribute__((unused)) int ret;
#ifndef NDEBUG
	int value;
	ret = TAMPI_Property_get(TAMPI_PROPERTY_THREAD_TASKAWARE, &value);
	assert(value == 1);
	assert(ret == MPI_SUCCESS);
#endif
	ret = TAMPI_Property_set(TAMPI_PROPERTY_THREAD_TASKAWARE, 0);
	assert(ret == MPI_SUCCESS);
}

inline void enableTAMPITaskAwareness()
{
	__attribute__((unused)) int ret;
#ifndef NDEBUG
	int value;
	ret = TAMPI_Property_get(TAMPI_PROPERTY_THREAD_TASKAWARE, &value);
	assert(value == 0);
	assert(ret == MPI_SUCCESS);
#endif
	ret = TAMPI_Property_set(TAMPI_PROPERTY_THREAD_TASKAWARE, 1);
	assert(ret == MPI_SUCCESS);
}

#else

inline void disableTAMPITaskAwareness()
{
}

inline void enableTAMPITaskAwareness()
{
}
#endif

#endif /* TAMPI_INTEROPERABILITY_HPP */

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef NANOS6_CLUSTER_H
#define NANOS6_CLUSTER_H

#include <stddef.h>

#include "major.h"

#pragma GCC visibility push(default)

// NOTE: The full version depends also on nanos6_major_api
//       That is:   nanos6_major_api . nanos6_cluster_device_api
enum nanos6_cluster_api_t { nanos6_cluster_api = 4 };

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
	//! Equally-partitioned distribution among all memory nodes
	nanos6_equpart_distribution = 0,

	//! Block distribution
	nanos6_block_distribution,

	//! Block cyclic distribution
	nanos6_cyclic_distribution
} nanos6_data_distribution_t;

typedef enum {
	//! No wait clause (cannot currently be specified with pragma clause)
	nanos6_no_wait = 0,

	//! Autowait (default for offloaded tasks if disable_autowait=false)
	nanos6_autowait,

	//! Wait (same as wait clause)
	nanos6_wait
} nanos6_early_release_t;

typedef enum {
	nanos6_spawn_default = 0,   // use the value configured as default
	nanos6_spawn_by_group,      // complete group relying on srun
	nanos6_spawn_by_host,       // Spawn by step size process per host (recommended)
	nanos6_spawn_by_one         // Spawn with granularity one
} nanos6_spawn_policy_t;

typedef enum {
	nanos6_spawn_lazy = 0,      // use lazy transfer during shrink process.
	nanos6_spawn_eager          // use eager transfer and move everything to home
} nanos6_shrink_transfer_policy_t;


typedef struct nanos6_cluster_info {
	nanos6_spawn_policy_t spawn_policy;
	nanos6_shrink_transfer_policy_t transfer_policy;
	unsigned long cluster_num_min_nodes;
	unsigned long cluster_num_max_nodes;
	unsigned long cluster_num_nodes;
	int malleability_enabled;

	int num_message_handler_workers;
	int namespace_enabled;
	int disable_remote_connect;
	int disable_autowait;
	int eager_weak_fetch;
	int eager_send;
	int merge_release_and_finish;
	int reserved_leader_thread;
	int group_messages_enabled;
	int write_id_enabled;

	void *virtual_region_start;
	unsigned long virtual_region_size;

	void *distributed_region_start;
	unsigned long distributed_region_size;
} nanos6_cluster_info_t;

//! Like nanos6_library_mode_init but requires argc and argv. It is intended to be used in cluster
//! mode only
__attribute__ ((used)) char const * nanos6_library_mode_init_cluster(int argc, char **argv);


//! \brief Determine whether we are on cluster mode
//!
//! \returns true if we are on cluster mode
int nanos6_in_cluster_mode(void);

//! \brief Check if it is the master irank (compatibility with normal API).
int nanos6_is_master_node(void);

//! \brief Get irank (compatibility with normal API)
int nanos6_get_cluster_node_id(void);

//! \brief Get the number of iranks (compatibility with normal API)
int nanos6_get_num_cluster_nodes(void);

//! \brief Get the id of the current physical cluster node
//!
//! \returns the id of the current physical cluster node

int nanos6_get_cluster_physical_node_id(void);

//! \brief Get the number of physical cluster nodes
//!
//! \returns the number of physical cluster nodes
int nanos6_get_num_cluster_physical_nodes(void);

//! \brief Determine whether current node is the master internal rank
//!
//! \returns true if the current node is the master internal rank
//! otherwise it returns false
int nanos6_is_master_irank(void);

//! \brief Get the id of the internal rank in this apprank
//!
//! \returns the id of the internal rank in this apprank
int nanos6_get_cluster_irank_id(void);

//! \brief Get the number of internal ranks in this apprank
//!
//! \returns the number of internal ranks in this apprank
int nanos6_get_num_cluster_iranks(void);

//! \brief Get if namespace propagation is enables
//!
//! \returns namespace propagation is enables.
int nanos6_get_namespace_is_enabled(void);

//! \brief Allocate distributed memory
//!
//! Distributed memory is a clsuter type of memory that can only be
//! accessed from within a task.
//!
//! \param[in] size is the size (in bytes) of distributed memory to allocate
//! \param[in] policy is the data distribution policy based on which we will
//!            distribute data across cluster nodes
//! \param[in] num_dimensions is the number of dimensions across which the data
//!            will be distributed
//! \param[in] dimensions is an array of num_dimensions elements, which contains
//!            the size of every distribution dimension
//!
//! \returns a pointer to distributed memory
void *nanos6_dmalloc(size_t size, nanos6_data_distribution_t policy, size_t num_dimensions, size_t *dimensions);

//! \brief deallocate a distributed array
//
//! \param[in] ptr is a pointer to memory previously allocated with nanos6_dmalloc
//! \param[in] size is the size of the distributed memory allocation
void nanos6_dfree(void *ptr, size_t size);

//! \brief deallocate a distributed array.
//
//! \param[in] ptr is a pointer to memory previously allocated with nanos6_dmalloc
void nanos6_dfree1(void *ptr);

//! \brief Allocate local memory
//!
//! \param[in] size is the size (in bytes) of local memory to allocate
//!
//! \returns a pointer to local memory
void *nanos6_lmalloc(size_t size);

//! \brief Deallocate a local array
//!
//! \param[in] ptr is a pointer to memory previously allocated with nanos6_lmalloc
//! \param[in] size is the size of the local memory allocation
void nanos6_lfree(void *ptr, size_t size);

//! \brief Deallocate a local array. It must be called on the node that did the lmalloc.
//!
//! \param[in] ptr is a pointer to memory previously allocated with nanos6_lmalloc
void nanos6_lfree1(void *ptr);

//! \brief Set early release.  Temporary until pragma clauses defined
//!
//! \param[in] early_release controls early release
void nanos6_set_early_release(nanos6_early_release_t early_release);

//! \brief Get application communicator
//!
//! \returns the communicator that can be used by the application
int nanos6_get_app_communicator(void *);
//! \brief Resize the cluster world size.
//!
//! \param[in] delta Number of nodes to add (>0) or remove (<0) from the world.
//! \returns Same value than delta on success.
int nanos6_cluster_resize(int delta);

int nanos6_cluster_resize_with_policy(int delta, nanos6_spawn_policy_t policy);

//! \brief Create or recover checkpoint info
//!
//! \param[in] start Address pointer to start
//! \param[in] nbytes Obvious
//! \param[in] process Id if the previous process to recover
//! \param[in] id Variable id must be unique inside the program for C&R
//! \param[in] checkpoint true to save, false to recover.
int nanos6_serialize(void *start, size_t nbytes, size_t process, size_t id, int checkpoint);

void nanos6_fail(const char message[]);

//! Get the cluster info in a struct... see struct definition before.
int nanos6_get_cluster_info(nanos6_cluster_info_t *info);

#ifdef __cplusplus
}
#endif

#pragma GCC visibility pop

#endif /* NANOS6_CLUSTER_H */

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#include "resolve.h"

#pragma GCC visibility push(default)

int nanos6_in_cluster_mode(void)
{
	typedef int nanos6_in_cluster_mode_t(void);

	static nanos6_in_cluster_mode_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_in_cluster_mode_t *) _nanos6_resolve_symbol(
				"nanos6_in_cluster_mode", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_is_master_node(void)
{
	typedef int nanos6_is_master_node_t(void);

	static nanos6_is_master_node_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_is_master_node_t *) _nanos6_resolve_symbol(
				"nanos6_is_master_node", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_cluster_node_id(void)
{
	typedef int nanos6_get_cluster_node_id_t(void);

	static nanos6_get_cluster_node_id_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_cluster_node_id_t *) _nanos6_resolve_symbol(
				"nanos6_get_cluster_node_id", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_num_cluster_nodes(void)
{
	typedef int nanos6_get_num_cluster_nodes_t(void);

	static nanos6_get_num_cluster_nodes_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_num_cluster_nodes_t *) _nanos6_resolve_symbol(
				"nanos6_get_num_cluster_nodes", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_is_master_node(void)
{
	typedef int nanos6_is_master_node_t(void);

	static nanos6_is_master_node_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_is_master_node_t *) _nanos6_resolve_symbol(
				"nanos6_is_master_node", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_cluster_node_id(void)
{
	typedef int nanos6_get_cluster_node_id_t(void);

	static nanos6_get_cluster_node_id_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_cluster_node_id_t *) _nanos6_resolve_symbol(
				"nanos6_get_cluster_node_id", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_num_cluster_nodes(void)
{
	typedef int nanos6_get_num_cluster_nodes_t(void);

	static nanos6_get_num_cluster_nodes_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_num_cluster_nodes_t *) _nanos6_resolve_symbol(
				"nanos6_get_num_cluster_nodes", "cluster", NULL);
	}

	return (*symbol)();
}


int nanos6_is_master_irank(void)
{
	typedef int nanos6_is_master_irank_t(void);

	static nanos6_is_master_irank_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_is_master_irank_t *) _nanos6_resolve_symbol(
				"nanos6_is_master_irank", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_cluster_physical_node_id(void)
{
	typedef int nanos6_get_cluster_physical_node_id_t(void);

	static nanos6_get_cluster_physical_node_id_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_cluster_physical_node_id_t *) _nanos6_resolve_symbol(
				"nanos6_get_cluster_physical_node_id", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_num_cluster_physical_nodes(void)
{
	typedef int nanos6_get_num_cluster_physical_nodes_t(void);

	static nanos6_get_num_cluster_physical_nodes_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_num_cluster_physical_nodes_t *) _nanos6_resolve_symbol(
				"nanos6_get_num_cluster_physical_nodes", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_cluster_irank_id(void)
{
	typedef int nanos6_get_cluster_irank_id_t(void);

	static nanos6_get_cluster_irank_id_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_cluster_irank_id_t *) _nanos6_resolve_symbol(
				"nanos6_get_cluster_irank_id", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_num_cluster_iranks(void)
{
	typedef int nanos6_get_num_cluster_iranks_t(void);

	static nanos6_get_num_cluster_iranks_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_num_cluster_iranks_t *) _nanos6_resolve_symbol(
				"nanos6_get_num_cluster_iranks", "cluster", NULL);
	}

	return (*symbol)();
}

int nanos6_get_namespace_is_enabled(void)
{
	typedef int nanos6_get_namespace_is_enabled_t(void);

	static nanos6_get_namespace_is_enabled_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_namespace_is_enabled_t *) _nanos6_resolve_symbol(
				"nanos6_get_namespace_is_enabled_t", "cluster", NULL);
	}

	return (*symbol)();
}


void *nanos6_dmalloc(size_t size, nanos6_data_distribution_t policy,
		size_t num_dimensions, size_t *dimensions)
{
	typedef void *nanos6_dmalloc_t(size_t, nanos6_data_distribution_t,
			size_t, size_t *);

	static nanos6_dmalloc_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_dmalloc_t *) _nanos6_resolve_symbol(
				"nanos6_dmalloc", "cluster", NULL);
	}

	return (*symbol)(size, policy, num_dimensions, dimensions);
}

void nanos6_dfree(void *ptr, size_t size)
{
	typedef void nanos6_dfree_t(void *, size_t);

	static nanos6_dfree_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_dfree_t *) _nanos6_resolve_symbol(
				"nanos6_dfree", "cluster", NULL);
	}

	(*symbol)(ptr, size);
}

void nanos6_dfree1(void *ptr)
{
	typedef void nanos6_dfree1_t(void *);

	static nanos6_dfree1_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_dfree1_t *) _nanos6_resolve_symbol(
				"nanos6_dfree1", "cluster", NULL);
	}

	(*symbol)(ptr);
}

void *nanos6_lmalloc(size_t size)
{
	typedef void *nanos6_lmalloc_t(size_t);

	static nanos6_lmalloc_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_lmalloc_t *) _nanos6_resolve_symbol(
				"nanos6_lmalloc", "cluster", NULL);
	}

	return (*symbol)(size);
}

void nanos6_lfree(void *ptr, size_t size)
{
	typedef void nanos6_lfree_t(void *, size_t);

	static nanos6_lfree_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_lfree_t *) _nanos6_resolve_symbol(
				"nanos6_lfree", "cluster", NULL);
	}

	(*symbol)(ptr, size);
}

void nanos6_lfree1(void *ptr)
{
	typedef void nanos6_lfree1_t(void *);

	static nanos6_lfree1_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_lfree1_t *) _nanos6_resolve_symbol(
				"nanos6_lfree1", "cluster", NULL);
	}

	(*symbol)(ptr);
}

void nanos6_set_early_release(nanos6_early_release_t early_release)
{
	typedef void nanos6_set_early_release_t(nanos6_early_release_t);

	static nanos6_set_early_release_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_set_early_release_t *) _nanos6_resolve_symbol(
				"nanos6_set_early_release", "cluster", NULL);
	}

	(*symbol)(early_release);
}

int nanos6_get_app_communicator(void *appcomm)
{
	typedef int nanos6_get_app_communicator_t(void *);

	static nanos6_get_app_communicator_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_app_communicator_t *) _nanos6_resolve_symbol(
				"nanos6_get_app_communicator", "cluster", NULL);
	}

	return (*symbol)(appcomm);
}

int nanos6_cluster_resize(int delta)
{
	typedef int nanos6_cluster_resize_t(int);

	static nanos6_cluster_resize_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_cluster_resize_t *) _nanos6_resolve_symbol(
			"nanos6_cluster_resize", "cluster", NULL);
	}

	return (*symbol)(delta);
}

int nanos6_cluster_resize_with_policy(int delta, nanos6_spawn_policy_t policy)
{
	typedef int nanos6_cluster_resize_with_policy_t(int, nanos6_spawn_policy_t);

	static nanos6_cluster_resize_with_policy_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_cluster_resize_with_policy_t *) _nanos6_resolve_symbol(
			"nanos6_cluster_resize_with_policy", "cluster", NULL);
	}

	return (*symbol)(delta, policy);
}

int nanos6_serialize(void *start, size_t nbytes, size_t process, size_t id, int checkpoint)
{
	typedef int nanos6_serialize_t(void *, size_t, size_t, size_t, int);

	static nanos6_serialize_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_serialize_t *) _nanos6_resolve_symbol("nanos6_serialize", "cluster", NULL);
	}

	return (*symbol)(start, nbytes, process, id, checkpoint);
}

int nanos6_fail(const char message[])
{
	typedef int nanos6_fail_t(const char *);

	static nanos6_fail_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_fail_t *) _nanos6_resolve_symbol("nanos6_fail", "cluster", NULL);
	}

	(*symbol)(message);
}

int nanos6_get_cluster_info(nanos6_cluster_info_t *info)
{
	typedef int nanos6_get_cluster_info_t(nanos6_cluster_info_t *info);

	static nanos6_get_cluster_info_t *symbol = NULL;
	if (__builtin_expect(symbol == NULL, 0)) {
		symbol = (nanos6_get_cluster_info_t *) _nanos6_resolve_symbol(
			"nanos6_get_cluster_info", "cluster", NULL);
	}

	(*symbol)(info);
}

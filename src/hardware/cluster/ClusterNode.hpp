/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef CLUSTER_NODE_HPP
#define CLUSTER_NODE_HPP

#include "hardware/places/ComputePlace.hpp"
#include "InstrumentCluster.hpp"

#include <ClusterMemoryNode.hpp>

class ClusterNode : public ComputePlace {
private:
	//! MemoryPlace associated with this cluster node
	ClusterMemoryNode *_memoryNode;

	//! This is the index of the node related to the
	//! communication layer
	int _commIndex;

	//! Name of the node for instrumentation
	std::string _instrumentationName;

	//! For Extrae tracing of hybrid clusters+DLB
	int _instrumentationRank;

	//! The number of cores allocated (global) or wanted right now (local)
	int _numAllocCores;

	//! Number of active cores available to this instance
	int _numEnabledCores;

	//! The number of ready tasks reported in the utilization<n> file
	size_t _numReadyTasks;

	//! The number of busy cores reported in the utilization<n> file
	float _numBusyCores;

	std::string _hostname;


	std::atomic<int> _numOffloadedTasks; // offloaded by us

public:
	ClusterNode(int index, int commIndex, int apprankNum, bool inHybridMode, int instrumentationRank)
		: ComputePlace(index, nanos6_device_t::nanos6_cluster_device),
		_memoryNode(new ClusterMemoryNode(index, commIndex)),
		_commIndex(commIndex), _instrumentationRank(instrumentationRank), _numAllocCores(0), _numEnabledCores(0),
		_numOffloadedTasks(0)
	{
		assert(_memoryNode != nullptr);
		assert (_commIndex >= 0);

		//! Set the instrumentation name
		std::stringstream ss;
		if (inHybridMode) {
			ss << "a" << apprankNum << "r" << index;
		} else {
			ss << index;
		}

		_instrumentationName = ss.str();
	}

	~ClusterNode()
	{
		assert(_memoryNode != nullptr);
		delete _memoryNode;
	}

	//! \brief Get the MemoryNode of the cluster node
	inline ClusterMemoryNode *getMemoryNode() const
	{
		assert(_memoryNode != nullptr);
		return _memoryNode;
	}

	//! \brief Get the communicator index of the ClusterNode
	inline int getCommIndex() const
	{
		assert (_commIndex >= 0);
		return _commIndex;
	}

	//! \brief Get the instrumentation name
	std::string &getInstrumentationName()
	{
		return _instrumentationName;
	}

	inline void setCurrentAllocCores(int numAllocCores)
	{
		_numAllocCores = numAllocCores;
	}

	inline int getCurrentAllocCores() const
	{
		return _numAllocCores;
	}

	inline void setCurrentEnabledCores(int numEnabledCores)
	{
		_numEnabledCores = numEnabledCores;
	}

	inline int getCurrentEnabledCores() const
	{
		return _numEnabledCores;
	}

	inline void setCurrentReadyTasks(int numReadyTasks)
	{
		_numReadyTasks = numReadyTasks;
	}

	inline int getCurrentReadyTasks() const
	{
		return _numReadyTasks;
	}

	inline void setCurrentBusyCores(float numBusyCores)
	{
		_numBusyCores = numBusyCores;
	}

	inline float getCurrentBusyCores() const
	{
		return _numBusyCores;
	}


	//! \brief Update number of tasks offloaded from this node to the ClusterNode
	inline void incNumOffloadedTasks(int by)
	{
		_numOffloadedTasks += by;
		assert(_numOffloadedTasks >= 0);
	}

	//! \brief Get number of tasks offloaded from this node to the ClusterNode
	inline int getNumOffloadedTasks() const
	{
		return _numOffloadedTasks;
	}

	inline int getInstrumentationRank() const
	{
		return _instrumentationRank;
	}

	std::string getHostName() const
	{
		return _hostname;
	}

	void setHostName(std::string hostname)
	{
		_hostname = hostname;
	}


	friend std::ostream& operator<<(std::ostream& out, const ClusterNode& in)
	{
		out << "Node:" << in._index << "[" << in._commIndex << "]";
		return out;
	}
};


#endif /* CLUSTER_NODE_HPP */

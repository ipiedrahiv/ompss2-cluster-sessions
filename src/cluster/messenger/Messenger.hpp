/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSENGER_HPP
#define MESSENGER_HPP

#include <deque>
#include <string>
#include <vector>
#include <type_traits>
#include <functional>

#pragma GCC visibility push(default)
#include <mpi.h>
#pragma GCC visibility pop

#include <Message.hpp>
#include <DataAccessRegion.hpp>
#include <support/GenericFactory.hpp>

class TransferBase;
class ClusterNode;
class DataTransfer;

class Messenger {
protected:
	int _argc;
	char **_argv;

	size_t _messageMaxSize = 0;

public:
	Messenger(int argc, char **argv) : _argc(argc), _argv(argv)
	{
		ConfigVariable<size_t> messageMaxSize("cluster.message_max_size");
		_messageMaxSize = messageMaxSize.getValue();
	}

	virtual ~Messenger()
	{
	}

	virtual void shutdown() = 0;

	template<typename T>
	static bool RegisterMSNClass(const std::string &name)
	{
		static_assert(std::is_base_of<Messenger, T>::value, "Base class type is wrong.");
		return GenericFactory<std::string, Messenger*, int, char**>::getInstance().emplace(
			name,
			[](int argc, char **argv) -> Messenger* {
				return new T(argc, argv);
			}
		);
	}

	//! \brief Send a message to a remote node
	//!
	//! \param[in] msg is the Message to send
	//! \param[in] toNode is the receiver node
	//! \param[in] block determines if the call will block until Message delivery
	virtual void sendMessage(Message *msg, ClusterNode const *toNode, bool block = false) = 0;

	//! \brief A barrier across all nodes within an apprank
	//!
	//! This is a collective operation that needs to be invoked
	//! by all nodes within an apprank
	virtual void synchronizeAll(void) = 0;

	virtual void abortAll(int errcode) = 0;

	//! \brief A barrier across all nodes within the real MPI_COMM_WORLD
	//!
	//! This is a collective operation that needs to be invoked
	//! by all nodes of all appranks
	virtual void synchronizeWorld(void) = 0;

	//! \brief Send a data region to a remote node, related to a previous message.
	//!
	//! \param[in] region is the data region to send
	//! \param[in] toNode is the receiver node
	//! \param[in] messageId is the id of the Message related with this
	//!		data transfer
	//! \param[in] if block is true then the call will block until the data
	//!		is sent
	//!
	//! \returns A DataTransfer object representing the pending data
	//!		transfer if the data is sent in non-blocking mode,
	//!		otherwise nullptr
	virtual DataTransfer *sendData(
		const DataAccessRegion &region,
		const ClusterNode *toNode,
		int messageId,
		bool block,
		bool instrument
	) = 0;

	//! \brief Receive a data region from a remote node, related to a previous message
	//!
	//! \param[in] region is the data region to fetch
	//! \param[in] fromNode is the node to fetch the data from
	//!		with this data transfer
	//! \param[in] messageId is the id of the Message related with this
	//!		data transfer
	//! \param[in] if block is true then the call will block until the data
	//!		is received
	//!
	//! \returns A DataTransfer object representing the pending data
	//!		transfer if the data is sent in non-blocking mode,
	//!		otherwise nullptr
	virtual DataTransfer *fetchData(
		const DataAccessRegion &region,
		const ClusterNode *fromNode,
		int messageId,
		bool block,
		bool instrument
	) = 0;

	//! \brief Check for incoming messages
	//!
	//! Invoke the messenger to check from incoming messages
	//!
	//! \return A pointer to a message or nullptr if none has been received
	virtual Message *checkMail() = 0;

	//! Get the index of the current node
	virtual int getNodeIndex() const = 0;

	//! Get the index of the master node
	virtual int getMasterIndex() const = 0;

	//! Get the size of the Cluster
	virtual int getClusterSize() const = 0;

	//! Returns true if this is the master node
	virtual bool isMasterNode() const = 0;

	virtual bool isSpawned() const = 0;

	size_t getMessageFragments(DataAccessRegion const &remoteRegion) const
	{
		assert(_messageMaxSize > 0);

		const size_t totalSize = remoteRegion.getSize();
		assert(totalSize > 0);

		// Note: this calculation still works when maxRegionSize == SIZE_MAX.
		return totalSize / _messageMaxSize + (bool) (totalSize % _messageMaxSize);
	};

	//! Spawn new processes.
	virtual int messengerSpawn(int delta, std::string hostname) = 0;
	virtual int messengerShrink(int delta) = 0;

	//! \brief Test if sending Messages has completed
	//!
	//! This tests whether messages stored in the 'messages'
	//! queue has been succesfully sent. All succesfully sent
	//! messages are marked as completed
	//!
	//! \param[in] messages holds the pending outgoing messages
	virtual void testCompletion(std::vector<Message *> &pendings) = 0;
	virtual void testCompletion(std::vector<DataTransfer *> &pendings) = 0;

	virtual void waitAllCompletion(std::vector<TransferBase *> &pendings) = 0;

	//! \brief Get external rank of the current node (meaning MPI rank in the original mpirun command)
	virtual int getExternalRank() const = 0;

	//! \brief Get number of external ranks (meaning MPI ranks in the original mpirun command)
	virtual int getNumExternalRanks() const = 0;

	//! Get the number of physical nodes
	virtual int getNumNodes() const = 0;

	//! \brief Get the physical node number
	virtual int getPhysicalNodeNum() const = 0;

	//! \brief Get the index number of the instances on this physical node
	virtual int getIndexThisPhysicalNode() const = 0;

	//! \brief Get total number of instances on this node
	virtual int getNumInstancesThisNode() const = 0;

	//! \brief Get the apprank number (set of instances collaborating for single application MPI rank)
	virtual int getApprankNum() const = 0;

	//! \brief Get the number of appranks (number of application MPI ranks)
	virtual int getNumAppranks() const = 0;

	//! \brief Get the application's MPI communicator
	virtual MPI_Comm getAppCommunicator() const = 0;

	//! \brief Get a list of the instances on this node and whether they are masters
	virtual const std::vector<bool> &getIsMasterThisNode(void) const = 0;

	//! \brief For verbose instrumentation, summarize the instances and appranks
	virtual void summarizeSplit() const = 0;

	//! Get vector relating internal rank to external rank in this apprank
	virtual const std::vector<int> &getInternalRankToExternalRank() const = 0;

	virtual const std::vector<int> &getInstanceThisNodeToExternalRank() const = 0;

	//! Get rank for Extrae traces
	virtual int getInstrumentationRank() const = 0;

	//! Get rank for Extrae traces for other internal ranks
	virtual int internalRankToInstrumentationRank(int i) const = 0;

	virtual void TAMPIInit() = 0;
	virtual void TAMPIFinalize() = 0;
	virtual bool TAMPIInitialized() const = 0;
};

#endif /* MESSENGER_HPP */

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2022 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_RESIZE_HPP
#define MESSAGE_RESIZE_HPP

#include <unistd.h>
#include <Message.hpp>
#include <api/nanos6/cluster.h>

struct MessageSpawnHostInfo {
	static constexpr MessageType messageType = SPAWN;
	char hostname[HOST_NAME_MAX];
	size_t nprocs;

	MessageSpawnHostInfo(std::string name, size_t procs) :nprocs(procs)
	{
		strncpy(hostname, name.c_str(), HOST_NAME_MAX);
	}

	friend std::ostream& operator<<(std::ostream& out, const MessageSpawnHostInfo& in)
	{
		out << "hostname:" << in.hostname << " nprocs:" << in.nprocs;
		return out;
	}
};

struct MessageShrinkDataInfo {
	static constexpr MessageType messageType = SHRINK;
	DataAccessRegion region;
	int oldLocationIdx;
	int newLocationIdx;
	WriteID oldWriteId;
	WriteID newWriteId;
	int tag;

	friend std::ostream& operator<<(std::ostream& out, const MessageShrinkDataInfo& in)
	{
		out << "region:[" << in.region << "]"
			<< " loc:" << in.oldLocationIdx << "->" << in.newLocationIdx
			<< " WID:" << in.oldWriteId << "->" << in.newWriteId
			<< " tag:" << in.tag;
		return out;
	}
};

template <typename T>
class MessageResize : public Message {

	struct ResizeMessageContent {
		//! address of the distributed allocation
		nanos6_spawn_policy_t _policy;
		int _deltaNodes;   // >0 spawn || <0 shrink: but always !=0
		size_t _nEntries;
		T _listEntries[];
	};

	ResizeMessageContent *_content;

public:
	MessageResize(nanos6_spawn_policy_t policy, int deltaNodes, size_t nEntries, const T *infoList)
		: Message(T::messageType, sizeof(ResizeMessageContent) + nEntries * sizeof(T)),
		  _content(reinterpret_cast<ResizeMessageContent *>(_deliverable->payload))
	{
		assert(_content != nullptr);
		assert(deltaNodes != 0);

		_content->_policy = policy;
		_content->_deltaNodes = deltaNodes;
		_content->_nEntries = nEntries;
		memcpy(_content->_listEntries, infoList, nEntries * sizeof(T));
	}

	MessageResize(nanos6_spawn_policy_t policy, int deltaNodes, const std::vector<T> infoList)
		: MessageResize(policy, deltaNodes, infoList.size(), infoList.data())
	{
	}

	MessageResize(Deliverable *dlv) : Message(dlv),
		  _content(reinterpret_cast<ResizeMessageContent *>(_deliverable->payload))
	{
		assert(_content != nullptr);
		assert(_content->_deltaNodes != 0);
	}

	int getDeltaNodes() const
	{
		return _content->_deltaNodes;
	}

	size_t getNEntries() const
	{
		return _content->_nEntries;
	}

	const T *getEntries() const
	{
		return _content->_listEntries;
	}

	inline std::string toString() const
	{
		std::stringstream ss;
		ss << "[" << this->getName() << ": " << _content->_deltaNodes << "]";
		return ss.str();
	}

	nanos6_spawn_policy_t getPolicy() const
	{
		return _content->_policy;
	}

	bool handleMessage() override;
	bool handleMessageNamespace() override;
};

typedef MessageResize<MessageSpawnHostInfo> MessageSpawn;
typedef MessageResize<MessageShrinkDataInfo> MessageShrink;

static const bool __attribute__((unused))_registered_spawn =
	Message::RegisterMSGClass<MessageSpawn>(SPAWN);

static const bool __attribute__((unused))_registered_shrink =
	Message::RegisterMSGClass<MessageShrink>(SHRINK);

#endif // MESSAGE_RESIZE_HPP

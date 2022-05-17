/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2022 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_RESIZE_HPP
#define MESSAGE_RESIZE_HPP

#include "Message.hpp"
 #include <unistd.h>

struct MessageSpawnHostInfo {
	static constexpr MessageType messageType = SPAWN;
	char hostname[HOST_NAME_MAX];
	size_t nprocs;

	MessageSpawnHostInfo(std::string name, size_t procs) :nprocs(procs)
	{
		strncpy(hostname, name.c_str(), HOST_NAME_MAX);
	}
};

struct MessageShrinkDataInfo {
	static constexpr MessageType messageType = SHRINK;
	DataAccessRegion region;
	int locationIdx;
};

template<typename T>
class MessageResize : public Message {

	struct ResizeMessageContent {
		//! address of the distributed allocation
		int _deltaNodes;   // >0 spawn || <0 shrink: but always !=0
		size_t _nEntries;
		T _listEntries[];
	};

	ResizeMessageContent *_content;

public:
	MessageResize(int deltaNodes, size_t nEntries, const T *infoList)
		: Message(T::messageType, sizeof(ResizeMessageContent) + nEntries * sizeof(T)),
		  _content(reinterpret_cast<ResizeMessageContent *>(_deliverable->payload))
	{
		assert(_content != nullptr);
		assert(deltaNodes != 0);

		_content->_deltaNodes = deltaNodes;
		_content->_nEntries = nEntries;
		memcpy(_content->_listEntries, infoList, nEntries * sizeof(T));
	}

	MessageResize(int deltaNodes, const std::vector<T> infoList)
		: MessageResize(deltaNodes, infoList.size(), infoList.data())
	{
	}


	MessageResize(Deliverable *dlv)
		: Message(dlv),
		  _content(reinterpret_cast<ResizeMessageContent *>(_deliverable->payload))
	{
		assert(_content != nullptr);
		assert(_content->_deltaNodes != 0);
	}

	bool handleMessage();

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

};

typedef MessageResize<MessageSpawnHostInfo> MessageSpawn;
typedef MessageResize<MessageShrinkDataInfo> MessageShrink;


#endif // MESSAGE_RESIZE_HPP

/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2022 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_RESIZE_HPP
#define MESSAGE_RESIZE_HPP

#include "Message.hpp"

class MessageResize : public Message {
	struct ResizeMessageContent {
		//! address of the distributed allocation
		int _deltaNodes;   // >0 spawn || <0 shrink: but always !=0
	};

	ResizeMessageContent *_content;

public:
	MessageResize(int deltaNodes);


	MessageResize(Deliverable *dlv) : Message(dlv)
	{
		_content = reinterpret_cast<ResizeMessageContent *>(_deliverable->payload);
		assert(_content->_deltaNodes != 0);
	}

	bool handleMessage();

	inline std::string toString() const
	{
		std::stringstream ss;

		if (_content->_deltaNodes > 0) {
			ss << "[spawn: ";
		} else if (_content->_deltaNodes < 0) {
			ss << "[shrink: ";
		} else {
			FatalErrorHandler::fail("Spawn message can't have delta 0");
		}

		ss << _content->_deltaNodes << "]";

		return ss.str();
	}

};

#endif // MESSAGE_RESIZE_HPP

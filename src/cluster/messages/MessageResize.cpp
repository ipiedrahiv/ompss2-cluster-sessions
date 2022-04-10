/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageResize.hpp"
#include <ClusterManager.hpp>

MessageResize::MessageResize(int deltaNodes, std::string hostname)
	: Message(RESIZE, sizeof(ResizeMessageContent))
{
	assert(deltaNodes != 0);
	_content = reinterpret_cast<ResizeMessageContent *>(_deliverable->payload);
	_content->_deltaNodes = deltaNodes;
	strncpy(_content->_hostname, hostname.c_str(), HOST_NAME_MAX);
}

bool MessageResize::handleMessage()
{
	const int deltaNodes = _content->_deltaNodes;

	FatalErrorHandler::failIf(deltaNodes == 0, "Handling resize message with delta == 0");
	if (deltaNodes > 0) {
		ClusterManager::nanos6Spawn(this);
	} else {
		FatalErrorHandler::fail("Negative delta (shrink) not supported yet");
	}

	return true;
}

static const bool __attribute__((unused))_registered_resize =
	Message::RegisterMSGClass<MessageResize>(RESIZE);

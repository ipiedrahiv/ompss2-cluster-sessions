/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageResize.hpp"
#include <ClusterManager.hpp>

MessageResize::MessageResize(int deltaNodes)
	: Message(RESIZE, sizeof(ResizeMessageContent))
{
	assert(deltaNodes != 0);
	_content = reinterpret_cast<ResizeMessageContent *>(_deliverable->payload);
	_content->_deltaNodes = deltaNodes;
}

bool MessageResize::handleMessage()
{
	const int deltaNodes = _content->_deltaNodes;

	FatalErrorHandler::failIf(deltaNodes == 0, "Handling resize message with delta == 0");
	ClusterManager::nanos6Resize(deltaNodes);

	return true;
}

static const bool __attribute__((unused))_registered_resize =
	Message::RegisterMSGClass<MessageResize>(RESIZE);

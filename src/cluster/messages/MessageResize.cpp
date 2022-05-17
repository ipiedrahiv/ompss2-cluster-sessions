/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#include "MessageResize.hpp"
#include <ClusterManager.hpp>

template<>
bool MessageSpawn::handleMessage()
{
	assert(_content->_deltaNodes > 0);
	ClusterManager::nanos6Spawn(this);
	return true;
}

template<>
bool MessageShrink::handleMessage()
{
	assert(_content->_deltaNodes < 0);
	ClusterManager::nanos6Shrink(this);
	return true;
}


static const bool __attribute__((unused))_registered_spawn =
	Message::RegisterMSGClass<MessageSpawn>(SPAWN);

static const bool __attribute__((unused))_registered_shrink =
	Message::RegisterMSGClass<MessageShrink>(SHRINK);

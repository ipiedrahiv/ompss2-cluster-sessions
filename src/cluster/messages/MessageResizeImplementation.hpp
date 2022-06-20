/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_RESIZE_IMPLEMENTATION_HPP
#define MESSAGE_RESIZE_IMPLEMENTATION_HPP


#include "MessageResize.hpp"

#include <ClusterManager.hpp>
#include <NodeNamespace.hpp>

template<typename T>
bool MessageResize<T>::handleMessage()
{
	NodeNamespace::setActionMessage(this);
	return false;
}

template<typename T>
bool MessageResize<T>::handleMessageNamespace()
{
	ClusterManager::handleResizeMessage(this);
	return true;
}

#endif // MESSAGE_RESIZE_IMPLEMENTATION_HPP

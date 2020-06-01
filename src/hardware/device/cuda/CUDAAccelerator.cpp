/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2020 Barcelona Supercomputing Center (BSC)
*/

#include <nanos6/polling.h>

#include "CUDAAccelerator.hpp"

#include "hardware/HardwareInfo.hpp"
#include "hardware/places/ComputePlace.hpp"
#include "hardware/places/MemoryPlace.hpp"
#include "scheduling/Scheduler.hpp"

thread_local Task* CUDAAccelerator::_currentTask;

CUDAAccelerator::CUDAAccelerator(int cudaDeviceIndex) :
	Accelerator(cudaDeviceIndex, nanos6_cuda_device),
	_streamPool(cudaDeviceIndex)
{
	CUDAFunctions::getDeviceProperties(_deviceProperties, _deviceHandler);
	registerPolling();
}

// Now the private functions

int CUDAAccelerator::pollingService(void *data)
{
	// Check if the thread running the service is a WorkerThread. nullptr means LeaderThread.
	bool worker = (WorkerThread::getCurrentWorkerThread() != nullptr);

	Task *task = nullptr;
	CUDAAccelerator *accel = (CUDAAccelerator *)data;
	assert(accel != nullptr);

	// For CUDA we need the setDevice operation here, for the stream & event context-to-thread association
	accel->setActiveDevice();
	do {
		if (accel->_streamPool.streamAvailable()) {
			task = Scheduler::getReadyTask(accel->_computePlace);
			if (task != nullptr) {
				accel->runTask(task);
			}
		}
		accel->processCUDAEvents();
	} while (accel->_activeEvents.size() != 0 && worker);

	// If process was run by LeaderThread, request a WorkerThread to continue.
	if (!worker && task != nullptr) {
		CPUManager::executeCPUManagerPolicy(nullptr, ADDED_TASKS, 1);
	}

	return 0;
}

// For each CUDA device task a CUDA stream is required for the asynchronous
// launch; To ensure kernel completion a CUDA event is 'recorded' on the stream
// right after the kernel is queued. Then when a cudaEventQuery call returns
// succefully, we can be sure that the kernel execution (and hence the task)
// has finished.

// Get a new CUDA event and queue it in the stream the task has launched
void CUDAAccelerator::postRunTask(Task *task)
{
	nanos6_cuda_device_environment_t &env =	task->getDeviceEnvironment().cuda;
	CUDAFunctions::recordEvent(env.event, env.stream);
	_activeEvents.push_back({env.event, task});
}

// Query the events issued to detect task completion
void CUDAAccelerator::processCUDAEvents()
{
	_preallocatedEvents.clear();
	std::swap(_preallocatedEvents, _activeEvents);

	for (CUDAEvent &ev : _preallocatedEvents)
		if (CUDAFunctions::cudaEventFinished(ev.event)) {
			finishTask(ev.task);
		}
		else
			_activeEvents.push_back(ev);
}


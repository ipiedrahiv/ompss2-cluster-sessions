
/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019-2020 Barcelona Supercomputing Center (BSC)
*/

#include "DefaultCPUActivation.hpp"
#include "DefaultCPUManager.hpp"
#include "executors/threads/ThreadManager.hpp"
#include "executors/threads/cpu-managers/default/policies/BusyPolicy.hpp"
#include "executors/threads/cpu-managers/default/policies/IdlePolicy.hpp"
#include "scheduling/Scheduler.hpp"
#include "system/TrackingPoints.hpp"

boost::dynamic_bitset<> DefaultCPUManager::_idleCPUs;
SpinLock DefaultCPUManager::_idleCPUsLock;
size_t DefaultCPUManager::_numIdleCPUs;


/*    CPUMANAGER    */

void DefaultCPUManager::preinitialize()
{
	_finishedCPUInitialization = false;

	// In cluster mode, the LeaderThread is necessary, otherwise the polling services (which manage
	// communication with other nodes) may not be run frequently enough, causing a high latency on
	// communications. This has been observed to be a problem in test cases. But the LeaderThread
	// normally overallocates a CPU, which means that it can preempt a worker thread that is holding
	// the scheduler lock. In this case, no new tasks can be scheduled until all the messages have
	// been handled.  Therefore, in cluster mode or malleable executions, leave one CPU free to be
	// used by the LeaderThread.
	_reserveCPUforLeaderThread
		= (ClusterManager::inClusterMode() || ClusterManager::clusterMalleableMaxSize() != 0);

	// Retreive the CPU mask of this process
	int rc = sched_getaffinity(0, sizeof(cpu_set_t), &_cpuMask);
	FatalErrorHandler::handle(rc, " when retrieving the affinity of the process");

	// Get the number of NUMA nodes and a list of all available CPUs
	nanos6_device_t hostDevice = nanos6_device_t::nanos6_host_device;
	const size_t numNUMANodes = HardwareInfo::getMemoryPlaceCount(hostDevice);
	HostInfo *hostInfo = ((HostInfo *) HardwareInfo::getDeviceInfo(hostDevice));
	assert(hostInfo != nullptr);

	const std::vector<ComputePlace *> &cpus = hostInfo->getComputePlaces();
	const size_t numCPUs = cpus.size();
	assert(numCPUs > 0);

	// Create the chosen policy for this CPUManager
	std::string policyValue = _policyChosen.getValue();
	if (policyValue == "idle" || (policyValue == "default" && !ClusterManager::inClusterMode())) {
		// default is normally the idle policy
		_cpuManagerPolicy = new IdlePolicy(numCPUs);
	} else if (policyValue == "busy" || (policyValue == "default" && ClusterManager::inClusterMode())) {
		// in cluster mode, default is the busy policy
		_cpuManagerPolicy = new BusyPolicy();
	} else {
		FatalErrorHandler::fail("Unexistent '", policyValue, "' CPU Manager Policy");
	}
	assert(_cpuManagerPolicy != nullptr);

	// Find the maximum system CPU id
	size_t maxSystemCPUId = 0;
	for (size_t i = 0; i < numCPUs; ++i) {
		const CPU *cpu = (const CPU *) cpus[i];
		assert(cpu != nullptr);

		if (cpu->getSystemCPUId() > maxSystemCPUId) {
			maxSystemCPUId = cpu->getSystemCPUId();
		}
	}

	// Set appropriate sizes for the vector of CPUs and their id maps
	const size_t numSystemCPUs = maxSystemCPUId + 1;
	const size_t numAvailableCPUs = CPU_COUNT(&_cpuMask);
	_cpus.resize(numAvailableCPUs);
	_systemToVirtualCPUId.resize(numSystemCPUs);

	// Find the appropriate value for taskfor groups
	std::vector<int> availableNUMANodes(numNUMANodes, 0);
	for (size_t i = 0; i < numCPUs; i++) {
		CPU *cpu = (CPU *) cpus[i];
		assert(cpu != nullptr);

		if (CPU_ISSET(cpu->getSystemCPUId(), &_cpuMask)) {
			size_t NUMANodeId = cpu->getNumaNodeId();
			availableNUMANodes[NUMANodeId]++;
		}
	}

	size_t numValidNUMANodes = 0;
	for (size_t i = 0; i < numNUMANodes; i++) {
		if (availableNUMANodes[i] > 0) {
			numValidNUMANodes++;
		}
	}
	refineTaskforGroups(numAvailableCPUs, numValidNUMANodes);

	// Initialize each CPU's fields
	size_t groupId = 0;
	size_t virtualCPUId = 0;
	size_t numCPUsPerTaskforGroup = numAvailableCPUs / getNumTaskforGroups();
	assert(numCPUsPerTaskforGroup > 0);

	for (size_t i = 0; i < numCPUs; ++i) {
		CPU *cpu = (CPU *) cpus[i];
		cpu->setIndex(-1);

		if (CPU_ISSET(cpu->getSystemCPUId(), &_cpuMask)) {
			// Check if this CPU goes into another group
			if (numCPUsPerTaskforGroup == 0) {
				numCPUsPerTaskforGroup = (numAvailableCPUs / getNumTaskforGroups()) - 1;
				++groupId;
			} else {
				--numCPUsPerTaskforGroup;
			}

			cpu->setIndex(virtualCPUId);
			cpu->setGroupId(groupId);
			_cpus[virtualCPUId++] = cpu;
		}
		_systemToVirtualCPUId[cpu->getSystemCPUId()] = cpu->getIndex();
	}
	assert(virtualCPUId == numAvailableCPUs);

	// The first CPU is always 0 (virtual identifier)
	_firstCPUId = 0;

	CPUManagerInterface::reportInformation(numSystemCPUs, numNUMANodes);
	if (_taskforGroupsReportEnabled) {
		CPUManagerInterface::reportTaskforGroupsInfo();
	}

	// Initialize idle CPU structures
	_idleCPUs.resize(numAvailableCPUs);
	_idleCPUs.reset();
	_numIdleCPUs = 0;

	// Initialize the virtual CPU for the leader thread
	_leaderThreadCPU = (_reserveCPUforLeaderThread ? _cpus[numAvailableCPUs - 1] : new CPU(numCPUs));
	assert(_leaderThreadCPU != nullptr);
}

void DefaultCPUManager::initialize()
{
	const size_t nWorkers = _cpus.size() - _reserveCPUforLeaderThread;

	for (size_t id = 0; id < _cpus.size(); ++id) {
		CPU *cpu = _cpus[id];
		assert(cpu != nullptr);

		__attribute__((unused)) bool worked = cpu->initializeIfNeeded();
		assert(worked);

		// reserve last CPU for the leader thread (not CPU zero which
		// always runs main when running with Extrae)
		if (id < nWorkers) {
			WorkerThread *initialThread = ThreadManager::createWorkerThread(cpu);
			assert(initialThread != nullptr);
			initialThread->resume(cpu, true);
		}
	}

	_finishedCPUInitialization = true;
}

void DefaultCPUManager::shutdownPhase1()
{
	// Notify all CPUs that the runtime is shutting down
	// reserve last CPU for the leader thread if _reserveCPUforLeaderThread
	const size_t nWorkers = _cpus.size() - _reserveCPUforLeaderThread;

	for (size_t id = 0; id < nWorkers; ++id) {
		DefaultCPUActivation::shutdownCPU(_cpus[id]);
	}
}

void DefaultCPUManager::forcefullyResumeFirstCPU()
{
	bool resumed = false;

	_idleCPUsLock.lock();

	if (_idleCPUs[_firstCPUId]) {
		_idleCPUs[_firstCPUId] = false;
		assert(_numIdleCPUs > 0);

		--_numIdleCPUs;
		resumed = true;
	}

	_idleCPUsLock.unlock();

	if (resumed) {
		assert(_cpus[_firstCPUId] != nullptr);

		// Runtime Tracking Point - A cpu becomes active
		TrackingPoints::cpuBecomesActive(_cpus[_firstCPUId]);

		ThreadManager::resumeIdle(_cpus[_firstCPUId]);
	}
}


/*    CPUACTIVATION BRIDGE    */

CPU::activation_status_t DefaultCPUManager::checkCPUStatusTransitions(WorkerThread *thread)
{
	return DefaultCPUActivation::checkCPUStatusTransitions(thread);
}

bool DefaultCPUManager::acceptsWork(CPU *cpu)
{
	return DefaultCPUActivation::acceptsWork(cpu);
}

bool DefaultCPUManager::enable(size_t systemCPUId)
{
	return DefaultCPUActivation::enable(systemCPUId);
}

bool DefaultCPUManager::disable(size_t systemCPUId)
{
	return DefaultCPUActivation::disable(systemCPUId);
}


/*    IDLE MECHANISM    */

bool DefaultCPUManager::cpuBecomesIdle(CPU *cpu)
{
	assert(cpu != nullptr);

	const int index = cpu->getIndex();

	std::lock_guard<SpinLock> guard(_idleCPUsLock);

	// If there is no CPU serving tasks in the scheduler,
	// abort the idle process of this CPU and go back
	// to the scheduling part. Notice that this check
	// must be inside the exclusive section covered
	// by the idle mutex
	if (!Scheduler::isServingTasks()) {
		return false;
	}

	// Runtime Tracking Point - A cpu becomes idle
	WorkerThread *currentThread = WorkerThread::getCurrentWorkerThread();
	TrackingPoints::cpuBecomesIdle(cpu, currentThread);

	// Mark the CPU as idle
	_idleCPUs[index] = true;
	++_numIdleCPUs;
	assert(_numIdleCPUs <= _cpus.size());

	return true;
}

CPU *DefaultCPUManager::getIdleCPU()
{
	_idleCPUsLock.lock();

	boost::dynamic_bitset<>::size_type id = _idleCPUs.find_first();
	if (id != boost::dynamic_bitset<>::npos) {
		CPU *cpu = _cpus[id];
		assert(cpu != nullptr);

		// Mark the CPU as active
		_idleCPUs[id] = false;
		assert(_numIdleCPUs > 0);

		--_numIdleCPUs;

		_idleCPUsLock.unlock();

		// Runtime Tracking Point - A cpu becomes active
		TrackingPoints::cpuBecomesActive(cpu);

		return cpu;
	} else {
		_idleCPUsLock.unlock();

		return nullptr;
	}
}

size_t DefaultCPUManager::getIdleCPUs(
	size_t numCPUs,
	CPU *idleCPUs[]
) {
	size_t numObtainedCPUs = 0;

	_idleCPUsLock.lock();

	boost::dynamic_bitset<>::size_type id = _idleCPUs.find_first();
	while (numObtainedCPUs < numCPUs && id != boost::dynamic_bitset<>::npos) {
		CPU *cpu = _cpus[id];
		assert(cpu != nullptr);

		// Mark the CPU as active
		_idleCPUs[id] = false;

		// Place the CPU in the vector
		idleCPUs[numObtainedCPUs] = cpu;
		++numObtainedCPUs;

		// Iterate to the next idle CPU
		id = _idleCPUs.find_next(id);
	}

	// Decrease the counter of idle CPUs by the obtained amount
	assert(_numIdleCPUs >= numObtainedCPUs);
	_numIdleCPUs -= numObtainedCPUs;

	_idleCPUsLock.unlock();

	for (size_t i = 0; i < numObtainedCPUs; ++i) {
		// Runtime Tracking Point - A cpu becomes active
		TrackingPoints::cpuBecomesActive(idleCPUs[i]);
	}

	return numObtainedCPUs;
}

void DefaultCPUManager::getIdleCollaborators(
	std::vector<CPU *> &idleCPUs,
	ComputePlace *cpu
) {
	assert(cpu != nullptr);

	size_t numObtainedCollaborators = 0;
	const size_t groupId = ((CPU *) cpu)->getGroupId();

	_idleCPUsLock.lock();

	boost::dynamic_bitset<>::size_type id = _idleCPUs.find_first();
	while (id != boost::dynamic_bitset<>::npos) {
		CPU *collaborator = _cpus[id];
		assert(collaborator != nullptr);

		if (groupId == collaborator->getGroupId()) {
			// Mark the CPU as active
			_idleCPUs[id] = false;

			// Place the CPU in the vector
			idleCPUs.push_back(collaborator);
			++numObtainedCollaborators;
		}

		// Iterate to the next idle CPU
		id = _idleCPUs.find_next(id);
	}

	// Decrease the counter of idle CPUs by the obtained amount
	assert(_numIdleCPUs >= numObtainedCollaborators);
	_numIdleCPUs -= numObtainedCollaborators;

	_idleCPUsLock.unlock();

	for (size_t i = 0; i < numObtainedCollaborators; ++i) {
		// Runtime Tracking Point - A cpu becomes active
		TrackingPoints::cpuBecomesActive(idleCPUs[i]);
	}
}



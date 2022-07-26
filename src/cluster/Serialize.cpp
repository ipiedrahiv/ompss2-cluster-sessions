/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#include "Serialize.hpp"

#include <map>
#include <mpi.h>

#include "system/ompss/AddTask.hpp"
#include "system/ompss/TaskWait.hpp"

#include "DataAccessRegistration.hpp"

#include <ClusterManager.hpp>
#include <DataAccessRegion.hpp>
#include <VirtualMemoryManagement.hpp>
#include <ClusterMemoryManagement.hpp>
#include <ClusterUtil.hpp>

#include <messenger/mpi/MPIMessenger.hpp>

#include "src/cluster/messenger/mpi/MPIErrorHandler.hpp"

#include <sys/stat.h> // for mkdir


Serialize *Serialize::_singleton = nullptr;

Serialize::Serialize(int clusterMaxSize) : _nSentinels(clusterMaxSize)
{
	assert(_nSentinels > 0);

	// this needs to be in the local memory because it will be a dependency
	_sentinels = (int *) ClusterMemoryManagement::lmalloc(_nSentinels * sizeof(int));
}

Serialize::~Serialize()
{
	assert(_nSentinels > 0);
	assert(_sentinels != nullptr);

	ClusterMemoryManagement::lfree((void *) _sentinels, _nSentinels);
}


void Serialize::serialize(void *arg, void *, nanos6_address_translation_entry_t *)
{
	assert(arg != nullptr);

	SerializeArgs *const serializeArgs = reinterpret_cast<SerializeArgs *const>(arg);
	assert(serializeArgs != nullptr);
	assert(serializeArgs->_nodeIdx == ClusterManager::getCurrentClusterNode()->getCommIndex());

	MPI_Comm intracom = MPI_COMM_NULL;

	// We need to do this every time because communicator may change.
	if (ClusterManager::clusterSize() == 1) {
		assert(serializeArgs->_nodeIdx == 0);
		intracom = MPI_COMM_SELF;
	} else {
		Messenger *msn = ClusterManager::getMessenger();
		assert(msn != nullptr);
		MPIMessenger *mpiMsn = dynamic_cast<MPIMessenger *>(msn);
		assert(mpiMsn != nullptr);
		intracom = mpiMsn->getCommunicator();
	}

	const bool isSerialize = serializeArgs->_isSerialize;
	const size_t nRegions = serializeArgs->_numRegions;

	int mode = isSerialize ? (MPI_MODE_CREATE|MPI_MODE_WRONLY) : MPI_MODE_RDONLY;

	MPI_File fh;
	const std::string filename = Serialize::getFilename(serializeArgs);

	int rc = MPI_File_open(intracom, filename.c_str(), mode, MPI_INFO_NULL, &fh);
	MPIErrorHandler::handle(rc, intracom, "from MPI_File_open");

	if (nRegions > 0) {
		const char *start = (char *) serializeArgs->_fullRegion.getStartAddress();

		MPI_Status *statuses = (MPI_Status *) malloc(nRegions * sizeof(MPI_Status));
		assert(statuses != nullptr);

		for (size_t i = 0; i < serializeArgs->_numRegions; ++i) {
			// This asserts also that offset will be positive.
			assert(serializeArgs->_regionsDeps[i].fullyContainedIn(serializeArgs->_fullRegion));

			void *start_i = serializeArgs->_regionsDeps[i].getStartAddress();

			MPI_Offset offset = (char *)start_i - start;
			int size = serializeArgs->_regionsDeps[i].getSize();

			if (isSerialize) {
				rc = MPI_File_write_at(fh, offset, start_i, size, MPI_BYTE, &statuses[i]);
				MPIErrorHandler::handle(rc, intracom, "from MPI_File_write_at");
			} else {
				rc = MPI_File_read_at(fh, offset, start_i, size, MPI_BYTE, &statuses[i]);
				MPIErrorHandler::handle(rc, intracom, "from MPI_File_read_at");
			}
		}
		free(statuses);
	}

	rc = MPI_File_close(&fh);
	MPIErrorHandler::handle(rc, intracom, "from MPI_File_close");
}

void Serialize::registerDependencies(void *arg, void *, void *)
{
	assert(arg != nullptr);
	SerializeArgs *const serializeArgs = reinterpret_cast<SerializeArgs *const>(arg);

	// READ_ACCESS_TYPE for serialize, WRITE_ACCESS_TYPE for opposite
	const DataAccessType accessType
		= serializeArgs->_isSerialize ? READ_ACCESS_TYPE : WRITE_ACCESS_TYPE;

	// Register the sentinel, required because only one MPI_File_Open can be called at the time.
	// MPI is so dumb that it can break just cross opening files (specially for reading).
	DataAccessRegistration::registerTaskDataAccess(
		serializeArgs->_task,
		READWRITE_ACCESS_TYPE,
		false,                          // weak
		serializeArgs->_sentinelRegion, // region
		0,                              // Symbol index... not sure
		no_reduction_type_and_operator,
		no_reduction_index
	);

	for (size_t i = 0; i < serializeArgs->_numRegions; ++i) {
		DataAccessRegistration::registerTaskDataAccess(
			serializeArgs->_task,
			accessType,
			false,                          // weak
			serializeArgs->_regionsDeps[i],            // region
			0,                              // Symbol index... not sure
			no_reduction_type_and_operator,
			no_reduction_index
		);
	}
}

void Serialize::getConstraints(void* arg, nanos6_task_constraints_t *const constraints)
{
	assert(arg != nullptr);
	SerializeArgs *const serializeArgs = reinterpret_cast<SerializeArgs *const>(arg);

	constraints->cost = 0;
	constraints->node = serializeArgs->_nodeIdx;
	constraints->stream = 0;
}

nanos6_task_invocation_info_t Serialize::invocationInfo = { "(De)Serialization code." };

nanos6_task_implementation_info_t Serialize::implementationsSerialize = {
	.device_type_id = nanos6_host_device,
	.run = Serialize::serialize,
	.get_constraints = Serialize::getConstraints,
	.task_label = "(De)Serialize",
	.declaration_source = 0,
	.run_wrapper = 0
};

nanos6_task_info_t Serialize::infoVarSerialize =
{
	.num_symbols = 1,                                      // WTF is this??
	.register_depinfo = Serialize::registerDependencies,
	.onready_action = 0,
	.get_priority = 0,
	.implementation_count = 1,
	.implementations = (nanos6_task_implementation_info_t*) &Serialize::implementationsSerialize,
	.destroy_args_block = 0,
	.duplicate_args_block = 0,
	.reduction_initializers = 0,
	.reduction_combiners = 0,
	.task_type_data = 0
};

// Get a vector with sets for all the regions and their homeNode
std::vector<Serialize::regionSet> Serialize::getHomeRegions(const DataAccessRegion &region)
{
	std::vector<regionSet> dependencies(ClusterManager::clusterSize());

	if (VirtualMemoryManagement::isClusterMemory(region)) {
		const Directory::HomeNodesArray *homeNodes = Directory::find(region);

		for (const auto &entry : *homeNodes) {
			const MemoryPlace *location = entry->getHomeNode();

			const size_t nodeId
				= (location->getType() == nanos6_host_device)
				? ClusterManager::getCurrentClusterNode()->getCommIndex()
				: location->getIndex();

			DataAccessRegion subregion = region.intersect(entry->getAccessRegion());
			assert(!subregion.empty());

			dependencies[nodeId].insert(subregion);
		}
		delete homeNodes;
	}
	return dependencies;
}

int Serialize::serializeRegion(
	const DataAccessRegion &region, size_t process, size_t id, bool serialize
) {
	assert(ClusterManager::isMasterNode());

	WorkerThread *workerThread = WorkerThread::getCurrentWorkerThread();
	assert(workerThread != nullptr);

	Task *parent = workerThread->getTask();
	assert(parent != nullptr);

	FatalErrorHandler::failIf(!parent->isMainTask(),
		"Serialization can only be called from main");

	if (_singleton == nullptr) {
		// This is a leak; the memory for _singleton->_sentinels is from lmalloc from inside the
		// main task, so we don't have a "good" way to release it. Even when this may not be an
		// issue... maybe you find a better way.
		// At the moment the only way I can imagine is to call the tryFinalize function from the
		// main_task_wrapper itself.. but that may be a bit too hacky?
		_singleton = new Serialize(ClusterManager::clusterMaxSize());
		assert(_singleton != nullptr);
	}

	// Attempt to create the directory if it does not exist.
	if (mkdir(std::to_string(process).c_str(), 0777) == 0) {
		std::cerr << "Serialization directory created: " << std::to_string(process) << std::endl;
	}

	std::vector<regionSet> dependencies = Serialize::getHomeRegions(region);
	assert(dependencies.size() == (size_t) ClusterManager::clusterSize());
	assert(dependencies.size() <= (size_t) ClusterManager::clusterMaxSize());

	// Submit one task/node even when there is not any region because the MPIIO function is
	// collective
	for (size_t nodeIdx = 0; nodeIdx < dependencies.size(); ++nodeIdx) {
		assert(nodeIdx < _singleton->_nSentinels);

		const regionSet &taskRegions = dependencies[nodeIdx];
		const size_t nRegions = taskRegions.size();
		int *sentinel = &(_singleton->_sentinels[nodeIdx]);

		Task *task = AddTask::createTask(
			(nanos6_task_info_t*) &Serialize::infoVarSerialize,
			(nanos6_task_invocation_info_t*) &Serialize::invocationInfo, // Task Invocation Info...
			nullptr,
			sizeof(SerializeArgs) + nRegions * sizeof(DataAccessRegion),
			0,                                        // flags
			nRegions,                                 // number of dependencies
			false                                     // from user code
		);
		assert(task != nullptr);

		SerializeArgs *args = reinterpret_cast<SerializeArgs *>(task->getArgsBlock());
		assert(args != nullptr);

		new (args) SerializeArgs(
			task, region, nodeIdx, process, id, serialize, sentinel, taskRegions
		);

		AddTask::submitTask(task, parent, false);
	}

	return 0;
}

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
#include <dirent.h>   // opendir
#include <errno.h>    // error 

Serialize *Serialize::_singleton = nullptr;

// Weak tasks part
nanos6_task_invocation_info_t Serialize::weakInvocationInfo = { "(De)Serialization weak code." };
nanos6_task_invocation_info_t Serialize::fetchInvocationInfo = { "(De)Serialization fetch code." };
nanos6_task_invocation_info_t Serialize::ioInvocationInfo = { "(De)Serialization strong code." };

// Strong tasks part
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

//============= Functions ===================================

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

void Serialize::createAndSubmitStrong(
	Task *parent,
	nanos6_task_invocation_info_t* invocationInfo,
	const SerializeArgs *const serializeArgs,
	void *sentinel
) {
	assert(parent != nullptr);
	assert(serializeArgs != nullptr);

	Task *task = AddTask::createTask(
		(nanos6_task_info_t*) &Serialize::infoVarSerialize,
		invocationInfo, // Task Invocation Info.
		nullptr,
		sizeof(SerializeArgs) + serializeArgs->_numRegions * sizeof(DataAccessRegion),
		0,                                        // flags
		serializeArgs->_numRegions,               // number of dependencies
		false                                     // from user code
	);
	assert(task != nullptr);

	SerializeArgs *args = reinterpret_cast<SerializeArgs *>(task->getArgsBlock());
	assert(args != nullptr);

	new (args) SerializeArgs(task, serializeArgs, sentinel);

	AddTask::submitTask(task, parent, false);
}

void Serialize::ioData(const SerializeArgs * const serializeArgs)
{
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

	ClusterManager::synchronizeAll();

	int rc = MPI_File_open(intracom, filename.c_str(), mode, MPI_INFO_NULL, &fh);
	MPIErrorHandler::handle(rc, intracom, "from MPI_File_open");

	// When deserializing, the file should have the same size than the full region, otherwise there
	// will be a nasty error latter.
	if (isSerialize == false && ClusterManager::isMasterNode()) {
		MPI_Offset fSize = 0;
		rc = MPI_File_get_size(fh, &fSize);
		MPIErrorHandler::handle(rc, intracom, "from MPI_File_open");

		const size_t regionSize = serializeArgs->_fullRegion.getSize();

		FatalErrorHandler::failIf(
			(size_t) fSize != regionSize,
			"Serialization file:", filename, " size:", fSize,
			" but region:", serializeArgs->_fullRegion, " has size:", regionSize
		);
	}

	if (nRegions > 0) {
		const char *start = (char *) serializeArgs->_fullRegion.getStartAddress();

		MPI_Status *statuses = (MPI_Status *) malloc(nRegions * sizeof(MPI_Status));
		assert(statuses != nullptr);

		for (size_t i = 0; i < serializeArgs->_numRegions; ++i) {
			// This asserts also that offset will be non-negative.
			assert(serializeArgs->_regionsDeps[i].fullyContainedIn(serializeArgs->_fullRegion));
			assert(serializeArgs->_regionsDeps[i].getSize() <= std::numeric_limits<int>::max());

			void * const start_i = serializeArgs->_regionsDeps[i].getStartAddress();
			const int size = serializeArgs->_regionsDeps[i].getSize();

			MPI_Offset offset = (char *)start_i - start;

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

	ClusterManager::synchronizeAll();
	rc = MPI_File_close(&fh);
	MPIErrorHandler::handle(rc, intracom, "from MPI_File_close");
}

void Serialize::serialize(void *arg, void *, nanos6_address_translation_entry_t *)
{
	assert(arg != nullptr);

	const SerializeArgs *const serializeArgs = reinterpret_cast<SerializeArgs *const>(arg);
	assert(serializeArgs != nullptr);
	assert(serializeArgs->_nodeIdx == ClusterManager::getCurrentClusterNode()->getCommIndex());

	if (serializeArgs->_isWeak) {
		WorkerThread *workerThread = WorkerThread::getCurrentWorkerThread();
		assert(workerThread != nullptr);

		Task *parent = workerThread->getTask();
		assert(parent != nullptr);

		// Weak task only create strong tasks: fetch and io task
		if (serializeArgs->_isSerialize) {
			// If it is a serialize, then create the fetch task to advance coping the data to this
			// node when needed. The fetch task is exactly like the io one, but without sentinel and
			// empty body, on de-serialization the fetch is not needed.
			createAndSubmitStrong(parent, &Serialize::fetchInvocationInfo, serializeArgs, nullptr);
		}

		createAndSubmitStrong(
			parent,
			&Serialize::ioInvocationInfo,
			serializeArgs,
			serializeArgs->_sentinelRegion.getStartAddress()
		);
	} else if (serializeArgs->_sentinelRegion.getStartAddress() != nullptr) {
		//Strong tasks are the one which make the io if have sentinel
		Serialize::ioData(serializeArgs);
	} else {
		// Do nothing, this is what happen when the task is strong without sentinel (Fetch task)
	}
}

void Serialize::registerDependencies(void *arg, void *, void *)
{
	assert(arg != nullptr);
	SerializeArgs *const serializeArgs = reinterpret_cast<SerializeArgs *const>(arg);

	// READ_ACCESS_TYPE for serialize, WRITE_ACCESS_TYPE for opposite
	const DataAccessType accessType
		= serializeArgs->_isSerialize ? READ_ACCESS_TYPE : WRITE_ACCESS_TYPE;

	if (serializeArgs->_sentinelRegion.getStartAddress() != nullptr) {
		// Register the sentinel, required because only one MPI_File_Open can be called at the time.
		// MPI is so dumb that it can break just cross opening files (specially for reading).
		DataAccessRegistration::registerTaskDataAccess(
			serializeArgs->_task,
			READWRITE_ACCESS_TYPE,
			serializeArgs->_isWeak,                 // weak
			serializeArgs->_sentinelRegion,         // region
			0,                                      // Symbol index... not sure
			no_reduction_type_and_operator,
			no_reduction_index
		);
	}

	for (size_t i = 0; i < serializeArgs->_numRegions; ++i) {
		DataAccessRegistration::registerTaskDataAccess(
			serializeArgs->_task,
			accessType,
			serializeArgs->_isWeak,             // weak
			serializeArgs->_regionsDeps[i],     // region
			0,                                  // Symbol index... not sure
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


			char *currAddress = (char *) subregion.getStartAddress();
			const char *endAddress = (char *) subregion.getEndAddress();

			// We won't be capable to write more bytes than numeric_limits<int> because MPI receives
			// an int as argument for size. From the OmpSs side (dependencies) we have the same
			// problem, so the big dependencies and datatransfers will be fragmented in the
			// MPIMessenger. Then it is simpler to just fragment them here in advance and we can be
			// sure we are save for the future.
			while (currAddress < endAddress) {
				const size_t currSize
					= std::min<size_t>(endAddress - currAddress, std::numeric_limits<int>::max());

				dependencies[nodeId].emplace((void *)currAddress, currSize);
				currAddress += currSize;
			}
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

	FatalErrorHandler::failIf(!parent->isMainTask(), "Serialization can only be called from main");

	if (_singleton == nullptr) {
		// This is a leak; the memory for _singleton->_sentinels is from lmalloc from inside the
		// main task, so we don't have a "good" way to release it. Even when this may not be an
		// issue... maybe you find a better way.
		// At the moment the only way I can imagine is to call the tryFinalize function from the
		// main_task_wrapper itself.. but that may be a bit too hacky?
		_singleton = new Serialize(ClusterManager::clusterMaxSize());
		assert(_singleton != nullptr);
	}

	// Check or create the serialization directory.
	const std::string dirname = std::to_string(process);

	DIR* dir = opendir(dirname.c_str());
	if (dir) {                                   // Directory exists, so do nothing,
		closedir(dir);
	} else if (ENOENT == errno) {                // Directory doesn't exist
		if (serialize) {                         // When serializing we can try to create it.
			const int ret = mkdir(dirname.c_str(), 0777);
			if (ret == 0) {
				FatalErrorHandler::warn("Serialization directory created: ", dirname);
			} else {
				FatalErrorHandler::warn("Couldn't create serialization dir: ", dirname);
				return -1;
			}
		} else {            // When deserializing we shouldn't create it, just warn and return error
			FatalErrorHandler::warn(
				"Deserialization directory: ", dirname, " doesn't exist. We can't recover from it.");
			return -1;
		}
	} else {                                     // On unknown error fail in either case
		FatalErrorHandler::warn(
			"Opendir failed to open serialization directory:", dirname, "Error: ", strerror(errno)
		);
		return -1;
	}

	// Get the dependencies vector.
	std::vector<regionSet> dependencies = Serialize::getHomeRegions(region);
	assert(dependencies.size() == (size_t) ClusterManager::clusterSize());
	assert(dependencies.size() <= (size_t) ClusterManager::clusterMaxSize());

	// Submit one task/node even when there is not any region because the MPI_File_open function is
	// collective
	for (size_t nodeIdx = 0; nodeIdx < dependencies.size(); ++nodeIdx) {
		assert(nodeIdx < _singleton->_nSentinels);

		const regionSet &taskRegions = dependencies[nodeIdx];
		const size_t nRegions = taskRegions.size();
		int *sentinel = &(_singleton->_sentinels[nodeIdx]);

		Task *task = AddTask::createTask(
			(nanos6_task_info_t*) &Serialize::infoVarSerialize,
			(nanos6_task_invocation_info_t*) &Serialize::weakInvocationInfo, // Task Invocation Info...
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

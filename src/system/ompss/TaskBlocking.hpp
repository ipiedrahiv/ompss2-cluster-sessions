#ifndef TASK_BLOCKING_HPP
#define TASK_BLOCKING_HPP


class WorkerThread;
class Task;


class TaskBlocking {
public:
	static void taskBlocks(WorkerThread *currentThread, Task *currentTask, bool allowRunningInline);
};


#endif // TASK_BLOCKING_HPP

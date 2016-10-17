#ifndef INSTRUMENT_NULL_TASK_STATUS_HPP
#define INSTRUMENT_NULL_TASK_STATUS_HPP


#include "../api/InstrumentTaskStatus.hpp"
#include <InstrumentTaskId.hpp>


namespace Instrument {
	inline void taskIsPending(__attribute__((unused)) task_id_t taskId)
	{
	}
	
	inline void taskIsReady(__attribute__((unused)) task_id_t taskId)
	{
	}
	
	inline void taskIsExecuting(__attribute__((unused)) task_id_t taskId)
	{
	}
	
	inline void taskIsBlocked(__attribute__((unused)) task_id_t taskId, __attribute__((unused)) task_blocking_reason_t reason)
	{
	}
	
	inline void taskIsZombie(__attribute__((unused)) task_id_t taskId)
	{
	}
	
}


#endif // INSTRUMENT_NULL_TASK_STATUS_HPP

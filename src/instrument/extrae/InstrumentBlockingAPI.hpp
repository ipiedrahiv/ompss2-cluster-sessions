/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018-2020 Barcelona Supercomputing Center (BSC)
*/

#ifndef INSTRUMENT_EXTRAE_BLOCKING_API_HPP
#define INSTRUMENT_EXTRAE_BLOCKING_API_HPP


#include "InstrumentCommon.hpp"
#include "InstrumentExtrae.hpp"
#include "InstrumentTaskExecution.hpp"
#include "instrument/api/InstrumentBlockingAPI.hpp"


namespace Instrument {
	inline void enterBlockCurrentTask(
		task_id_t taskId,
		__attribute__((unused)) bool taskRuntimeTransition,
		__attribute__((unused)) InstrumentationContext const &context
	) {
		extrae_combined_events_t ce;

		ce.HardwareCounters = 1;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 6;
		ce.nCommunications = 0;

		// Generate control dependency information
		// At level 1 we show "explicit" blocking calls but not taskwait dependencies
		if (Extrae::_detailTaskGraph) {
			ce.nCommunications++;
		}

		ce.Types  = (extrae_type_t *)  alloca (ce.nEvents * sizeof (extrae_type_t) );
		ce.Values = (extrae_value_t *) alloca (ce.nEvents * sizeof (extrae_value_t));

		if (ce.nCommunications > 0) {
			ce.Communications = (extrae_user_communication_t *) alloca(sizeof(extrae_user_communication_t) * ce.nCommunications);
		}

		ce.Types[0] = (extrae_type_t) EventType::RUNTIME_STATE;
		ce.Values[0] = (extrae_value_t) NANOS_SYNCHRONIZATION;

		ce.Types[1] = (extrae_type_t) EventType::RUNNING_CODE_LOCATION;
		ce.Values[1] = (extrae_value_t) nullptr;

		ce.Types[2] = (extrae_type_t) EventType::NESTING_LEVEL;
		ce.Values[2] = (extrae_value_t) nullptr;

		ce.Types[3] = (extrae_type_t) EventType::TASK_INSTANCE_ID;
		ce.Values[3] = (extrae_value_t) nullptr;

		ce.Types[4] = (extrae_type_t) EventType::PRIORITY;
		ce.Values[4] = (extrae_value_t) nullptr;

		ce.Types[5] = (extrae_type_t) EventType::RUNNING_FUNCTION_NAME;
		ce.Values[5] = (extrae_value_t) nullptr;

		// Generate control dependency information
		// At level 1 we show "explicit" blocking calls but not taskwait dependencies
		if (Extrae::_detailTaskGraph) {
			ce.Communications[0].type = EXTRAE_USER_SEND;
			ce.Communications[0].tag = (extrae_comm_tag_t) control_dependency_tag;
			ce.Communications[0].size = taskId._taskInfo->_taskId;
			ce.Communications[0].partner = EXTRAE_COMM_PARTNER_MYSELF;
			ce.Communications[0].id = taskId._taskInfo->_taskId;

			taskId._taskInfo->_lock.lock();
			taskId._taskInfo->_predecessors.emplace(0, control_dependency_tag);
			taskId._taskInfo->_lock.unlock();
		}

		Extrae::emit_CombinedEvents ( &ce );
	}

	inline void exitBlockCurrentTask(
		task_id_t taskId,
		__attribute__((unused)) bool taskRuntimeTransition,
		InstrumentationContext const &context
	) {
		returnToTask(taskId, context);
	}

	inline void enterUnblockTask(
		task_id_t taskId,
		__attribute__((unused)) bool taskRuntimeTransition,
		__attribute__((unused)) InstrumentationContext const &context
	) {
		// Generate control dependency information
		// At level 1 we show "explicit" blocking calls but not taskwait dependencies
		if (!Extrae::_detailTaskGraph) {
			return;
		}

		extrae_combined_events_t ce;

		ce.HardwareCounters = 1;
		ce.Callers = 0;
		ce.UserFunction = EXTRAE_USER_FUNCTION_NONE;
		ce.nEvents = 0;
		ce.nCommunications = 2;

		ce.Types  = nullptr;
		ce.Values = nullptr;
		ce.Communications = (extrae_user_communication_t *)
			alloca(sizeof(extrae_user_communication_t) * ce.nCommunications);

		// From blocking to unblocker
		ce.Communications[0].type = EXTRAE_USER_RECV;
		ce.Communications[0].tag = (extrae_comm_tag_t) control_dependency_tag;
		ce.Communications[0].size = taskId._taskInfo->_taskId;
		ce.Communications[0].partner = EXTRAE_COMM_PARTNER_MYSELF;
		ce.Communications[0].id = taskId._taskInfo->_taskId;

		// From unblocker to actual resumption
		ce.Communications[1].type = EXTRAE_USER_SEND;
		ce.Communications[1].tag = (extrae_comm_tag_t) control_dependency_tag;
		ce.Communications[1].size = taskId._taskInfo->_taskId;
		ce.Communications[1].partner = EXTRAE_COMM_PARTNER_MYSELF;
		ce.Communications[1].id = taskId._taskInfo->_taskId;

		taskId._taskInfo->_lock.lock();
		taskId._taskInfo->_predecessors.emplace(0, control_dependency_tag);
		taskId._taskInfo->_lock.unlock();

		Extrae::emit_CombinedEvents ( &ce );
	}

	inline void exitUnblockTask(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) bool taskRuntimeTransition,
		__attribute__((unused)) InstrumentationContext const &context
	) {
	}

	inline void enterWaitFor(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) InstrumentationContext const &context
	) {
		enterBlockCurrentTask(taskId, /* taskRuntimeTransition */ true, context);
	}

	inline void exitWaitFor(
		__attribute__((unused)) task_id_t taskId,
		__attribute__((unused)) InstrumentationContext const &context
	) {
		exitBlockCurrentTask(taskId, /* taskRuntimeTransition */ true, context);
	}
}


#endif // INSTRUMENT_EXTRAE_BLOCKING_HPP

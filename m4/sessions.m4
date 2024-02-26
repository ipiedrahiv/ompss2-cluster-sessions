#	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.
#
#	Copyright (C) 2023 Barcelona Supercomputing Center (BSC)

AC_DEFUN([AC_CHECK_SESSIONS],
	[
		AC_ARG_WITH(
			[sessions],
			[AS_HELP_STRING([--with-sessions], [specify if MPI Sessions should be used])],
		[ac_use_sessions=yes]
		)
		if test "x${ac_use_sessions}" = x"yes"; then
			AC_DEFINE(HAVE_SESSIONS, 1, [use sessions])
		fi
	]
)

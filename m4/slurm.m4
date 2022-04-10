#	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.
#
#	Copyright (C) 2015-2018 Barcelona Supercomputing Center (BSC)

AC_DEFUN([AC_CHECK_LIBSLURM],
	[
		AC_ARG_WITH(
			[libslurm],
			[AS_HELP_STRING([--with-libslurm=prefix], [specify the installation prefix of the slurm library])],
			[ ac_use_libslurm_prefix="${withval}" ],
			[ ac_use_libslurm_prefix="" ]
		)

		if test x"${ac_use_libslurm_prefix}" != x"" ; then
			AC_MSG_CHECKING([the libslurm installation prefix])
			AC_MSG_RESULT([${ac_use_libslurm_prefix}])
			libslurm_CPPFLAGS="-I${ac_use_libslurm_prefix}/include"
			libslurm_LIBS="-L${ac_use_libslurm_prefix}/lib"
			ac_use_libslurm=yes
		else
			PKG_CHECK_MODULES(
				[slurm],
				[slurm],
				[
					AC_MSG_CHECKING([the SLURM installation prefix])
					AC_MSG_RESULT([retrieved from pkg-config])
					libslurm_CPPFLAGS="${slurm_CFLAGS}"
					libslurm_LIBS="${slurm_LIBS}"
					ac_use_libslurm=yes
				], [
					AC_MSG_CHECKING([the SLURM installation prefix])
					AC_MSG_RESULT([not available])
				]
			)
		fi

		if test x"${ac_use_libslurm}" != x"" ; then
			ac_save_CPPFLAGS="${CPPFLAGS}"
			ac_save_LIBS="${LIBS}"

			CPPFLAGS="${CPPFLAGS} ${libslurm_CPPFLAGS}"
			LIBS="${LIBS} ${libslurm_LIBS}"

			AC_CHECK_HEADERS([slurm/slurm.h],[],
				[
					if test x"${ac_use_libslurm_prefix}" != x"" ; then
						AC_MSG_ERROR([SLURM header cannot be found.])
					else
						AC_MSG_WARN([SLURM header cannot be found.])
					fi
				])

			AC_CHECK_LIB([slurm],
				[slurm_hostlist_create],
				[
					AC_DEFINE(HAVE_SLURM, [1], [SLURM API is available])
					ac_use_libslurm=yes
				],[
					if test x"${ac_use_libslurm_prefix}" != x"" ; then
						AC_MSG_ERROR([SLURM library cannot be found.])
					else
						AC_MSG_WARN([SLURM library cannot be found.])
					fi
					libslurm_CPPFLAGS=""
					libslurm_LIBS=""
					ac_use_libslurm=no
				]
			)

			CPPFLAGS="${ac_save_CPPFLAGS}"
			LIBS="${ac_save_LIBS}"
		fi

		AM_CONDITIONAL(HAVE_SLURM, test x"${ac_use_libslurm}" = x"yes")

		AC_SUBST([libslurm_CPPFLAGS])
		AC_SUBST([libslurm_LIBS])
	]
)

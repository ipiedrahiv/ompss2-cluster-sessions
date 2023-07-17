#	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.
#
#	Copyright (C) 2023 Barcelona Supercomputing Center (BSC)

AC_DEFUN([AC_CHECK_TAMPI],
	[
		AC_ARG_WITH(
			[tampi],
			[AS_HELP_STRING([--with-tampi=prefix], [specify the installation prefix of TAMPI])],
			[ ac_cv_use_tampi_prefix=$withval ],
			[ ac_cv_use_tampi_prefix="" ]
		)

		if test x"${ac_cv_use_tampi_prefix}" != x"" ; then
			AC_MSG_CHECKING([the TAMPI installation prefix])
			AC_MSG_RESULT([${ac_cv_use_tampi_prefix}])
			tampi_LIBS="-L${ac_cv_use_tampi_prefix}/lib -ltampi"
			tampi_CPPFLAGS="-I$ac_cv_use_tampi_prefix/include"
			ac_use_tampi=yes
		else
			PKG_CHECK_MODULES(
				[tampi],
				[tampi],
				[
					AC_MSG_CHECKING([the TAMPI installation prefix])
					AC_MSG_RESULT([retrieved from pkg-config])
					tampi_CPPFLAGS="${tampi_CFLAGS}"
					ac_use_tampi=yes
				], [
					AC_MSG_CHECKING([the TAMPI installation prefix])
					AC_MSG_RESULT([not available])
				]
			)
		fi

		if test x"${ac_use_tampi}" != x"" ; then
			ac_save_CPPFLAGS="${CPPFLAGS}"
			ac_save_LIBS="${LIBS}"

			CPPFLAGS="${MPI_CXXFLAGS} ${tampi_CPPFLAGS}"
			LIBS="${LIBS} ${tampi_LIBS}"

			AC_CHECK_HEADER([TAMPI.h],
				[ ],
				[
					if test x"${ac_use_tampi_prefix}" != x"" ; then
						AC_MSG_ERROR([TAMPI header cannot be found.])
					else
						AC_MSG_WARN([TAMPI header cannot be found.])
					fi
					ac_use_tampi=no
				]
				)
			AC_CHECK_LIB([tampi-c],
				[TAMPI_Property_get],
				[
					tampi_LIBS="${tampi_LIBS}"
				],
				[
					if test x"${ac_cv_use_tampi_prefix}" != x"" ; then
						AC_MSG_ERROR([TAMPI cannot be found.])
					else
						AC_MSG_WARN([TAMPI cannot be found.])
					fi
					ac_use_tampi=no
				]
			)

			CPPFLAGS="${ac_save_CPPFLAGS}"
			LIBS="${ac_save_LIBS}"
		fi

		AM_CONDITIONAL(HAVE_TAMPI, test x"${ac_use_tampi}" = x"yes")

		AC_SUBST([tampi_LIBS])
		AC_SUBST([tampi_CPPFLAGS])
	]
)

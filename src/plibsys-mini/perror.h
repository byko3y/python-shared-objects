/* A single function extracted from perror.h and perror.c */

#pragma once

#include "ptypes.h"

#ifndef P_OS_WIN
#  if defined (P_OS_OS2)
#    define INCL_DOSERRORS
#    include <os2.h>
#  endif
#  include <errno.h>
#endif

static inline pint
p_error_get_last_system (void)
{
#ifdef P_OS_WIN
	return (pint) GetLastError ();
#else
#  ifdef P_OS_VMS
	pint error_code = errno;

	if (error_code == EVMSERR)
		return vaxc$errno;
	else
		return error_code;
#  else
	return errno;
#  endif
#endif
}

#ifndef PLIBSYS_HEADER_PLIBSYSCONFIG_H
#define PLIBSYS_HEADER_PLIBSYSCONFIG_H

#define PLIBSYS_VERSION_MAJOR 0
#define PLIBSYS_VERSION_MINOR 0
#define PLIBSYS_VERSION_PATCH 4
#define PLIBSYS_VERSION_STR "0.0.4"
#define PLIBSYS_VERSION 0x000004

// #define PLIBSYS_NEED_WINDOWS_H
#define PLIBSYS_NEED_FLOAT_H
#define PLIBSYS_NEED_LIMITS_H
/* #undef PLIBSYS_NEED_VALUES_H */
/* #undef PLIBSYS_NEED_PTHREAD_NP_H */
/* #undef PLIBSYS_IS_BIGENDIAN */

#include <pmacros.h>

#ifdef PLIBSYS_NEED_FLOAT_H
#  include <float.h>
#endif

#ifdef PLIBSYS_NEED_LIMITS_H
#  include <limits.h>
#endif

#ifdef PLIBSYS_NEED_VALUES_H
#  include <values.h>
#endif

P_BEGIN_DECLS

#define P_MINFLOAT    FLT_MIN
#define P_MAXFLOAT    FLT_MAX
#define P_MINDOUBLE   DBL_MIN
#define P_MAXDOUBLE   DBL_MAX
#define P_MINSHORT    SHRT_MIN
#define P_MAXSHORT    SHRT_MAX
#define P_MAXUSHORT   USHRT_MAX
#define P_MININT      INT_MIN
#define P_MAXINT      INT_MAX
#define P_MAXUINT     UINT_MAX
#define P_MINLONG     LONG_MIN
#define P_MAXLONG     LONG_MAX
#define P_MAXULONG    ULONG_MAX

// #define PLIBSYS_SIZEOF_VOID_P 4
// #define PLIBSYS_SIZEOF_SIZE_T 4
// #define PLIBSYS_SIZEOF_LONG 4

// #ifdef PLIBSYS_IS_BIGENDIAN
// #  define P_BYTE_ORDER P_BIG_ENDIAN
// #else
// #  define P_BYTE_ORDER P_LITTLE_ENDIAN
// #endif

P_END_DECLS

#endif /* PLIBSYS_HEADER_PLIBSYSCONFIG_H */

/*
 * The MIT License
 *
 * Copyright (C) 2010-2016 Alexander Saprykin <saprykin.spb@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * 'Software'), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * @file ptypes.h
 * @brief Types definitions
 * @author Alexander Saprykin
 *
 * Every operating system in pair with a compiler has its own set of data types.
 * Here you can find unified platform independent data types which guarantee the
 * same bit-size on every supported platform: #pint8, #pint16, #pint32, #pint64
 * and their unsigned variants. Also other types are defined for convinience:
 * #ppointer, #pboolean, #pint, #plong, #pdouble and more.
 *
 * Along with the types, length and format modifiers are defined. They can be
 * used to print and scan data from/to a variable.
 *
 * Sometimes it is useful to use an integer variable as a pointer, i.e. to
 * prevent memory allocation when using hash tables or trees. Use special macros
 * for that case: #PINT_TO_POINTER, #PPOINTER_TO_INT and their variants. Note
 * that it will not work with 64-bit data types.
 *
 * To check data type limits use P_MIN* and P_MAX* macros.
 *
 * If you need to check system endianness compare the P_BYTE_ORDER definition
 * with the #P_LITTLE_ENDIAN or #P_BIG_ENDIAN macro.
 *
 * To convert between the little and big endian byte orders use the Px_TO_LE,
 * Px_TO_BE, Px_FROM_LE and Px_FROM_BE macros. Macros for the network<->host
 * byte order conversion are also provided: #p_ntohl, #p_ntohs, #p_ntohs and
 * #p_ntohl. All the described above macros depend on the target system
 * endianness. Use PUINTx_SWAP_BYTES to manually swap data types independently
 * from the endianness.
 *
 * You can also find some of the function definitions used within the library.
 */

#ifndef PLIBSYS_HEADER_PTYPES_H
#define PLIBSYS_HEADER_PTYPES_H

#include <plibsysconfig.h>
#include <pmacros.h>
// Original libsys module reworked for C99 stdint support
#include <stdint.h>

P_BEGIN_DECLS

/** Type for signed 8 bit.	*/
typedef signed char		pint8;
/** Type for unsigned 8 bit.	*/
typedef unsigned char		puint8;
/** Type for signed 16 bit.	*/
typedef signed short		pint16;
/** Type for unsigned 16 bit.	*/
typedef unsigned short		puint16;
/** Type for signed 32 bit.	*/
typedef signed int		pint32;
/** Type for unsigned 32 bit.	*/
typedef unsigned int		puint32;

/**
  * @var pint64
  * @brief Type for signed 64 bit.
  */

/**
  * @var puint64
  * @brief Type for unsigned 64 bit.
  */

typedef int64_t	pint64;
typedef uint64_t	puint64;

/** Type for a pointer.		*/
typedef void *		ppointer;
/** Type for a const pointer.	*/
typedef const void *	pconstpointer;

/** Type for a bool.	*/
typedef signed int	pboolean;
/** Type for a char.	*/
typedef char		pchar;
/** Type for a short.	*/
typedef short		pshort;
/** Type for an int.	*/
typedef int		pint;
/** Type for a long.	*/
typedef long		plong;

/** Type for an unsigned char.	*/
typedef unsigned char	puchar;
/** Type for an unsigned short.	*/
typedef unsigned short	pushort;
/** Type for an unsigned int.	*/
typedef unsigned int	puint;
/** Type for an unsigned long.	*/
typedef unsigned long	pulong;

/** Type for a float.			*/
typedef float		pfloat;
/** Type for a double precision float.	*/
typedef double		pdouble;

/**
  * @var pssize
  * @brief Type for a platform independent signed size_t.
  */

/**
  * @var psize
  * @brief Type for a platform independent size_t.
  */

/**
  * @def PSIZE_MODIFIER
  * @brief Platform dependent length modifier for conversion specifiers of
  * #psize or #pssize type for printing and scanning values. It is a string
  * literal, but doesn't include the percent sign so you can add precision and
  * length modifiers and append a conversion specifier.
  * @code
  * psize size_val = 256;
  * printf ("%#" PSIZE_MODIFIER "x", size_val);
  * @endcode
  */

/**
  * @def PSSIZE_FORMAT
  * @brief Platform dependent conversion specifier of #pssize type for printing
  * and scanning values.
  * @code
  * pssize size_val = 100;
  * printf ("%" PSSIZE_FORMAT, size_val);
  * @endcode
  */

/**
  * @def PSIZE_FORMAT
  * @brief Platform dependent conversion specifier of #psize type for printing
  * and scanning values.
  */

/**
  * @def P_MAXSIZE
  * @brief Maximum value of a #psize type.
  */

/**
  * @def P_MINSSIZE
  * @brief Minimum value of a #pssize type.
  */

/**
  * @def P_MAXSSIZE
  * @brief Maximum value of a #pssize type.
  */


typedef uintptr_t  pssize;
typedef size_t     psize;
// #define PSIZE_MODIFIER		"l"
// #define PSSIZE_FORMAT		"li"
// #define PSIZE_FORMAT		"lu"
#define P_MAXSIZE		SIZE_MAX
#define P_MINSSIZE		INTPTR_MIN
#define P_MAXSSIZE		INTPTR_MAX

/**
  * @var pintptr
  * @brief Type for a platform independent signed pointer represented by an
  * integer.
  */

/**
  * @var puintptr
  * @brief Type for a platform independent unsigned pointer represented by an
  * integer.
  */

/**
  * @def PINTPTR_MODIFIER
  * @brief Platform dependent length modifier for conversion specifiers of
  * #pintptr or #puintptr type for printing and scanning values. It is a string
  * literal, but doesn't include the percent sign so you can add precision and
  * length modifiers and append a conversion specifier.
  */

/**
  * @def PINTPTR_FORMAT
  * @brief Platform dependent conversion specifier of #pintptr type for printing
  * and scanning values.
  */

/**
  * @def PUINTPTR_FORMAT
  * @brief Platform dependent conversion specifier of #puintptr type for
  * printing and scanning values.
  */

typedef intptr_t  pintptr;
typedef uintptr_t  puintptr;
// #define PINTPTR_MODIFIER	"I64"
// #define PINTPTR_FORMAT	"I64i"
// #define PUINTPTR_FORMAT	"I64u"


/** Platform independent offset_t definition. */
typedef pint64 poffset;

#  define P_INT_TO_POINTER(i)	((void *)	(intptr_t) (i))
#  define P_POINTER_TO_INT(p)	((int)		(intptr_t) (p))
#  define PPOINTER_TO_INT(p)	((pint)		((intptr_t) (p)))
#  define PPOINTER_TO_UINT(p)	((puint)	((uintptr_t) (p)))
#  define PINT_TO_POINTER(i)	((ppointer)	(intptr_t) (i))
#  define PUINT_TO_POINTER(u)	((ppointer)	(uintptr_t) (u))

/**
 * @def P_INT_TO_POINTER
 * @brief Casts an int to a pointer.
 * @param i Variable to cast.
 * @return Casted variable.
 * @since 0.0.1
 */

/**
 * @def P_POINTER_TO_INT
 * @brief Casts a pointer to an int.
 * @param p Pointer to cast.
 * @return Casted pointer.
 * @since 0.0.1
 */

/**
 * @def PPOINTER_TO_INT
 * @brief Casts a #ppointer to a #pint value.
 * @param p #ppointer to cast.
 * @return Casted #ppointer.
 * @since 0.0.1
 */

 /**
  * @def PPOINTER_TO_UINT
  * @brief Casts a #ppointer to a #pint value.
  * @param p #ppointer to cast.
  * @return Casted #ppointer.
  * @since 0.0.1
  */

 /**
  * @def PINT_TO_POINTER
  * @brief Casts a #pint value to a #ppointer.
  * @param i #pint to cast.
  * @return Casted #pint.
  * @since 0.0.1
  */

 /**
  * @def PUINT_TO_POINTER
  * @brief Casts a #puint value to a #ppointer.
  * @param u #puint to cast.
  * @return Casted #puint.
  * @since 0.0.1
  */

/** Casts a #psize value to a #ppointer.	*/
#define PSIZE_TO_POINTER(i)	((ppointer)  ((psize) (i)))
/** Casts a #ppointer to a #psize value.	*/
#define PPOINTER_TO_PSIZE(p)	((psize)  (p))

/** Min value for a 8-bit int.			*/
#define P_MININT8	((pint8)  0x80)
/** Max value for a 8-bit int.			*/
#define P_MAXINT8	((pint8)  0x7F)
/** Max value for a 8-bit unsigned int.		*/
#define P_MAXUINT8	((puint8) 0xFF)

/** Min value for a 16-bit int.			*/
#define P_MININT16	((pint16)  0x8000)
/** Max value for a 16-bit int.			*/
#define P_MAXINT16	((pint16)  0x7FFF)
/** Max value for a 16-bit unsigned int.	*/
#define P_MAXUINT16	((puint16) 0xFFFF)

/** Min value for a 32-bit int.			*/
#define P_MININT32	((pint32)  0x80000000)
/** Max value for a 32-bit int.			*/
#define P_MAXINT32	((pint32)  0x7FFFFFFF)
/** Max value for a 32-bit unsigned int.	*/
#define P_MAXUINT32	((puint32) 0xFFFFFFFF)

/** Min value for a 64-bit int.			*/
#define P_MININT64	((pint64)  0x8000000000000000LL)
/** Max value for a 64-bit int.			*/
#define P_MAXINT64	((pint64)  0x7FFFFFFFFFFFFFFFLL)
/** Max value for a 64-bit unsigned int.	*/
#define P_MAXUINT64	((puint64) 0xFFFFFFFFFFFFFFFFULL)

/* Endianess and host/network byte order macroses completely removed */

#ifndef FALSE
/** Type definition for a false boolean value.	*/
#  define FALSE (0)
#endif

#ifndef TRUE
/** Type definition for a true boolean value.	*/
#  define TRUE (!FALSE)
#endif

/**
 * @brief Platform independent system handle.
 */
typedef void * P_HANDLE;

/**
 * @brief Function to traverse through a key-value container.
 * @param key The key of an item.
 * @param value The value of the item.
 * @param user_data Data provided by a user, maybe NULL.
 * @return FALSE to continue traversing, TRUE to stop it.
 * @since 0.0.1
 */
typedef pboolean (*PTraverseFunc) (ppointer key,
				   ppointer value,
				   ppointer user_data);

/**
 * @brief General purpose function.
 * @param data Main argument related to a context value.
 * @param user_data Additional (maybe NULL) user-provided data.
 * @since 0.0.1
 */
typedef void (*PFunc) (ppointer data, ppointer user_data);

/**
 * @brief Object destroy notification function.
 * @param data Pointer to an object to be destroyed.
 * @since 0.0.1
 */
typedef void (*PDestroyFunc) (ppointer data);

/**
 * @brief Compares two values.
 * @param a First value to compare.
 * @param b Second value to compare.
 * @return -1 if the first value is less than the second, 1 if the first value
 * is greater than the second, 0 otherwise.
 * @since 0.0.1
 */
typedef pint (*PCompareFunc) (pconstpointer a, pconstpointer b);

/**
 * @brief Compares two values with additional data.
 * @param a First value to compare.
 * @param b Second value to compare.
 * @param data Addition data, may be NULL.
 * @return -1 if the first value is less than the second, 1 if the first value
 * is greater than the second, 0 otherwise.
 * @since 0.0.1
 */
typedef pint (*PCompareDataFunc) (pconstpointer a, pconstpointer b, ppointer data);

P_END_DECLS

#endif /* PLIBSYS_HEADER_PTYPES_H */

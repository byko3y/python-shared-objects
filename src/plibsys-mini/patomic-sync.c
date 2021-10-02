/*
 * The MIT License
 *
 * Copyright (C) 2010-2017 Alexander Saprykin <saprykin.spb@gmail.com>
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

#include "patomic.h"

#ifdef P_CC_CRAY
#  include <intrinsics.h>
#endif

// https://gcc.gnu.org/onlinedocs/gcc/_005f_005fsync-Builtins.html#g_t_005f_005fsync-Builtins

P_LIB_API pint
p_atomic_int_get (const volatile pint *atomic)
{
	pint value = *atomic;
#ifdef P_CC_CRAY
	__builtin_ia32_mfence ();
#else
	__sync_synchronize ();
#endif
	return value;
}

P_LIB_API void
p_atomic_int_set (volatile pint	*atomic,
		  pint		val)
{
#ifdef P_CC_CRAY
	__builtin_ia32_mfence ();
#else
	__sync_synchronize ();
#endif
	*atomic = val;
#ifdef P_CC_CRAY
	__builtin_ia32_mfence ();
#else
	__sync_synchronize ();
#endif
}

P_LIB_API void
p_atomic_int_inc (volatile pint *atomic)
{
	(void) __sync_fetch_and_add (atomic, 1);
}

P_LIB_API pint
p_atomic_int_dec (volatile pint *atomic)
{
	return __sync_sub_and_fetch (atomic, 1);
}

P_LIB_API pboolean
p_atomic_int_dec_and_test (volatile pint *atomic)
{
	return __sync_fetch_and_sub (atomic, 1) == 1 ? TRUE : FALSE;
}

P_LIB_API pboolean
p_atomic_int_compare_and_exchange (volatile pint	*atomic,
				   pint			oldval,
				   pint			newval)
{
#ifdef P_CC_CRAY
	return __sync_val_compare_and_swap (atomic, oldval, newval) == oldval ? TRUE : FALSE;
#else
	return !! __sync_bool_compare_and_swap (atomic, oldval, newval);
#endif
}

/* New function */
P_LIB_API pint
p_atomic_int_exchange (volatile pint	*atomic,
			     pint		newval)
{
	while (TRUE)
	{
		pint oldval = p_atomic_int_get(atomic);
		if (p_atomic_int_compare_and_exchange(atomic, oldval, newval))
			return oldval;
	};
}

P_LIB_API pint
p_atomic_int_add (volatile pint	*atomic,
		      pint		val)
{
	return (pint) __sync_fetch_and_add (atomic, val);
}

P_LIB_API puint
p_atomic_int_and (volatile puint	*atomic,
		      puint			val)
{
	return (puint) __sync_fetch_and_and (atomic, val);
}

P_LIB_API puint
p_atomic_int_or (volatile puint	*atomic,
		 puint		val)
{
	return (puint) __sync_fetch_and_or (atomic, val);
}

P_LIB_API puint
p_atomic_int_xor (volatile puint	*atomic,
		      puint			val)
{
	return (puint) __sync_fetch_and_xor (atomic, val);
}

/* Moved barrier into acquire position */
P_LIB_API ppointer
p_atomic_pointer_get (const volatile void *atomic)
{
	ppointer rslt = *((const volatile ppointer *) atomic);
#ifdef P_CC_CRAY
	__builtin_ia32_mfence ();
#else
	__sync_synchronize ();
#endif
	return rslt;
}

P_LIB_API void
p_atomic_pointer_set (volatile void	*atomic,
			    ppointer		val)
{
#ifdef P_CC_CRAY
	__builtin_ia32_mfence ();
#else
	__sync_synchronize ();
#endif
	*((volatile ppointer *) atomic) = val;
#ifdef P_CC_CRAY
	__builtin_ia32_mfence ();
#else
	__sync_synchronize ();
#endif
}

P_LIB_API pboolean
p_atomic_pointer_compare_and_exchange (volatile void	*atomic,
						   ppointer		oldval,
						   ppointer		newval)
{
#ifdef P_CC_CRAY
	return __sync_val_compare_and_swap ((volatile ppointer *) atomic,
							(ppointer) oldval,
							(ppointer) newval) == ((ppointer) oldval) ? TRUE : FALSE;
#else
	return !! __sync_bool_compare_and_swap ((volatile ppointer *) atomic,
							    (ppointer) oldval,
							    (ppointer) newval);
#endif
}

/* New function */
P_LIB_API ppointer
p_atomic_pointer_exchange (volatile void	*atomic,
				   ppointer		newval)
{
	while (TRUE)
	{
		ppointer oldval = p_atomic_pointer_get(atomic);
		if (p_atomic_pointer_compare_and_exchange(atomic, oldval, newval))
			return oldval;
	};
	return NULL;
}

P_LIB_API psize p_atomic_pointer_add(volatile void	*atomic,
						 psize val)
{
	return __sync_fetch_and_add ((volatile psize *) atomic, val);
}

P_LIB_API psize
p_atomic_pointer_and (volatile void	*atomic,
			    psize		val)
{
	return (psize) __sync_fetch_and_and ((volatile psize *) atomic, val);
}

P_LIB_API psize
p_atomic_pointer_or (volatile void	*atomic,
			   psize		val)
{
	return (psize) __sync_fetch_and_or ((volatile psize *) atomic, val);
}

P_LIB_API psize
p_atomic_pointer_xor (volatile void	*atomic,
			    psize		val)
{
	return (psize) __sync_fetch_and_xor ((volatile psize *) atomic, val);
}

/* New function */
P_LIB_API pboolean
p_atomic_bit_test_and_set (volatile void	*atomic,
				   puint		bit)
{
	// Effectively lower endian
	volatile psize *tmp_pointer = (volatile psize *)atomic;
	psize mask = ((psize)(1)) << (bit & (sizeof(*tmp_pointer) * 8 - 1));
	psize oldval = __sync_fetch_and_or(&tmp_pointer[bit / (sizeof(*tmp_pointer) * 8)], mask);
	return (pboolean) ((oldval & mask) != 0);
}

/* New function */
P_LIB_API pboolean
p_atomic_bit_test_and_reset (volatile void	*atomic,
				     puint		bit)
{
	// Effectively lower endian
	volatile psize *tmp_pointer = (volatile psize *)atomic;
	psize mask = ((psize)(1)) << (bit & (sizeof(*tmp_pointer) * 8 - 1));
	psize oldval = __sync_fetch_and_and(&tmp_pointer[bit / (sizeof(*tmp_pointer) * 8)], ~mask);
	return (pboolean) ((oldval & mask) != 0);
}

P_LIB_API pboolean
p_atomic_is_lock_free (void)
{
	return TRUE;
}

void
p_atomic_thread_init (void)
{
}

void
p_atomic_thread_shutdown (void)
{
}

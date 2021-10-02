/*
 * The MIT License
 *
 * Copyright (C) 2016 Alexander Saprykin <saprykin.spb@gmail.com>
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

#ifdef P_CC_SUN
#  define PATOMIC_INT_CAST(x) (pint *) (x)
#  define PATOMIC_SIZE_CAST(x) (psize *) (x)
#else
#  define PATOMIC_INT_CAST(x) x
#  define PATOMIC_SIZE_CAST(x) x
#endif

// https://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html

P_LIB_API pint
p_atomic_int_get (const volatile pint *atomic)
{
    return (pint) __atomic_load_n (PATOMIC_INT_CAST (atomic), __ATOMIC_SEQ_CST);
}

P_LIB_API void
p_atomic_int_set (volatile pint	*atomic,
		  pint		val)
{
    __atomic_store_n (PATOMIC_INT_CAST (atomic), val, __ATOMIC_SEQ_CST);
}

P_LIB_API void
p_atomic_int_inc (volatile pint *atomic)
{
	(void) __atomic_fetch_add (atomic, 1, __ATOMIC_SEQ_CST);
}

P_LIB_API pint
p_atomic_int_dec (volatile pint *atomic)
{
    return __atomic_sub_fetch (atomic, 1, __ATOMIC_SEQ_CST);
}

P_LIB_API pboolean
p_atomic_int_dec_and_test (volatile pint *atomic)
{
	return (__atomic_fetch_sub (atomic, 1, __ATOMIC_SEQ_CST) == 1) ? TRUE : FALSE;
}

P_LIB_API pboolean
p_atomic_int_compare_and_exchange (volatile pint	*atomic,
				   pint			oldval,
				   pint			newval)
{
	pint tmp_int = oldval;

	return (pboolean) __atomic_compare_exchange_n (PATOMIC_INT_CAST (atomic),
						       &tmp_int,
						       newval,
						       0,
						       __ATOMIC_SEQ_CST,
						       __ATOMIC_SEQ_CST);
}

/* New function */
P_LIB_API pint
p_atomic_int_exchange (volatile pint	*atomic,
		       pint		newval)
{
	return __atomic_exchange_n (PATOMIC_INT_CAST (atomic), newval, __ATOMIC_SEQ_CST);
}

P_LIB_API pint
p_atomic_int_add (volatile pint	*atomic,
		  pint		val)
{
	return (pint) __atomic_fetch_add (atomic, val, __ATOMIC_SEQ_CST);
}

P_LIB_API puint
p_atomic_int_and (volatile puint	*atomic,
		  puint			val)
{
	return (puint) __atomic_fetch_and (atomic, val, __ATOMIC_SEQ_CST);
}

P_LIB_API puint
p_atomic_int_or (volatile puint	*atomic,
		 puint		val)
{
	return (puint) __atomic_fetch_or (atomic, val, __ATOMIC_SEQ_CST);
}

P_LIB_API puint
p_atomic_int_xor (volatile puint	*atomic,
		  puint			val)
{
	return (puint) __atomic_fetch_xor (atomic, val, __ATOMIC_SEQ_CST);
}

P_LIB_API ppointer
p_atomic_pointer_get (const volatile void *atomic)
{
	return (ppointer) __atomic_load_n ((const volatile ppointer *)atomic, __ATOMIC_SEQ_CST);
}

P_LIB_API void
p_atomic_pointer_set (volatile void	*atomic,
		      ppointer		val)
{
	__atomic_store_n ((volatile ppointer *)atomic, val, __ATOMIC_SEQ_CST);
}

P_LIB_API pboolean
p_atomic_pointer_compare_and_exchange (volatile void	*atomic,
				       ppointer		oldval,
				       ppointer		newval)
{
	ppointer tmp_pointer = oldval;

	return (pboolean) __atomic_compare_exchange_n ((volatile ppointer *)atomic,
							&tmp_pointer,
							// PPOINTER_TO_PSIZE (newval),
							newval,
							0,
							__ATOMIC_SEQ_CST,
							__ATOMIC_SEQ_CST);
}

/* New function */
P_LIB_API ppointer
p_atomic_pointer_exchange (volatile void	*atomic,
				       ppointer		newval)
{
	return __atomic_exchange_n ((volatile ppointer *)atomic, newval, __ATOMIC_SEQ_CST);
}

P_LIB_API psize
p_atomic_pointer_add (volatile void	*atomic,
		      psize		val)
{
	return __atomic_fetch_add ((volatile psize *) atomic, val, __ATOMIC_SEQ_CST);
}

P_LIB_API psize
p_atomic_pointer_and (volatile void	*atomic,
		      psize		val)
{
	return (psize) __atomic_fetch_and ((volatile psize *) atomic, val, __ATOMIC_SEQ_CST);
}

P_LIB_API psize
p_atomic_pointer_or (volatile void	*atomic,
		     psize		val)
{
	return (psize) __atomic_fetch_or ((volatile psize *) atomic, val, __ATOMIC_SEQ_CST);
}

P_LIB_API psize
p_atomic_pointer_xor (volatile void	*atomic,
		      psize		val)
{
	return (psize) __atomic_fetch_xor ((volatile psize *) atomic, val, __ATOMIC_SEQ_CST);
}

/* New function */
P_LIB_API pboolean
p_atomic_bit_test_and_set (volatile void	*atomic,
			   puint		bit)
{
	// Effectively lower endian
	volatile psize *tmp_pointer = (volatile psize *)atomic;
	psize mask = ((psize)(1)) << (bit & (sizeof(*tmp_pointer) * 8 - 1));
	psize oldval = __atomic_fetch_or(&tmp_pointer[bit / (sizeof(*tmp_pointer) * 8)], mask, __ATOMIC_SEQ_CST);
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
	psize oldval = __atomic_fetch_and(&tmp_pointer[bit / (sizeof(*tmp_pointer) * 8)], ~mask, __ATOMIC_SEQ_CST);
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

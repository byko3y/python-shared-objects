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

#ifndef PLIBSYS_HEADER_PMEM_H
#define PLIBSYS_HEADER_PMEM_H

#include <ptypes.h>
#include <pmacros.h>

/* The custom allocators are completely removed i.e. this file is a placeholder. */

#include <malloc.h>
#include <string.h> // memset

P_BEGIN_DECLS

/**
 * @brief Allocates a memory block for the specified number of bytes.
 * @param n_bytes Size of the memory block in bytes.
 * @return Pointer to a newly allocated memory block in case of success, NULL
 * otherwise.
 * @since 0.0.1
 */
static inline ppointer
p_malloc (psize n_bytes)
{
	if (P_LIKELY (n_bytes > 0))
		return malloc (n_bytes);
	else
		return NULL;
}

/**
 * @brief Allocates a memory block for the specified number of bytes and fills
 * it with zeros.
 * @param n_bytes Size of the memory block in bytes.
 * @return Pointer to a newly allocated memory block filled with zeros in case
 * of success, NULL otherwise.
 * @since 0.0.1
 */
static inline ppointer	p_malloc0		(psize			n_bytes)
{
	ppointer ret;

	if (P_LIKELY (n_bytes > 0)) {
		if (P_UNLIKELY ((ret = malloc (n_bytes)) == NULL))
			return NULL;

		memset (ret, 0, n_bytes);
		return ret;
	} else
		return NULL;
}


/**
 * @brief Changes the memory block size.
 * @param mem Pointer to the memory block.
 * @param n_bytes New size for @a mem block.
 * @return Pointer to a newlly allocated memory block in case of success (if
 * @a mem is NULL then it acts like p_malloc()), NULL otherwise.
 * @since 0.0.1
 */
static inline ppointer
p_realloc (ppointer mem, psize n_bytes)
{
	if (P_UNLIKELY (n_bytes == 0))
		return NULL;

	if (P_UNLIKELY (mem == NULL))
		return malloc (n_bytes);
	else
		return realloc (mem, n_bytes);
}

/**
 * @brief Frees a memory block by its pointer.
 * @param mem Pointer to the memory block to free.
 * @since 0.0.1
 *
 * You should only call this function for the pointers which were obtained using
 * the p_malloc(), p_malloc0() and p_realloc() routines, otherwise behavior is
 * unpredictable.
 *
 * Checks the pointer for the NULL value.
 */
static inline void
p_free (ppointer mem)
{
	if (P_LIKELY (mem != NULL))
		free (mem);
}

P_END_DECLS

#endif /* PLIBSYS_HEADER_PMEM_H */

/*
 * The MIT License
 *
 * Copyright (C) 2021 Pavel Kostyuchenko <byko3y@gmail.com>
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

#pragma once

#include "ptypes.h"
#include <stdbool.h>

#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
// Windows
    #include <windows.h>

    typedef unsigned long DWORD;
    typedef void *HANDLE;
    typedef HANDLE ShmEventHandle;
    typedef HANDLE ShmFileHandle;
    typedef DWORD ShmProcessID; // DWORD
    typedef DWORD ShmThreadID; // DWORD
    typedef HANDLE ShmProcessHandle;
    typedef HANDLE ShmHandle;
    typedef DWORD ShmTicks;
#else
// POSIX
    #include <sys/types.h>

    typedef int ShmFileHandle;
    typedef pid_t ShmProcessID;
    typedef pid_t ShmProcessHandle;
    typedef pid_t ShmThreadID;
#endif

#define vl volatile
#define vl2 
// naive ShmPointer routines inlining is not very efficient
// (approx x1.3 accounts.py run time with shmassertions enabled)
#define shminline inline

typedef pint ShmInt; // Small aligned integer value for atomic access
					 // typedef vl __ShmPointer ShmPointer;
typedef intptr_t ShmWord;
typedef uintptr_t __ShmPointer;
typedef vl uintptr_t ShmPointer; // ShmPointer addresses the process memory and should be same or larger than pointer
typedef vl uintptr_t *PShmPointer;

#define CAST_VL(x) ((void *)(uintptr_t)(volatile void *)(x))
#define isizeof(x) ((int)sizeof(x))
#define SHM_UNUSED(x)  (void)x

#define CHECK_FLAG(value, flag)  (flag == (value & flag))

#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
	ShmHandle
	handle_to_coordinator(ShmProcessID coordinator_process, ShmHandle handle, bool close);
#endif

// # define SHM_BLOCK_COUNT 1024*4
# define SHM_BLOCK_COUNT 2*1024
# define SHM_BLOCK_GROUP_SIZE 16
# define SHM_FIXED_CHUNK_SIZE 1024*1024
# define SHM_BLOCK_BITS 12
# define SHM_OFFSET_BITS 20
# define SHM_INVALID_BLOCK ((1 << SHM_BLOCK_BITS) - 1)
# define SHM_INVALID_OFFSET ((1 << SHM_OFFSET_BITS) - 1)

// MM
#define SIZE_CLASS_LARGEST 8
#define SIZE_CLASS_COUNT (SIZE_CLASS_LARGEST + 1)
#define MEDIUM_SIZE_CLASS_LARGEST 7
#define MEDIUM_SIZE_CLASS_COUNT (MEDIUM_SIZE_CLASS_LARGEST + 1)

#define SHM_SECTOR_SIZE  SHM_FIXED_CHUNK_SIZE
#define SHM_SEGMENT_SIZE  (4*1024)
#define SHM_SEGMENT_BITS  12
#define SHM_SEGMENTS_IN_SECTOR  (SHM_SECTOR_SIZE / SHM_SEGMENT_SIZE)

#define SHM_ALIGNMENT_BITS 3
// 8 = 1<<3
// extern ShmWord size_map[11] = { 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 0 };
extern ShmWord size_map_orig[SIZE_CLASS_COUNT+1];
extern ShmWord medium_size_map_orig[MEDIUM_SIZE_CLASS_COUNT + 1];

extern ShmWord max_heap_block_size;
extern ShmWord max_heap_small_block_size;
extern uintptr_t actual_flex_sector_size;
extern ShmWord size_map[SIZE_CLASS_COUNT + 1];
extern ShmWord medium_size_map[MEDIUM_SIZE_CLASS_COUNT + 1];

#define guard_bytes  0xCCCCCCCC

#define MAX_THREAD_COUNT 64


// 1 for 2-aligned, 2 for 4-aligned, 3 for 8-aligned, 4 for 16-aligned, 5 for 32-aligned, 6 for 64-aligned
static inline ShmWord
align_lower(ShmWord value, ShmWord bits)
{
	if (bits <= 0) return value;
	// just take upper bits
	ShmWord mask = ~((1 << bits) - 1);
	return value & mask;
}

static inline ShmWord
align_higher(ShmWord value, ShmWord bits)
{
	if (bits <= 0) return value;
	// take "higher bits"+1, except when lower bits are all zeroes
	value--;
	ShmWord mask = ~((1 << bits) - 1);
	return (value & mask) + (1 << bits);
}

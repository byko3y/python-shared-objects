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

#include "shm_utils.h"

#define SHM_SAFE_NAME_LENGTH 14

#define SHM_NAME_PREFIX  "pso_"
#define EVENT_NAME_PREFIX  "pso_"

void _make_filename(char *buf);

#define SHM_BLOCK_TYPE_SUPER ((0xB10C<<8) | 0)
#define SHM_BLOCK_TYPE_ROOT ((0xB10C<<8) | 1)
#define SHM_BLOCK_TYPE_THREAD_SECTOR ((0xB10C<<8) | 2)
// MM-only, chunks cannot be SHM_BLOCK_TYPE_THREAD_SEGMENT
#define SHM_BLOCK_TYPE_THREAD_SEGMENT ((0xB10C<<8) | 3)
#define SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX ((0xB10C<<8) | 4)

#define SHM_FIXED_CHUNK_HEADER_SIZE 16
typedef vl struct {
	ShmInt type;
	ShmInt used;
} ShmChunkHeader;

typedef struct {
	char id[SHM_SAFE_NAME_LENGTH + 1]; // FreeBSD (and perhaps other BSDs) limit names to 14 characters.
	ShmInt size;
	ShmFileHandle handle;
} __ShmChunk;

typedef vl __ShmChunk ShmChunk;

// Allocate/load superblock or a regular fixed-size block.
typedef struct {
	void *data;
	ShmFileHandle FileMapping;
} ChunkAllocationResult;

ChunkAllocationResult
shm_chunk_allocate(__ShmChunk *chunk, bool isnew, ShmInt memsize);
void
shm_chunk_register_handle(ShmChunk *chunk, ShmFileHandle FileMapping, ShmProcessID coordinator_process, bool i_am_the_coordinator);
void
shm_chunk_release(ShmChunk *chunk, vl void *view, bool is_coordinator);

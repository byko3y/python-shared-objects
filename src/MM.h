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

// #include "stdafx.h"
#include "assert.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
// #include <windows.h>
#include "shm_defs.h"
#include "shm_memory.h"

// MM

// superheap->heap sector (mmap chunk)->heap segment->heap block (get_mem)
// Heap sector is thread-bound. Each sector contains segments, every segment has fixed size and holds blocks of same size.
// Thus list of free segments is thread-segment type-bound.
// For simplicity we implement a segment-bound list with links to other segments of the same type.

// Segments are page-aligned and there is a header at the start of each page for validation/debugging purpose.
// Minimal page size is 4kB. 8kB pages and larger are still 4k-aligned. 
// https://en.wikipedia.org/wiki/Page_(computer_memory)#Multiple_page_sizes
// To save the pages, we merge segment and sector headers into one header.

typedef ShmInt ShmSectorOffset;
typedef ShmInt ShmSegmentOffset;

#define MM_INVALID_OFFSET ((ShmInt)-1)
#define mm_guard_bytes  ((ShmInt)0xBBBBBBBB)

typedef vl struct {
	ShmSegmentOffset next; // Segment offset. link to the next item in a stack-like structure. Zero is invalid (don't use -1).
	// debug
	ShmInt last_id;
	ShmInt prev_id; // so we know at least 2 last id-s
	ShmInt magick;
} ShmHeapBlockHeader;

#define SHM_HEAP_BLOCK_HEADER_SIZE  align_higher(sizeof(ShmHeapBlockHeader), SHM_ALIGNMENT_BITS)

// segments are SHM_SEGMENT_SIZE aligned
typedef vl struct {
	ShmChunkHeader base; // ShmChunkHeader->type either SHM_BLOCK_TYPE_THREAD_SECTOR or SHM_BLOCK_TYPE_THREAD_SECTOR here
	// Below are persistent fields, changed when the segment is allocated.
	ShmInt header_size;
	ShmPointer sector; // pointer to sector where this segment resides
	ShmSectorOffset next_segment; // Sector offset. Zero is valid, MM_INVALID_OFFSET(-1) is invalid.
	                              // Next segment with free blocks of same element_size and bound to the same thread and within the same sector.
	                              // Free segments belong to a separate class (element_size = 0).
	// Below are volatile fields
	ShmPointer lock;
	ShmInt element_size_class;
	ShmInt element_size; // without a header
	ShmSegmentOffset free_blocks_head; // segment offset. Zero invalid, MM_INVALID_OFFSET unused.
	ShmInt capacity; // free elements are linked, so we lazily allocate them.
	ShmInt max_capacity;
	ShmInt used_count;
	// here resides an array of blocks of element_size. Thus address of element is ((char*)header) + header->size + i*header->element_size
} ShmHeapSegmentHeader;

// sectors are SHM_FIXED_CHUNK_SIZE aligned
typedef vl struct {
	ShmHeapSegmentHeader segment_header;
	ShmInt lock;
	ShmPointer thread;
	ShmPointer heap; // usually resides in superblock, so its "shm_pointer_get_block() = SHM_INVALID_BLOCK"
	// bool empty[SHM_SEGMENTS_IN_SECTOR];
	ShmPointer next_sector; // global pointer
	ShmPointer prev_sector; // global pointer
	// linked lists of available segments grouped by their sizes, which are of power of two, 8 to 4096
	// ShmPointer here is just an offset within the sector.
	// 11-th head is for empty segments.
	ShmSectorOffset segments_heads[10]; // sector offset, zero is valid, MM_INVALID_OFFSET is invalid.
} ShmHeapSectorHeader;

// single block of memory, SHM_ALIGNMENT_BITS aligned
typedef vl struct {
	ShmPointer sector; // pointer to sector where this block resides
	ShmSectorOffset next_free; // sector offset, zero is valid, MM_INVALID_OFFSET is invalid.
	                           // Next free block of the same element_size and bound to the same thread and within the same sector.
	                           // Next goes from head to tail, prev goes from tail to head.
	ShmSectorOffset prev_free; // Doubly linked

	ShmSectorOffset next_block; // Zero is valid, MM_INVALID_OFFSET invalid, similar to ShmHeapSegmentHeader->next_segment
	ShmSectorOffset prev_block;

	ShmInt claimed;
	ShmInt size_class;

	// for debug purpose
	ShmInt size; // without a header
	ShmInt last_id;
	ShmInt prev_id; // so we know at least 2 last id-s
} ShmHeapFlexBlockHeader;

#define SHM_HEAP_FLEX_BLOCK_HEADER_SIZE align_higher(sizeof(ShmHeapFlexBlockHeader), SHM_ALIGNMENT_BITS)
#define flex_with_header_block_size(size_class)  (medium_size_map[size_class] + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE)
// padding for smallest block
// #define MEDIUM_BLOCK_MAX_PADDING (ShmWord)((1 << (SHM_ALIGNMENT_BITS + MEDIUM_SIZE_CLASS_COUNT - 1)) + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE)
// use actual_flex_sector_size instead

typedef vl struct {
	ShmChunkHeader base; // ShmChunkHeader->type = SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX
	ShmPointer self;
	ShmPointer thread;
	ShmPointer heap; // usually resides in superblock, so its "shm_pointer_get_block() = SHM_INVALID_BLOCK"
					 // bool empty[SHM_SEGMENTS_IN_SECTOR];
	ShmPointer next_sector; // global pointer
	ShmPointer prev_sector; // global pointer
	// Doubly linked linked lists of free blocks grouped by their sizes.
	ShmSectorOffset class_heads[MEDIUM_SIZE_CLASS_COUNT + 1]; // sector offset, zero is valid, SHM_INVALID_POINTER is invalid
	ShmSectorOffset class_tails[MEDIUM_SIZE_CLASS_COUNT + 1];
} ShmHeapFlexSectorHeader;

#define SHM_HEAP_FLEX_SECTOR_HEADER_SIZE align_higher(sizeof(ShmHeapFlexSectorHeader), SHM_ALIGNMENT_BITS)

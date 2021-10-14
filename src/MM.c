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

#include "MM.h"
#include "shm_types.h"

// 8 = 1<<3
// size_t size_map[11] = { 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 0 };
ShmWord size_map_orig[SIZE_CLASS_COUNT + 1] = { 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 0 };
ShmWord size_map[SIZE_CLASS_COUNT + 1] = { 0 };
ShmWord medium_size_map_orig[MEDIUM_SIZE_CLASS_COUNT + 1] = { 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 0 };
// medium_size_map[i + 1] + header_size = 2 * (medium_size_map[i] + header_size)
ShmWord medium_size_map[MEDIUM_SIZE_CLASS_COUNT + 1] = { 0 };
ShmWord max_heap_block_size = 0;
uintptr_t actual_flex_sector_size = 0;
ShmWord max_heap_small_block_size = 0;

void init_mm_maps()
{
	shmassert_msg(size_map[0] == 0, "Double initialization");
	if (size_map[0] != 0) return;
	ShmWord header_size = align_higher(sizeof(ShmHeapSectorHeader), SHM_ALIGNMENT_BITS);
	for (int i = 0; i < SIZE_CLASS_COUNT; ++i)
	{
		if (size_map_orig[i] > header_size * 3)
		{
			// header is too small and will leave the first segment empty. So we slightly lower the block size
			ShmWord capacity = (SHM_SEGMENT_SIZE - header_size) / (size_map_orig[i] + SHM_HEAP_BLOCK_HEADER_SIZE);
			ShmWord newsize = (SHM_SEGMENT_SIZE - header_size) / (capacity + 1);
			size_map[i] = align_lower(newsize, SHM_ALIGNMENT_BITS);
		}
		else
			size_map[i] = size_map_orig[i];
	}
	shmassert(size_map_orig[SIZE_CLASS_COUNT] == 0);
	size_map[SIZE_CLASS_COUNT] = size_map_orig[SIZE_CLASS_COUNT];

	/*
	const ShmWord flex_sector_data_size = SHM_FIXED_CHUNK_SIZE - SHM_HEAP_FLEX_SECTOR_HEADER_SIZE;
	for (int i = MEDIUM_SIZE_CLASS_LARGEST; i >= 0; --i)
	{
		ShmWord cnt = SHM_FIXED_CHUNK_SIZE / medium_size_map_orig[i];
		// We store exactly "cnt" amount of blocks in the sector-chunk i.e. power of two,
		// so every two smaller blocks (including headers) can fit into its larger parent.
		// We can't just use multiples of 4k (page size), because we have headers.
		medium_size_map[i] = align_lower(flex_sector_data_size / cnt - SHM_HEAP_FLEX_BLOCK_HEADER_SIZE, SHM_ALIGNMENT_BITS);
		if (i != MEDIUM_SIZE_CLASS_LARGEST && medium_size_map[i + 1] + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE < 2 * (medium_size_map[i] + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE))
		{
			// usually never happens
			shmassert(false);
			medium_size_map[i] = (medium_size_map[i + 1] + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE) / 2;
		}
	}
	medium_size_map[0] = align_lower(flex_sector_data_size / small_block_count - SHM_HEAP_FLEX_BLOCK_HEADER_SIZE, SHM_ALIGNMENT_BITS);
	*/
	// Simplified solution for block alignment:
	//   align smallest block boundaries by 8 bytes, then make larger blocks multiples of the smallest one.
	// Both medium_size_map_orig and SHM_FIXED_CHUNK_SIZE should be powers of two.
	const int largest_block_count = SHM_FIXED_CHUNK_SIZE / medium_size_map_orig[MEDIUM_SIZE_CLASS_LARGEST];
	const ShmWord flex_sector_data_size = SHM_FIXED_CHUNK_SIZE - SHM_HEAP_FLEX_SECTOR_HEADER_SIZE;
	const ShmWord largest_block_size_tmp = flex_sector_data_size / largest_block_count;
	const ShmWord smallest_block_size = align_lower(largest_block_size_tmp >> MEDIUM_SIZE_CLASS_LARGEST, SHM_ALIGNMENT_BITS);
	ShmWord full_block_size = smallest_block_size;
	for (int i = 0; i < MEDIUM_SIZE_CLASS_COUNT; i++)
	{
		medium_size_map[i] = full_block_size - SHM_HEAP_FLEX_BLOCK_HEADER_SIZE;
		shmassert(full_block_size <= medium_size_map_orig[i]);

		full_block_size = full_block_size * 2;
	}
	medium_size_map[MEDIUM_SIZE_CLASS_COUNT] = medium_size_map_orig[MEDIUM_SIZE_CLASS_COUNT];

	max_heap_block_size = medium_size_map[MEDIUM_SIZE_CLASS_LARGEST];
	max_heap_small_block_size = size_map[SIZE_CLASS_LARGEST];
	// Round the sector size by multiples of max_heap_block_size
	actual_flex_sector_size = (uintptr_t)((max_heap_block_size + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE) * largest_block_count + SHM_HEAP_FLEX_SECTOR_HEADER_SIZE);
	shmassert(actual_flex_sector_size < SHM_FIXED_CHUNK_SIZE);
	// shmassert(medium_size_map_orig[MEDIUM_SIZE_CLASS_LARGEST] - max_heap_block_size <= MEDIUM_BLOCK_MAX_PADDING);
}

int
classify_mem_size(ShmWord size)
{
	if (size == 0) return -1;
	//size = size - 1;
	//size = size >> 3; // 8 byte is lowest
	//for (int i = 0; i < 10; ++i)
	//{
	//	if (size == 0) return i;
	//	size = size >> 1;
	//}
	for (int i = 0; i < SIZE_CLASS_COUNT; ++i)
	{
		if (size < size_map[i])
			return i;
	}
	return 666; // too large
}

int
classify_mem_size_medium(ShmWord size)
{
	if (size == 0) return -1;
	shmassert(size > size_map[SIZE_CLASS_COUNT]);
	for (int i = 0; i < MEDIUM_SIZE_CLASS_COUNT; ++i)
	{
		if (size < medium_size_map[i])
			return i;
	}
	return 666; // too large
}

void
init_empty_segment(ShmHeapSegmentHeader *segment)
{
	segment->lock = 0;
	segment->element_size = 0;
	segment->element_size_class = SIZE_CLASS_COUNT;
	segment->free_blocks_head = 0; // no blocks allocated yet
	segment->capacity = 1; // so used_count < capacity
	segment->max_capacity = 0;
	segment->used_count = 0;
}

static int prev_sectors = 0;
static int prev_segments = 0;
static int prev_flex_sectors = 0;
static int sector_count = 0;
static int segment_count = 0;
static int flex_sector_count = 0;
static int block_in_new_segment = 0;
static int block_appended_to_segment = 0;
static int block_from_free_list = 0;
static int block_in_new_sector = 0;
static int prev_allocs = 0;
static int prev_frees = 0;
static int allocs = 0;
static int frees = 0;
static uint64_t get_mem_locking = 0;
static uint64_t get_mem_spinlocking = 0;

void
dump_mm_debug()
{
	// Should also output statistics about allocated and freed blocks of different size
	char sector_sign = '+';
	int sector_delta = sector_count - prev_sectors;
	if (sector_delta < 0)
	{
		sector_sign = '-';
		sector_delta = -sector_delta;
	}
	char segment_sign = '+';
	int segment_delta = segment_count - prev_segments;
	if (segment_delta < 0)
	{
		segment_sign = '-';
		segment_delta = -segment_delta;
	}
	char flex_sector_sign = '+';
	int flex_sector_delta = flex_sector_count - prev_flex_sectors;
	if (flex_sector_delta < 0)
	{
		flex_sector_sign = '-';
		flex_sector_delta = -flex_sector_delta;
	}

	char allocs_sign = '+';
	int allocs_delta = allocs - prev_allocs;
	if (allocs_delta < 0)
	{
		allocs_sign = '-';
		allocs_delta = -allocs_delta;
	}
	char frees_sign = '+';
	int frees_delta = frees - prev_frees;
	if (frees_delta < 0)
	{
		frees_sign = '-';
		frees_delta = -frees_delta;
	}
	fprintf(stderr, "   allocs %d(%c%d), frees %d(%c%d)\n", allocs, allocs_sign, allocs_delta, frees, frees_sign, frees_delta);
	fprintf(stderr, "   sectors %d(%c%d), segments %d(%c%d), flex sectors %d(%c%d)\n", sector_count, sector_sign, sector_delta, segment_count, segment_sign, segment_delta,
		flex_sector_count, flex_sector_sign, flex_sector_delta);
	fprintf(stderr, "   New sectors %d, new segments %d, new blocks %d, reused %d\n",
		block_in_new_sector, block_in_new_segment, block_appended_to_segment, block_from_free_list);
	prev_flex_sectors = flex_sector_count;
	prev_sectors = sector_count;
	prev_segments = segment_count;
	prev_allocs = allocs;
	prev_frees = frees;
	block_in_new_segment = 0;
	block_appended_to_segment = 0;
	block_from_free_list = 0;
	block_in_new_sector = 0;
	fprintf(stderr, "   get_mem locking time %9.3f ms, of which spinlocking %9.3f ms\n",
		get_mem_locking / 3500000.0, get_mem_spinlocking / 3500000.0);
	get_mem_locking = 0;
	get_mem_spinlocking = 0;
}

int
offset_to_int(volatile void *higher, volatile void *lower)
{
	intptr_t value = (intptr_t)higher - (intptr_t)lower;
	shmassert(value < INT_MAX);
	shmassert(value >= -1);
	return value;
}

int
alloc_sector(ShmHeap *heap, ShmHeapSectorHeader **newblock, ShmPointer *newblock_shm)
{
	// ShmHeapSectorList old;
	// old.head = heap->sectors.head;
	// old.tail = heap->sectors.tail;
	int newindex = -1;
	// relies on outer lock to ensure uninitialized shared memory block will not become visible and multiple thread won't overallocate redundant blocks on contention.
	int err = superblock_alloc_more(heap->thread, SHM_BLOCK_TYPE_THREAD_SECTOR, &newindex, -2);
	shmassert(err != RESULT_FAILURE);
	if (err == RESULT_REPEAT)
		return err;
	shmassert(newindex >= 0);
	shmassert(superblock_get_block_noside(newindex));
	if (err == RESULT_OK && newindex >= 0)
	{
		sector_count++;
		ShmHeapSectorHeader *header = superblock_get_block(newindex);
		char *header_pntr = CAST_VL(header);
		memset(header_pntr + SHM_FIXED_CHUNK_HEADER_SIZE, 0x73, SHM_FIXED_CHUNK_SIZE - SHM_FIXED_CHUNK_HEADER_SIZE);
		init_empty_segment(&header->segment_header);
		// init persistent fields
		// header->segment_header.type = SHM_BLOCK_TYPE_THREAD_SECTOR;
		// type already inited by superblock_alloc_more
		shmassert(header->segment_header.base.type == SHM_BLOCK_TYPE_THREAD_SECTOR);
		header->segment_header.header_size = align_higher(sizeof(ShmHeapSectorHeader), SHM_ALIGNMENT_BITS);
		// header->segment_header
		header->lock = 2;
		header->next_sector = EMPTY_SHM;
		header->prev_sector = EMPTY_SHM;
		header->thread = heap->thread;
		header->heap = pack_shm_pointer(offset_to_int(heap, superblock), SHM_INVALID_BLOCK);
		// header->segments_heads

		// append the new sector into double-linked list
		shmassert(heap->fixed_sectors.head != NONE_SHM);
		if (heap->fixed_sectors.head == EMPTY_SHM)
		{
			// create a new head
			// for empty list both head and tail are invalid pointers. Otherwise they should both stay correct.
			shmassert(heap->fixed_sectors.tail == EMPTY_SHM);
			heap->fixed_sectors.head = pack_shm_pointer(0, newindex);
			heap->fixed_sectors.tail = heap->fixed_sectors.head;
			header->prev_sector = EMPTY_SHM;
			header->next_sector = EMPTY_SHM;
		}
		else
		{
			shmassert(heap->fixed_sectors.tail != EMPTY_SHM);
			ShmHeapSectorHeader *prev_sector = shm_pointer_to_pointer_unsafe(heap->fixed_sectors.tail);
			shmassert(prev_sector);
			shmassert_msg(prev_sector->next_sector == EMPTY_SHM, "Tail has a next element");
			shmassert(prev_sector->segment_header.base.type == SHM_BLOCK_TYPE_THREAD_SECTOR);
			prev_sector->next_sector = pack_shm_pointer(0, newindex);
			header->prev_sector = heap->fixed_sectors.tail;
			heap->fixed_sectors.tail = pack_shm_pointer(0, newindex);
		}

		for (int i = 0; i < SIZE_CLASS_COUNT; ++i)
			header->segments_heads[i] = MM_INVALID_OFFSET;
		header->segments_heads[SIZE_CLASS_COUNT] = 0; // first segment

		// link the segments
		for (int i = 0; i < SHM_SEGMENTS_IN_SECTOR; ++i)
		{
			ShmHeapSegmentHeader *segment = (ShmHeapSegmentHeader *)((intptr_t)header + SHM_SEGMENT_SIZE * i);
			if (i != 0)
			{
				// shmassert(segment->type == 0, NULL);
				// shmassert(segment->header_size == 0, NULL);
				segment->base.type = SHM_BLOCK_TYPE_THREAD_SEGMENT;
				segment->header_size = align_higher(sizeof(ShmHeapSegmentHeader), SHM_ALIGNMENT_BITS);
			}
			else
			{
				shmassert(segment->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR);
				shmassert(segment->header_size == align_higher(sizeof(ShmHeapSectorHeader), SHM_ALIGNMENT_BITS));
			}

			segment->sector = pack_shm_pointer(0, newindex);
			if (i < SHM_SEGMENTS_IN_SECTOR - 1)
				segment->next_segment = SHM_SEGMENT_SIZE * (i + 1);
			else
				segment->next_segment = MM_INVALID_OFFSET;

			init_empty_segment(segment);
		}
		if (newblock)
			*newblock = header;
		ShmPointer shm = pack_shm_pointer(0, newindex);
		if (newblock_shm)
			*newblock_shm = shm;
		shmassert(header == shm_pointer_to_pointer_unsafe(shm));
		return RESULT_OK;
	}
	return RESULT_FAILURE;
}

ShmHeapSegmentHeader *
get_segment_pointer(ShmHeapSectorHeader *sector, ShmSectorOffset offset)
{
	if (offset == MM_INVALID_OFFSET)
		return NULL;
	return (ShmHeapSegmentHeader *)((intptr_t)sector + offset);
}

ShmHeapBlockHeader *
get_block_pointer(ShmHeapSegmentHeader *segment, ShmSegmentOffset offset)
{
	if (offset == 0 || offset == MM_INVALID_OFFSET)
		return NULL;
	return (ShmHeapBlockHeader *)((intptr_t)segment + offset);
}

ShmHeapFlexBlockHeader *
get_flex_block_pointer(ShmHeapFlexSectorHeader *sector, ShmSectorOffset offset)
{
	if (offset == MM_INVALID_OFFSET)
		return NULL;
	shmassert(offset >= SHM_HEAP_FLEX_SECTOR_HEADER_SIZE);
	return (ShmHeapFlexBlockHeader *)((intptr_t)sector + offset);
}

ShmHeapBlockHeader *
segment_alloc_block(ShmHeapSegmentHeader *segment)
{
	if (segment->capacity < segment->max_capacity)
	{
		int idx = segment->capacity;
		segment->capacity++;
		ShmHeapBlockHeader *block = (ShmHeapBlockHeader *)((intptr_t)segment + segment->header_size +
			(SHM_HEAP_BLOCK_HEADER_SIZE + segment->element_size) * idx);
		segment->used_count++;

		block->next = 0;
		block->prev_id = -1;
		block->last_id = -1;
		block->magick = mm_guard_bytes;
		return block;
	}
	return NULL;
}

ShmHeapBlockHeader *
sector_find_free_block(ShmHeapSectorHeader *sector, int size)
{
	ShmHeapSegmentHeader *segment = get_segment_pointer(sector, sector->segments_heads[size]);
	ShmHeapSegmentHeader *prev = NULL;

	while (segment)
	{
		shmassert(segment->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR || segment->base.type == SHM_BLOCK_TYPE_THREAD_SEGMENT);
		if (segment->used_count < segment->max_capacity)
		{
			ShmHeapBlockHeader *block = get_block_pointer(segment, segment->free_blocks_head);
			if (block)
			{
				// pop the element from the list
				segment->free_blocks_head = block->next;
				block->next = 0;
				segment->used_count++;
			}
			//else if (!block)
			//{
			//	block = segment_alloc_block(segment);
			//}
			if (block)
			{
				if (segment->used_count == segment->max_capacity)
				{
					// remove this segment from list of available ones
					if (prev)
						prev->next_segment = segment->next_segment;
					else
						sector->segments_heads[size] = segment->next_segment;
				}
				return block;
			}
		}
		// next available segment
		prev = segment;
		if (segment->next_segment != MM_INVALID_OFFSET)
			segment = (ShmHeapSegmentHeader *)((intptr_t)sector + segment->next_segment);
		else
			segment = NULL;
	}

	return NULL;
}


ShmHeapBlockHeader *
sector_alloc_in_segment(ShmHeapSectorHeader *sector, int size)
{
	ShmHeapSegmentHeader *segment = get_segment_pointer(sector, sector->segments_heads[size]);
	ShmHeapSegmentHeader *prev = NULL;

	while (segment)
	{
		shmassert(segment->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR || segment->base.type == SHM_BLOCK_TYPE_THREAD_SEGMENT);
		if (segment->used_count < segment->max_capacity)
		{
			ShmHeapBlockHeader *block = segment_alloc_block(segment);
			if (block)
			{
				if (segment->used_count == segment->max_capacity)
				{
					// remove this segment from list of available ones
					if (prev)
						prev->next_segment = segment->next_segment;
					else
						sector->segments_heads[size] = segment->next_segment;
				}
				return block;
			}
		}
		// next available segment
		prev = segment;
		if (segment->next_segment != MM_INVALID_OFFSET)
			segment = (ShmHeapSegmentHeader *)((intptr_t)sector + segment->next_segment);
		else
			segment = NULL;
	}

	return NULL;
}

int
sector_claim_segment(ShmHeapSectorHeader *run, ShmHeapSegmentHeader *segment, int size)
{
	shmassert(segment->element_size == 0);
	shmassert(segment->element_size_class == SIZE_CLASS_COUNT);
	segment->element_size = size_map[size];
	segment->element_size_class = size;
	segment->max_capacity = (SHM_SEGMENT_SIZE - segment->header_size) / (SHM_HEAP_BLOCK_HEADER_SIZE + segment->element_size);
	segment->capacity = 0;
	segment->used_count = 0;
	segment->free_blocks_head = 0;
	segment_count++;
	return RESULT_OK;
}

ShmHeapSegmentHeader *
block_get_segment(ShmHeapBlockHeader *block)
{
	if (block == NULL)
		return NULL;
	uintptr_t val = (uintptr_t)block;
	val = (val >> SHM_SEGMENT_BITS) << SHM_SEGMENT_BITS;
	ShmHeapSegmentHeader *segment = (ShmHeapSegmentHeader *)val;
	shmassert(segment->base.type == SHM_BLOCK_TYPE_THREAD_SEGMENT || segment->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR);
	return segment;
}

ShmHeapSectorHeader *
segment_get_sector(ShmHeapSegmentHeader *segment)
{
	if (segment == NULL)
		return NULL;
	shmassert(SBOOL(segment->sector));
	if ( ! SBOOL(segment->sector) )
		return NULL;
	shmassert(shm_pointer_get_offset(segment->sector) == 0);
	ShmHeapSectorHeader *sector = shm_pointer_to_pointer_unsafe(segment->sector);
	shmassert(sector->segment_header.base.type == SHM_BLOCK_TYPE_THREAD_SECTOR);
	return sector;
}

ShmHeapFlexSectorHeader *
flex_block_get_sector(ShmHeapFlexBlockHeader *block)
{
	if (block == NULL)
		return NULL;
	shmassert(block->sector != NONE_SHM && block->sector != EMPTY_SHM);
	shmassert(shm_pointer_get_offset(block->sector) == 0);
	ShmHeapFlexSectorHeader *sector = shm_pointer_to_pointer_unsafe(block->sector);
	// Verify the block is within the sector
	shmassert((intptr_t)block - (intptr_t)sector > 0);
	shmassert((intptr_t)block - (intptr_t)sector < SHM_FIXED_CHUNK_SIZE);
	shmassert(sector->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX);
	return sector;
}

vl char *
data_get_flex_block(vl void *data)
{
	if (data == NULL)
		return NULL;
    ShmHeapFlexBlockHeader *block = (ShmHeapFlexBlockHeader *)((intptr_t)data - SHM_HEAP_FLEX_BLOCK_HEADER_SIZE);
	shmassert(block->sector != NONE_SHM && block->sector != EMPTY_SHM);
	shmassert(shm_pointer_get_offset(block->sector) == 0);
	return (vl char *)block;
}

ShmHeapBlockHeader *
allocate_block_of_size(ShmHeap *heap, int size, ShmHeapSectorHeader **sector)
{
	// int blocks_count = SHM_SEGMENT_SIZE;
	shmassert(heap);
	shmassert(heap->size == sizeof(ShmHeap));
	shmassert(size >= 0 && size < SIZE_CLASS_COUNT);
	int cycle = 0;
	ShmHeapBlockHeader *block = NULL;
	for (cycle = 20; cycle > 0; --cycle)
	{
		block = NULL;
		{
			shmassert(heap->fixed_sectors.head != NONE_SHM);
			ShmHeapSectorHeader *run = shm_pointer_to_pointer_unsafe(heap->fixed_sectors.head);
			while (run)
			{
				shmassert(run->segment_header.base.type == SHM_BLOCK_TYPE_THREAD_SECTOR);

				block = sector_find_free_block(run, size);
				if (block)
				{
					shmassert(segment_get_sector(block_get_segment(block)) == run);
					if (sector)
						*sector = run;
					block_from_free_list++;
					break; // while (run)
				}

				run = shm_pointer_to_pointer_unsafe(run->next_sector);
			}
		}
		if (!block)
		{
			ShmHeapSectorHeader *run = shm_pointer_to_pointer_unsafe(heap->fixed_sectors.head);
			while (run)
			{
				shmassert(run->segment_header.base.type == SHM_BLOCK_TYPE_THREAD_SECTOR);

				block = sector_alloc_in_segment(run, size);
				if (block)
				{
					shmassert(segment_get_sector(block_get_segment(block)) == run);
					if (sector)
						*sector = run;
					block_appended_to_segment++;
					break; // while (run)
				}

				run = shm_pointer_to_pointer_unsafe(run->next_sector);
			}
		}
		if (!block)
		{
			// now try to allocate a new segment
			ShmHeapSectorHeader *run = shm_pointer_to_pointer_unsafe(heap->fixed_sectors.head);
			while (run)
			{
				shmassert(run->segment_header.base.type == SHM_BLOCK_TYPE_THREAD_SECTOR);

				ShmHeapSegmentHeader *segment = get_segment_pointer(run, run->segments_heads[SIZE_CLASS_COUNT]);
				if (segment)
				{
					// remove it from empty segments
					run->segments_heads[SIZE_CLASS_COUNT] = segment->next_segment;
					sector_claim_segment(run, segment, size);
					// append into appropriate class
					segment->next_segment = run->segments_heads[size];
					run->segments_heads[size] = offset_to_int(segment, run);

					block = segment_alloc_block(segment);
					shmassert_msg(block, "Could not allocate a new block in an empty segment");
					shmassert(block_get_segment(block) == segment);
					shmassert(segment_get_sector(block_get_segment(block)) == run);
					if (sector)
						*sector = run;
					block_in_new_segment++;
					break; // while (run)
				}
				run = shm_pointer_to_pointer_unsafe(run->next_sector);
			}
		}
		if (!block)
		{
			// No free segments. Allocate new sector.
			ShmHeapSegmentHeader *segment = NULL;
			ShmHeapSectorHeader *run = NULL;
			// no free segments. Get a new sector.
			// Warning: alloc_sector might return RESULT_REPEAT for a prolonger period of time due to reclamation thread working.
			if (alloc_sector(heap, &run, NULL) == RESULT_REPEAT) continue; // for (cycle...)
			segment = get_segment_pointer(run, run->segments_heads[SIZE_CLASS_COUNT]);
			shmassert(segment);

			run->segments_heads[SIZE_CLASS_COUNT] = segment->next_segment;
			sector_claim_segment(run, segment, size);
			segment->next_segment = run->segments_heads[size];
			run->segments_heads[size] = offset_to_int(segment, run);

			block = segment_alloc_block(segment);
			shmassert_msg(block, "Could not allocate a new block in an empty segment");
			shmassert(block_get_segment(block) == segment);
			shmassert(segment_get_sector(block_get_segment(block)) == run);
			if (sector)
				*sector = run;
			block_in_new_sector++;
		}
		if (block) break; // for (cycle...)
	}

	shmassert_msg(cycle != 0, "possibly infinite loop in allocate_block_of_size()");
	shmassert(block);
	return block;
}

void
reclaim_small_block(ShmHeapBlockHeader *block, ShmHeapSegmentHeader *segment, ShmHeapSectorHeader *sector)
{
	// append the freed block first so it will be cached when used again
	shmassert(block->next == 0);
	block->next = segment->free_blocks_head;
	segment->free_blocks_head = offset_to_int(block, segment);
	shmassert(segment->free_blocks_head < SHM_SEGMENT_SIZE);

	shmassert(segment->used_count <= segment->max_capacity);
	if (segment->used_count == segment->max_capacity)
	{
		// append the segment into free segments list as a first one
		shmassert(segment->element_size_class >= 0 && segment->element_size_class < SIZE_CLASS_COUNT);
		segment->next_segment = sector->segments_heads[segment->element_size_class];
		sector->segments_heads[segment->element_size_class] = offset_to_int(segment, sector);
	};
	segment->used_count--;
}

void
init_medium_block(ShmHeapFlexBlockHeader *block, ShmPointer sector, ShmInt size_class)
{
	block->sector = sector; // pointer to sector where this block resides
	block->next_free = MM_INVALID_OFFSET;
	block->prev_free = MM_INVALID_OFFSET;

	block->next_block = MM_INVALID_OFFSET;
	block->prev_block = MM_INVALID_OFFSET;

	block->claimed = false;
	block->size_class = size_class;

	// for debug purpose
	block->size = -1;
    block->last_id = -1;
    block->prev_id = -1;
}

void
validate_medium_block_fast(ShmHeapFlexBlockHeader *block, ShmHeapFlexSectorHeader *sector, ShmPointer sector_shm)
{
	if (block)
	{
		shmassert(((uintptr_t)block % (1 << SHM_ALIGNMENT_BITS)) == 0);
		shmassert((uintptr_t)block > (uintptr_t)sector);
		shmassert((uintptr_t)block < (uintptr_t)sector + actual_flex_sector_size);
		puint full_size = (puint)SHM_HEAP_FLEX_BLOCK_HEADER_SIZE;
		if (block->size > 0)
			full_size += (puint)block->size;

		shmassert((uintptr_t)block + full_size <= (uintptr_t)sector + actual_flex_sector_size);
		shmassert(block->sector == sector_shm);
		shmassert(block->size_class >= 0 && block->size_class < MEDIUM_SIZE_CLASS_COUNT);
		shmassert(block->prev_block >= MM_INVALID_OFFSET);
		shmassert(block->next_block >= MM_INVALID_OFFSET);
		shmassert(block->prev_free >= MM_INVALID_OFFSET);
		shmassert(block->next_free >= MM_INVALID_OFFSET);
	}
}

void
validate_medium_block(ShmHeapFlexBlockHeader *block, ShmHeapFlexSectorHeader *sector, ShmPointer sector_shm)
{
	if (block)
	{
		validate_medium_block_fast(block, sector, sector_shm);
		// Cross-check via "next_block" and "medium_size_map".
		// Every block is aligned by smallest block size (approx 4k).
		uintptr_t next_block = (uintptr_t)block + (uintptr_t)flex_with_header_block_size(block->size_class);
		// Let's speed things up slightly.
		// validate_medium_block_fast((ShmHeapFlexBlockHeader *)next_block, sector, sector_shm);
		if (next_block < (uintptr_t)sector + actual_flex_sector_size)
			shmassert(block->next_block == MM_INVALID_OFFSET ||
			         (uintptr_t)(get_flex_block_pointer(sector, block->next_block)) == next_block);

		intptr_t prev_block_offset = block->prev_block;
		if (prev_block_offset != MM_INVALID_OFFSET)
		{
			intptr_t prev_block = (intptr_t)sector + prev_block_offset;
			ShmHeapFlexBlockHeader *prev_block_header = (ShmHeapFlexBlockHeader *)prev_block;
			validate_medium_block_fast(prev_block_header, sector, sector_shm);
			shmassert(prev_block_header->next_block != MM_INVALID_OFFSET);
			shmassert((intptr_t)sector + prev_block_header->next_block == (intptr_t)block);
			intptr_t me = (intptr_t)prev_block + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE + medium_size_map[prev_block_header->size_class];
			shmassert(me == (intptr_t)block);
		}
	}
}

void
validate_flex_sector_slow(ShmHeapFlexSectorHeader *sector, ShmPointer sector_shm)
{
	intptr_t pntr = (intptr_t)sector + SHM_HEAP_FLEX_SECTOR_HEADER_SIZE;
	intptr_t last = (intptr_t)sector + (intptr_t)actual_flex_sector_size;
	ShmHeapFlexBlockHeader *prev_block = NULL;
	while (pntr < last)
	{
		ShmHeapFlexBlockHeader *block = (ShmHeapFlexBlockHeader *)pntr;
		validate_medium_block(block, sector, sector_shm);

		pntr += medium_size_map[block->size_class] + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE;
		shmassert((prev_block != NULL) == (block->prev_block != MM_INVALID_OFFSET));
		if (prev_block)
			shmassert((intptr_t)prev_block == (intptr_t)sector + block->prev_block);

		prev_block = block;
	}
}

int
alloc_flex_sector(ShmHeap *heap, ShmHeapFlexSectorHeader **sector, ShmPointer *sector_shm)
{
	for (int cycle = 10; cycle > 0; --cycle)
	{
		// ShmHeapSectorList old;
		// old.head = heap->sectors.head;
		// old.tail = heap->sectors.tail;

		// relies on outer lock to ensure uninitialized shared memory block will not become visible and multiple thread won't overallocate redundant blocks on contention.
		shmassert(shm_lock_owned(&heap->lock));
		int newindex = -1;
		int err = superblock_alloc_more(heap->thread, SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX, &newindex, -2);
		shmassert(err != RESULT_FAILURE);
		if (err == RESULT_REPEAT)
			continue;
		shmassert(newindex >= 0);
		shmassert(superblock_get_block_noside(newindex));
		if (err == RESULT_OK && newindex >= 0)
		{
			flex_sector_count++;
			intptr_t header_pntr = ((intptr_t)(vl void *)superblock_get_block(newindex));
			memset((char *)(header_pntr + SHM_FIXED_CHUNK_HEADER_SIZE), 0x73, SHM_FIXED_CHUNK_SIZE - SHM_FIXED_CHUNK_HEADER_SIZE);
			ShmHeapFlexSectorHeader *sector_header = (ShmHeapFlexSectorHeader *)header_pntr;
			// init_flex_sector
			shmassert(sector_header->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX);
			sector_header->self = pack_shm_pointer(0, newindex);
			sector_header->thread = heap->thread;
			sector_header->heap = sector_header->heap = pack_shm_pointer(offset_to_int(heap, superblock), SHM_INVALID_BLOCK);
			sector_header->next_sector = EMPTY_SHM;
			sector_header->prev_sector = EMPTY_SHM;
			for (int i = 0; i < MEDIUM_SIZE_CLASS_COUNT + 1; ++i)
			{
				sector_header->class_heads[i] = MM_INVALID_OFFSET;
				sector_header->class_tails[i] = MM_INVALID_OFFSET;
			}
			shmassert(heap->flex_sectors.head != NONE_SHM);
			shmassert((heap->flex_sectors.head != EMPTY_SHM) == (heap->flex_sectors.tail != EMPTY_SHM));
			if (SBOOL(heap->flex_sectors.head))
			{
				// prepend the new head
				ShmHeapFlexSectorHeader *old_head = shm_pointer_to_pointer_unsafe(heap->flex_sectors.head);
				shmassert(old_head);
				shmassert(old_head->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX);
				shmassert_msg(old_head->prev_sector == EMPTY_SHM, "Head has a prev element");
				sector_header->next_sector = heap->flex_sectors.head;
				old_head->prev_sector = sector_header->self;
				heap->flex_sectors.head = sector_header->self;
			}
			else
			{
				heap->flex_sectors.head = sector_header->self;
				heap->flex_sectors.tail = sector_header->self;
			}

			int block_count = SHM_FIXED_CHUNK_SIZE / flex_with_header_block_size(MEDIUM_SIZE_CLASS_LARGEST);
			// shmassert(SHM_FIXED_CHUNK_SIZE - (SHM_HEAP_FLEX_BLOCK_HEADER_SIZE + medium_size_map[MEDIUM_SIZE_CLASS_LARGEST]) * block_count <=
			// 	(MEDIUM_BLOCK_MAX_PADDING*block_count));
			intptr_t new_block_offset = (intptr_t)sector_header + SHM_HEAP_FLEX_SECTOR_HEADER_SIZE;
			ShmHeapFlexBlockHeader *prev_free_block = NULL;
			ShmHeapFlexBlockHeader *prev_block = NULL;
			for (int block_index = 0; block_index < block_count; ++block_index)
			{
				shmassert((uintptr_t)sector_header + actual_flex_sector_size >=
				         (uintptr_t)new_block_offset + (puint)flex_with_header_block_size(MEDIUM_SIZE_CLASS_LARGEST));
				ShmHeapFlexBlockHeader *new_block = (ShmHeapFlexBlockHeader *)new_block_offset;

				init_medium_block(new_block, sector_header->self, MEDIUM_SIZE_CLASS_LARGEST);
				if (block_index == 0)
					sector_header->class_heads[MEDIUM_SIZE_CLASS_LARGEST] = offset_to_int(new_block, sector_header);
				else
				{
					if (prev_free_block)
					{
						// link
						prev_free_block->next_free = offset_to_int(new_block, sector_header);
						new_block->prev_free = offset_to_int(prev_free_block, sector_header);
					}
					if (block_index == block_count - 1)
						sector_header->class_tails[MEDIUM_SIZE_CLASS_LARGEST] = offset_to_int(new_block, sector_header);
				}

				if (prev_block)
				{
					prev_block->next_block = offset_to_int(new_block, sector_header);
					new_block->prev_block = offset_to_int(prev_block, sector_header);
				}

				new_block_offset += flex_with_header_block_size(MEDIUM_SIZE_CLASS_LARGEST);
				prev_free_block = new_block;
				prev_block = new_block;
			}

			validate_flex_sector_slow(sector_header, pack_shm_pointer(0, newindex));

			*sector = sector_header;
			*sector_shm = pack_shm_pointer(0, newindex);
			return RESULT_OK;
		}
		else
			continue;
	}
	shmassert_msg(false, "Too long loop allocating a new flex sector");
	return RESULT_FAILURE;
}

void
flex_block_append_free_to_head(ShmHeapFlexBlockHeader *new_block, int size_class, ShmHeapFlexSectorHeader *sector, ShmPointer sector_shm)
{
	ShmSectorOffset old_head = sector->class_heads[size_class];
	ShmSectorOffset old_tail = sector->class_tails[size_class];
	ShmHeapFlexBlockHeader *old_head_block = get_flex_block_pointer(sector, old_head);
	validate_medium_block_fast(old_head_block, sector, sector_shm);
	shmassert((old_tail == MM_INVALID_OFFSET) == (old_head == MM_INVALID_OFFSET));
	shmassert((old_head_block == NULL) == (old_tail == MM_INVALID_OFFSET));
	if (old_head_block)
		shmassert((old_head_block->prev_free == MM_INVALID_OFFSET));

	int new_block_offset = offset_to_int(new_block, sector);
	shmassert(0 <= new_block_offset && new_block_offset < INT_MAX);
	// insert the new block
	new_block->next_free = old_head;
	// There is no validation for next_free/prev_free list. Old code had next_free herre instead of prev_free
	new_block->prev_free = MM_INVALID_OFFSET;
	if (old_head_block)
	{
		old_head_block->prev_free = new_block_offset;
	}
	sector->class_heads[size_class] = new_block_offset;
	// if (sector->class_tails[splitting_size] == MM_INVALID_OFFSET)
	// same:
	if (old_tail == MM_INVALID_OFFSET)
		sector->class_tails[size_class] = new_block_offset;
}

ShmHeapFlexBlockHeader *
try_alloc_medium_block(ShmHeap *heap, int target_size_class, int size_class, ShmHeapFlexSectorHeader *sector, ShmPointer sector_shm)
{
	ShmHeapFlexBlockHeader *block = get_flex_block_pointer(sector, sector->class_heads[size_class]);
	if (block)
	{
		shmassert(block->size_class == size_class);
		validate_medium_block((ShmHeapFlexBlockHeader *)block, sector, sector_shm);
		int acquired_block_size = flex_with_header_block_size(block->size_class);
		{
			// validate prev-next and extract the block
			ShmHeapFlexBlockHeader *next_free_block = get_flex_block_pointer(sector, block->next_free);
			ShmHeapFlexBlockHeader *prev_free_block = get_flex_block_pointer(sector, block->prev_free);
			validate_medium_block_fast(block, sector, sector_shm);
			validate_medium_block_fast(prev_free_block, sector, sector_shm);
			validate_medium_block_fast(next_free_block, sector, sector_shm);

			// Verify all the three blocks are free.
			uintptr_t following_block = (uintptr_t)block + acquired_block_size;
			if (following_block - (uintptr_t)sector < actual_flex_sector_size)
				validate_medium_block_fast((ShmHeapFlexBlockHeader *)following_block, sector, sector_shm);

			// item is either linked or a tail
			shmassert(prev_free_block || get_flex_block_pointer(sector, sector->class_heads[size_class]) == block);
			shmassert(next_free_block || get_flex_block_pointer(sector, sector->class_tails[size_class]) == block);

			// validate_flex_sector_slow(sector, sector_shm);

			// remove the old block from doubly linked list of free blocks
			if (next_free_block)
				next_free_block->prev_free = block->prev_free;
			if (prev_free_block)
				prev_free_block->next_free = block->next_free;
			if (block->prev_free == MM_INVALID_OFFSET) // was a head
				sector->class_heads[size_class] = block->next_free;
			if (block->next_free == MM_INVALID_OFFSET) // was a tail
				sector->class_tails[size_class] = block->prev_free;
		}

		shmassert(block->claimed == false);
		block->next_free = MM_INVALID_OFFSET;
		block->prev_free = MM_INVALID_OFFSET;
		// thus sector may temporary become inconsistent (target_size_class < size_class), until we split the larger block and register its parts.
		block->size_class = target_size_class;
		block->claimed = true;
		int new_block_size = flex_with_header_block_size(target_size_class);
		shmassert(new_block_size <= acquired_block_size);

		ShmHeapFlexBlockHeader *old_next_block = NULL;
		if (block->next_block != MM_INVALID_OFFSET)
		{
			old_next_block = get_flex_block_pointer(sector, block->next_block);
			shmassert((intptr_t)old_next_block == (intptr_t)block + acquired_block_size);
		}
		if (size_class > target_size_class)
		{
			// Split the block in steps, smallest to largest, appending new blocks into appropriate double-linked list.
			// Pad the "block" up to the next size class, doubling the block size each step until we reach the "size_class" of the original block.
			// | 1 | 1 |  2  |    4    |        8        |
			intptr_t current_position = (intptr_t)block;
			current_position += new_block_size;
			ShmHeapFlexBlockHeader *prev_block = block;
			for (int splitting_size = target_size_class; splitting_size < size_class; ++splitting_size)
			{
				int block_full_size = flex_with_header_block_size(splitting_size);
				ShmHeapFlexBlockHeader *new_block = (ShmHeapFlexBlockHeader *)current_position;

				init_medium_block(new_block, sector_shm, splitting_size);
				validate_medium_block_fast(new_block, sector, sector_shm);
				flex_block_append_free_to_head(new_block, splitting_size, sector, sector_shm);

				// now link the next_block/prev_block
				shmassert(prev_block);
				int new_block_offset = offset_to_int(new_block, sector);
				prev_block->next_block = new_block_offset;
				new_block->prev_block = offset_to_int(prev_block, sector);

				prev_block = new_block;
				current_position += block_full_size;
			}
			if (old_next_block)
			{
				shmassert(old_next_block == NULL || current_position == (intptr_t)old_next_block);
				old_next_block->prev_block = offset_to_int(prev_block, sector);
				prev_block->next_block = offset_to_int(old_next_block, sector);
			}
			shmassert(current_position == (intptr_t)block + acquired_block_size); // whole block is taken
		}
		validate_flex_sector_slow(sector, sector_shm);
	}
	return block;
}

ShmHeapFlexBlockHeader *
allocate_medium_block_of_size(ShmHeap *heap, int size, ShmHeapFlexSectorHeader **sector)
{
	shmassert(heap);
	shmassert(heap->size == sizeof(ShmHeap));
	shmassert(size >= 0 && size < MEDIUM_SIZE_CLASS_COUNT);

	// You should rewrite this function to support RESULT_REPEAT
	for (int trying_size = size; trying_size <= MEDIUM_SIZE_CLASS_COUNT; ++trying_size)
	{
		if (trying_size == MEDIUM_SIZE_CLASS_COUNT)
		{
			// free block not found
			ShmHeapFlexSectorHeader *new_sector = NULL;
			ShmPointer new_sector_shm = EMPTY_SHM;
			int rslt = alloc_flex_sector(heap, &new_sector, &new_sector_shm);
			shmassert(RESULT_OK == rslt);
			ShmHeapFlexBlockHeader *block = try_alloc_medium_block(heap, size, MEDIUM_SIZE_CLASS_LARGEST, new_sector, new_sector_shm);
			if (block)
			{
				if (sector)
					*sector = new_sector;
				return block;
			}
		}
		ShmPointer iter_shm = heap->flex_sectors.head;
		shmassert(heap->flex_sectors.head != NONE_SHM);
		ShmHeapFlexSectorHeader *iter = shm_pointer_to_pointer_unsafe(heap->flex_sectors.head);
		while (iter)
		{
			shmassert(iter->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX);

			ShmHeapFlexBlockHeader *block = try_alloc_medium_block(heap, size, trying_size, iter, iter_shm);
			if (block)
			{
				if (sector)
					*sector = iter;
				return block;
			}
			iter_shm = iter->next_sector;
			iter = shm_pointer_to_pointer_unsafe(iter_shm);
		}
	}
	return NULL;
}

void
reclaim_medium_block(ShmHeapFlexBlockHeader *block, ShmHeapFlexSectorHeader *sector, ShmPointer sector_shm)
{
	int trying_size = block->size_class;
	block->claimed = false;
	validate_medium_block_fast(block, sector, sector_shm);
	shmassert(block->next_free == MM_INVALID_OFFSET);
	shmassert(block->prev_free == MM_INVALID_OFFSET);

	flex_block_append_free_to_head(block, trying_size, sector, sector_shm);

	return;
	// We could try to at least consolidate the blocks of same sizes in a tree-like fasion, merging only blocks of the same parent.
	ShmHeapFlexBlockHeader * parent_prev_block = NULL;
	ShmHeapFlexBlockHeader * parent_next_block = NULL;
	// while (trying_size < MEDIUM_SIZE_CLASS_LARGEST)
	{
#pragma message ("This code could not even possibly work correctly. Need some way to verify its correctness")
		int block_size = medium_size_map[trying_size] + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE;
		// this code was invalid, giving even numbers only
		int wannabe_index = (offset_to_int(block, sector) - SHM_HEAP_FLEX_SECTOR_HEADER_SIZE) / block_size;
		bool odd = wannabe_index % 2 != 0;
		int neightbour_index;
		ShmHeapFlexBlockHeader *neighbour = NULL;
		ShmHeapFlexBlockHeader *starting = NULL;
		// 1's neighbour is 0; 0's neighbour is 1
		if (odd)
		{
			neightbour_index = wannabe_index + 1;
			neighbour = get_flex_block_pointer(sector, block->next_block);
			starting = block;
			parent_prev_block = get_flex_block_pointer(sector, block->prev_block);
			if (neighbour)
				parent_next_block = get_flex_block_pointer(sector, neighbour->next_block);
		}
		else
		{
			neightbour_index = wannabe_index - 1;
			neighbour = get_flex_block_pointer(sector, block->prev_block);
			starting = neighbour;
			if (neighbour)
				parent_prev_block = get_flex_block_pointer(sector, neighbour->prev_block);
			parent_next_block = get_flex_block_pointer(sector, block->next_block);
		}
		shmassert(neightbour_index >= 0);
		if (neighbour)
		{
			validate_medium_block(neighbour, sector, sector_shm);
			if (neighbour->size_class == block->size_class && block->size_class < MEDIUM_SIZE_CLASS_LARGEST)
			{
				// they met each other
				init_medium_block(starting, sector_shm, block->size_class + 1);
				if (parent_prev_block)
					starting->prev_block = offset_to_int(parent_prev_block, sector); // this expression calculated negative offset
				else
					starting->prev_block = MM_INVALID_OFFSET;

				if (parent_next_block)
					starting->next_block = offset_to_int(parent_next_block, sector); // this expression calculated negative offset
				else
					starting->next_block = MM_INVALID_OFFSET;
			}
		}
	}

	validate_flex_sector_slow(sector, sector_shm);
}

#define print_allocations false

void *get_mem(ThreadContext *thread, PShmPointer shm_pointer, int size, int debug_id)
{
	if (superblock == NULL)
		return NULL;
	debug_id |= 0x1D >> 24;
	if (shm_pointer) *shm_pointer = EMPTY_SHM;
	if (size <= 0)
		return NULL;
	if (thread == NULL) {
		// simple sequential allocation without reclamation of freed memory

		// if (superblock->mm_last_used_root_sector == (LONG)-1)
		// {
		// 	fprintf(stdout, "initial allocation\n");
		// 	ShmInt err = superblock_alloc_more(1, SHM_BLOCK_TYPE_ROOT, &superblock->mm_last_used_root_sector);
		// 	shmassert(err != RESULT_FAILURE);
		// 	shmassert(err != RESULT_REPEAT)
		// }
		while (1)
		{
			// int idx = (int) *superblock_count - 1;
			int idx = superblock->mm_last_used_root_sector;
			ShmInt last_used = 0;
			ShmChunkHeader *block = NULL;
			if (idx >= 0)
			{
				block = superblock_get_block(idx);
				if (!block)
				{
					fprintf(stderr, "Could not get block %d", idx);
					return NULL;
				}
				last_used = block->used;
			}
			// superblock_blocks[idx].size is immutable
			// if (idx >= 0 && superblock->blocks[idx].size - last_used >= (ShmInt)size)
			shmassert(idx < 0 || block);
			if (block && SHM_FIXED_CHUNK_SIZE - SHM_FIXED_CHUNK_HEADER_SIZE - last_used >= (ShmInt)size)
			{
				// block.used can only go up, so no ABA possible here
				if (CAS(&block->used, last_used + size, last_used))
				{
					void *rslt = (void *)((intptr_t)block + last_used);
					if (shm_pointer)
						*shm_pointer = pointer_to_shm_pointer(rslt, idx);
					return (void *)rslt;
				}
				// else repeat
			}
			else
			{
				// No free mem left in the last block. Or there are no blocks at all. Allocate a new one.

				// Here we use the atomic (under superblock->lock) nature of superblock_alloc_more for manipulation on new_index (= &superblock->mm_last_used_root_sector)
				// and initialization of ShmChunkHeader, to ensure invalid data won't be visible and overallocation on contention won't happen.
				int err = superblock_alloc_more(1, SHM_BLOCK_TYPE_ROOT, &superblock->mm_last_used_root_sector, idx);
				shmassert(err != RESULT_FAILURE);
				// repeat
			}
		}
	}
	else
	{
		// thread-bound memory
		allocs++;
		ShmWord total_size = size + 4;
		int class = classify_mem_size(total_size);
		ShmHeap *heap = (ShmHeap*)shm_pointer_to_pointer_root(thread->heap);
		shmassert(heap->owner == ShmGetCurrentThreadId());
		uint64_t spin_clock = rdtsc();
		// take_spinlock(PCAS2, &heap->lock, thread->self, 0, {});
		shm_lock_acquire(&heap->lock); // rare occasion of two locks being taken simulteneously (second one is mm_allocate...() -> superblock->lock)
		get_mem_spinlocking += rdtsc() - spin_clock;
		vl void *debug_ptr = NULL;
		if (class == 666)
		{
			ShmHeapFlexBlockHeader *medium_block;
			ShmHeapFlexSectorHeader *medium_sector = NULL;
			int medium_class = classify_mem_size_medium(total_size);
			shmassert_msg(medium_class != 666, "Too large block size");
			medium_block = allocate_medium_block_of_size(heap, medium_class, &medium_sector);
			if (medium_block)
			{
				medium_block->size = size; // possibly truncating leading zeroes
				medium_block->prev_id = medium_block->last_id;
				medium_block->last_id = debug_id;

				intptr_t rslt = (intptr_t)medium_block + SHM_HEAP_FLEX_BLOCK_HEADER_SIZE;
				ShmPointer shm = pointer_to_shm_pointer((void *)rslt, shm_pointer_get_block(medium_block->sector));
				memset((void *)(rslt + size), 0xCC, sizeof(uint32_t)); // guard bytes at the tail

				shm_lock_release(&heap->lock, __LINE__);
				get_mem_locking += rdtsc() - spin_clock;
				if (print_allocations)
					printf("%d. malloc %s\n", size, debug_id_to_str(debug_id));

				if (shm_pointer)
					*shm_pointer = shm;
				return (void *)rslt;
			}
			debug_ptr = medium_block;
		}
		else
		{
			ShmHeapBlockHeader *block;
			ShmHeapSectorHeader *sector = NULL;
			block = allocate_block_of_size(heap, class, &sector);
			if (block && sector)
			{
				block->prev_id = block->last_id;
				block->last_id = debug_id;
				shmassert(block->next == 0);
				shmassert(pack_shm_pointer(0, shm_pointer_get_block(sector->segment_header.sector)) == sector->segment_header.sector);
				ShmPointer shm = pointer_to_shm_pointer((void *)((intptr_t)block + SHM_HEAP_BLOCK_HEADER_SIZE), shm_pointer_get_block(sector->segment_header.sector));
				if (shm_pointer)
					*shm_pointer = shm;
				intptr_t result = (intptr_t)block + SHM_HEAP_BLOCK_HEADER_SIZE;
				shmassert((void *)result == shm_pointer_to_pointer(shm));
				memset((void *)(result + size), 0xCC, sizeof(uint32_t)); // guard bytes at the tail

				// release_spinlock(&heap->lock, 0, thread->self);
				shm_lock_release(&heap->lock, __LINE__);
				get_mem_locking += rdtsc() - spin_clock;
				if (print_allocations)
					printf("%d. alloc %s\n", size, debug_id_to_str(debug_id));
				return (void *)result;
			}
			debug_ptr = block;
		}
		SHM_UNUSED(debug_ptr);
		// release_spinlock(&heap->lock, 0, thread->self);
		shm_lock_release(&heap->lock, __LINE__);
		get_mem_locking += rdtsc() - spin_clock;
		shmassert_msg(false, "Memory allocation failed");
		return NULL;
	}
}

void
mm_validate_flex_block_fast(ShmHeapFlexBlockHeader *header)
{

}

void
mm_validate_small_block_fast(ShmHeapBlockHeader *header)
{

}

void
mm_validate_chunk_header(ShmChunkHeader *chunk_header)
{
	int type = chunk_header->type;
	shmassert(
		// type == SHM_BLOCK_TYPE_SUPER || -- invalid for MM
		// type == SHM_BLOCK_TYPE_ROOT || -- invalid for MM
		type == SHM_BLOCK_TYPE_THREAD_SECTOR ||
		// type == SHM_BLOCK_TYPE_THREAD_SEGMENT || -- invalid chunk type
		type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX);

	if (type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX)
	{

	}
	else if (type == SHM_BLOCK_TYPE_THREAD_SECTOR)
	{

	}
	// otherwise we can't check the sector
}

ShmChunkHeader *
mm_block_get_sector(vl void *block, ShmPointer shm_pointer)
{
	ShmPointer sector_shm = pack_shm_pointer(0, shm_pointer_get_block(shm_pointer));
	// ShmChunkHeader *sector_header = shm_pointer_to_pointer_unsafe(sector_shm);
	shmassert(shm_pointer >= sector_shm);
	ShmPointer offset = shm_pointer - sector_shm;
	uintptr_t sector_ptr = (uintptr_t)(vl char*)block - offset;
	shmassert(sector_ptr % 4 == 0);
	ShmChunkHeader *sector_header = (ShmChunkHeader *)sector_ptr;
	return sector_header;
}

ShmHeapBlockHeader *
mm_small_block_get_header(vl void *block)
{
	return (ShmHeapBlockHeader *)((intptr_t)block - SHM_HEAP_BLOCK_HEADER_SIZE);
}

ShmHeapFlexBlockHeader *
mm_medium_block_get_header(vl void *block)
{
	return (ShmHeapFlexBlockHeader *)((intptr_t)block - SHM_HEAP_FLEX_BLOCK_HEADER_SIZE);
}

int
mm_block_get_debug_id(vl void *block, ShmPointer block_shm)
{
	shmassert(block);
	shmassert(SBOOL(block_shm));
	ShmChunkHeader *chunk_header = mm_block_get_sector(block, block_shm);
	mm_validate_chunk_header(chunk_header);
	bool is_flex = chunk_header->type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX;
	if (is_flex)
	{
		ShmHeapFlexBlockHeader *header = mm_medium_block_get_header(block);
		mm_validate_flex_block_fast(header);
		return header->last_id;
	}
	else
	{
		ShmHeapBlockHeader *header = mm_small_block_get_header(block);
		mm_validate_small_block_fast(header);
		return header->last_id;
	}
}

// Use size = -1 for custom memory blocks
void
free_mem(ThreadContext *thread, ShmPointer shm_pointer, int size)
{
	if (!shm_pointer_is_valid(shm_pointer))
		return;
	frees++;
	// just for debug
	ShmPointer sector_shm = pack_shm_pointer(0, shm_pointer_get_block(shm_pointer));
	ShmChunkHeader *sector_header = shm_pointer_to_pointer_unsafe(sector_shm);
    ShmAbstractBlock *data = shm_pointer_to_pointer(shm_pointer);
	if (sector_header->type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX)
	{
		// medium block
		shmassert(size < 0 || size <= max_heap_block_size);
		ShmHeapFlexBlockHeader *block_header = (ShmHeapFlexBlockHeader *)((intptr_t)data - SHM_HEAP_FLEX_BLOCK_HEADER_SIZE);
		validate_medium_block(block_header, (ShmHeapFlexSectorHeader*)sector_header, sector_shm);
		if (size != -1)
		{
			shmassert(*(uint32_t*)((intptr_t)data + data->size) == guard_bytes);
		}
	}
	else
	{
		// small block
		if (size != -1)
		{
			shmassert(size <= max_heap_small_block_size);
			shmassert(data->size > 0);
			shmassert(data->size <= max_heap_small_block_size);
			shmassert(data->size < 64 * 1024);
			shmassert(*(uint32_t*)((intptr_t)data + data->size) == guard_bytes);
		}
		shmassert(block_get_segment((ShmHeapBlockHeader *)data)->base.type == SHM_BLOCK_TYPE_THREAD_SECTOR ||
			block_get_segment((ShmHeapBlockHeader *)data)->base.type == SHM_BLOCK_TYPE_THREAD_SEGMENT);

		ShmHeapBlockHeader *header = (ShmHeapBlockHeader *)((intptr_t)data - SHM_HEAP_BLOCK_HEADER_SIZE);
		shmassert(header->next == 0);
	}

	if (size != -1)
	{
		// this should go to _unallocate_mem, because the value might still be used in concurrent transaction.
		if ((data->type & SHM_TYPE_FLAG_REFCOUNTED) == SHM_TYPE_FLAG_REFCOUNTED)
		{
			ShmRefcountedBlock *refcounted = (ShmRefcountedBlock *)data;
			shm_refcounted_block_destroy(thread, shm_pointer, refcounted);
		}
	}

	ShmFreeList *block;
	ShmPointer block_shm;
	// done below
	// if (!thread->private_data->free_list)
	// 	thread->private_data->free_list = get_mem(thread, &thread->private_data->free_list_shm, sizeof(ShmFreeList), "thread->private_data->free_list");
	block = thread->private_data->free_list;
	block_shm = thread->private_data->free_list_shm;
	shmassert(SBOOL(block_shm) == !!block);
	
	if (block && block->count >= block->capacity)
	{
		// Append the block into shared list.
		// Clear the private date in case some error happens
		thread->private_data->free_list_shm = EMPTY_SHM;
		thread->private_data->free_list = NULL;

		bool success = false;
		for (int i = 0; i < 20; ++i)
		{
			ShmPointer next = thread->free_list; // "start of transaction"
			block->next = thread->free_list;
			success = PCAS(&thread->free_list, block_shm, next); // "transaction commit"
			if (success)
			{
				p_atomic_int_set(&superblock->has_garbage, 1);
				// always trigger event after setting the flag, otherwise coordinator might not see it.
				shm_event_signal(&superblock->has_garbage_event);
				break;
			}
		}
		shmassert_msg(success, "Too long loop while appending into free_list in the free_mem(). Memory leaked.");
		block = NULL;
	}
	if (!block)
	{
		block_shm = EMPTY_SHM;
		block = get_mem(thread, &block_shm, sizeof(ShmFreeList), PRIVATE_DATA_FREE_LIST2_DEBUG_ID);
		memset(CAST_VL(block), 0x53, sizeof(ShmFreeList));
		block->capacity = THREAD_FREE_LIST_BLOCK_SIZE;
		block->count = 0;
		block->next = EMPTY_SHM;
		thread->private_data->free_list = block;
		thread->private_data->free_list_shm = block_shm;
	}
	int idx = block->count;
	block->count++;
	block->items[idx] = shm_pointer;
}

void
_unallocate_mem(ShmPointer shm_pointer, ShmInt lock_value)
{
	ShmPointer sector_shm = pack_shm_pointer(0, shm_pointer_get_block(shm_pointer));
	ShmChunkHeader *sector_header = shm_pointer_to_pointer_unsafe(sector_shm);
	ShmPointer heap_shm = EMPTY_SHM;
	ShmHeapBlockHeader *block_small = NULL;
	ShmHeapSegmentHeader *segment_small = NULL;
	ShmHeapSectorHeader* sector_small = NULL;
	ShmHeapFlexBlockHeader *medium_block = NULL;
	ShmHeapFlexSectorHeader *medium_sector = NULL;

	ShmAbstractBlock *data = shm_pointer_to_pointer(shm_pointer);
	shmassert(data);
	// every managed block shall have type field
	shmassert(SHM_TYPE_RELEASE_MARK != (data->type & SHM_TYPE_RELEASE_MARK)); // check for double release
	if (data->type)
		data->type |= SHM_TYPE_RELEASE_MARK;

	if (sector_header->type == SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX)
	{
		// medium block
		medium_block = (ShmHeapFlexBlockHeader *)((intptr_t)(void*)shm_pointer_to_pointer(shm_pointer) - SHM_HEAP_FLEX_BLOCK_HEADER_SIZE);
		validate_medium_block(medium_block, (ShmHeapFlexSectorHeader*)sector_header, sector_shm);
		medium_sector = (ShmHeapFlexSectorHeader*)sector_header;
		heap_shm = medium_sector->heap;
		shmassert(medium_sector);
		shmassert(medium_block);
		if (print_allocations)
			printf("%d. mfree %s\n", medium_block->size, debug_id_to_str(medium_block->last_id));
	}
	else
	{
		block_small = (ShmHeapBlockHeader *)((intptr_t)data - SHM_HEAP_BLOCK_HEADER_SIZE);
		segment_small = block_get_segment((ShmHeapBlockHeader *)data);
		sector_small = (ShmHeapSectorHeader*)sector_header;
		shmassert(segment_small);
		shmassert(sector_small);
		shmassert(block_small);
		shmassert(segment_get_sector(block_get_segment((ShmHeapBlockHeader *)data)) == sector_small);

		if (print_allocations)
			printf("%d. free %s\n", segment_small->element_size, debug_id_to_str(block_small->last_id));

		heap_shm = sector_small->heap;
	}

	// memset((char*)data, 0xC, data->size); // invalidate the region
	//memset((char*)data, 0xC, segment->element_size);
	//*((int*)data) = segment->element_size;

	// get the owning thread's heap
	ShmHeap *heap = (ShmHeap*)shm_pointer_to_pointer_root(heap_shm);
	shmassert(heap);
	bool locked = false;
	// if (heap->lock != lock_value)
	// {
	// 	locked = true;
	// 	take_spinlock(PCAS2, &heap->lock, lock_value, 0, {});
	// }
	if (lock_value == -1)
	{
		// auto-detect
		if (!shm_lock_owned(&heap->lock))
		{
			locked = true;
			shm_lock_acquire(&heap->lock);
		}
	}
	else if (lock_value != 1)
	{
		locked = true;
		shm_lock_acquire(&heap->lock);
	}
	// otherwise the heap is already locked

	if (block_small)
	{
		reclaim_small_block(block_small, segment_small, sector_small);
	}
	else
	{
		reclaim_medium_block(medium_block, medium_sector, sector_shm);
	}

	if (locked)
	{
		// release_spinlock(&heap->lock, 0, lock_value);
		shm_lock_release(&heap->lock, __LINE__);
	}
}

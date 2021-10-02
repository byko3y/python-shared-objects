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

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include "shm_types.h"
#include "MM.h"
#include <pmacrosos.h>

static bool inited = false;

void
init_coordinator(void)
{
	assert(!inited);
	// p_libsys_init();
	p_uthread_init();
	inited = true;
}

static PUThread *reclaimer_thread;
static PUThread *keeper_thread;

static uint64_t got_threads = 0;
static uint64_t collected = 0;
static uint64_t waited = 0;
static uint64_t cleanup = 0;
static uint64_t cleanup_wait = 0;

bool
is_coordinator(void)
{
	return reclaimer_thread != NULL;
}

int
process_garbage(void)
{
	uint64_t tmp = rdtsc();
	p_atomic_int_set(&superblock->has_garbage, 0); // set before the cycle in case somebody will set it again in process.

	tmp = rdtsc();
	ShmPointer prev_free_list_shm = EMPTY_SHM;
	ShmPointer current_free_list_shm = EMPTY_SHM;
	for (int idx = 0; idx < MAX_THREAD_COUNT; ++idx)
	{
		ShmPointer thread_shm = superblock->threads.threads[idx]; // to slightly ease the detection of random race conditions (bugs)
		if (SBOOL(thread_shm))
		{
			ThreadContext *thread = LOCAL(thread_shm);
			ShmPointer free_list_shm = p_atomic_shm_pointer_exchange(&thread->free_list, EMPTY_SHM);
			if (SBOOL(free_list_shm))
			{
				if (SBOOL(prev_free_list_shm))
				{
					// find the tail and link it
					ShmFreeList *item = LOCAL(current_free_list_shm);
					while (item && SBOOL(item->next))
					{
						item = LOCAL(item->next);
					}
					shmassert(item);
					item->next = prev_free_list_shm;
				}
				current_free_list_shm = free_list_shm;
				prev_free_list_shm = current_free_list_shm;
			}
		}
	}
	collected += rdtsc() - tmp;

	// We have to check the thread status only after the free_list-s are gathered to avoid unallocating a recently retired block
	superblock->coordinator_data.count = 0;
	for (int idx = 0; idx < MAX_THREAD_COUNT; ++idx)
	{
		// First we flag all the thread            s.
		ShmPointer thread_shm = superblock->threads.threads[idx]; // to ease random race conditions (bugs)
		if (SBOOL(thread_shm))
		{
			ThreadContext *thread = LOCAL(thread_shm);
			if (p_atomic_int_get(&thread->transaction_mode) >= TRANSACTION_TRANSIENT) // otherwise thread is idle and cannot use references
			{
				p_atomic_int_set(&thread->test_finished, 1);
				if (p_atomic_int_get(&thread->transaction_mode) != TRANSACTION_TRANSIENT)
				{
					int idx = superblock->coordinator_data.count;
					superblock->coordinator_data.threads[idx] = thread_shm;
					superblock->coordinator_data.count += 1;
				}
			}
		}
	}
	got_threads += rdtsc() - tmp;

	if (superblock->coordinator_data.count > 0)
	{
		// Now we wait for the marked threads.
		tmp = rdtsc();
		bool has_thread = true;
		while (has_thread && p_atomic_int_get(&superblock->coordinator_data.halt) == 0)
		{
			Sleep(0);
			has_thread = false;
			for (int idx = 0; idx < superblock->coordinator_data.count; ++idx)
			{
				ShmPointer thread_shm = superblock->coordinator_data.threads[idx];
				if (SBOOL(thread_shm))
				{
					ThreadContext *thread = LOCAL(thread_shm);
					if (p_atomic_int_get(&thread->test_finished) == 0 || p_atomic_int_get(&thread->transaction_mode) <= TRANSACTION_IDLE)
						superblock->coordinator_data.threads[idx] = EMPTY_SHM;
					else
						has_thread = true;
				}
			}
		}

		// Fill the coordinator's thread list with invalid values.
		// It will be filled with correct onces in the next cycle.
		memset(CAST_VL(&superblock->coordinator_data.threads), 0x59, sizeof(ShmPointer) * MAX_THREAD_COUNT);
		waited += rdtsc() - tmp;
	}
	// Grace period has ended, now we can free the items
	tmp = rdtsc();
	// stats
	int unallocated_count = 0;
	// optimized locks
	bool locked = false;
	ShmPointer locked_heap_shm = EMPTY_SHM;
	ShmHeap *locked_heap = NULL;

	ShmFreeList *item = LOCAL(current_free_list_shm);
	ShmFreeList *prev = NULL;
	while (item)
	{
		for (int i = 0; i < item->count; ++i)
		{
			ShmPointer pntr = item->items[i];
			ShmAbstractBlock *block = LOCAL(pntr);
			SHM_UNUSED(block);
			ShmRefcountedBlock *refcounted = shm_pointer_to_refcounted(NULL, pntr, false, false);
			if (refcounted)
			{
				ShmInt revival_count = refcounted->revival_count;
				ShmInt release_count = refcounted->release_count;
				shmassert(revival_count >= 0);
				shmassert(release_count >= 0);
				shmassert(revival_count >= release_count);
				// release_count can only be modified by this very function (process_garbage).
				// Thus only revival_count might be changed concurrently, and it can only grow
				if (revival_count > release_count)
				{
					// item revived, wait for the new owner to send a new release request
					p_atomic_int_add(&refcounted->release_count, 1);
					continue;
				}
			}
			ShmPointer sector_shm = pack_shm_pointer(0, shm_pointer_get_block(pntr));
			ShmChunkHeader* sector = shm_pointer_to_pointer_unsafe(sector_shm);
			shmassert(sector);
			ShmPointer current_heap_shm = EMPTY_SHM;
			if (SHM_BLOCK_TYPE_THREAD_SECTOR_FLEX == sector->type)
			{
				ShmHeapFlexSectorHeader *header = (ShmHeapFlexSectorHeader*)sector;
				current_heap_shm = header->heap;
				shmassert(sector_shm == header->self);
			}
			else
			{
				shmassert(SHM_BLOCK_TYPE_THREAD_SECTOR == sector->type);
				ShmHeapSectorHeader *header = (ShmHeapSectorHeader*)sector;
				current_heap_shm = header->heap;
				shmassert(sector_shm == header->segment_header.sector);
			}
			shmassert(shm_pointer_to_pointer_root(current_heap_shm));

			if (locked && current_heap_shm == locked_heap_shm)
			{
				// just use the existing lock
			}
			else
			{
				// release the lock for the old heap and take for the new one.
				if (locked)
				{
					shmassert(locked_heap);
					// release_spinlock(&locked_heap->lock, 0, 1);
					shm_lock_release(&locked_heap->lock, __LINE__);
				}
				locked = false;
				locked_heap_shm = current_heap_shm;
				locked_heap = (ShmHeap*)shm_pointer_to_pointer_root(current_heap_shm);
				shmassert(locked_heap);
				if (!locked_heap)
					locked_heap_shm = EMPTY_SHM;
				else
				{
					uint64_t spin_clock = rdtsc();
					// take_spinlock(PCAS2, &locked_heap->lock, 1, 0, {});
					shm_lock_acquire(&locked_heap->lock);
					cleanup_wait += rdtsc() - spin_clock;
					locked = true;
				}
			}
			_unallocate_mem(pntr, 1);
			unallocated_count++;
		}

		prev = item;
		item = LOCAL(item->next);
	}
	if (locked)
	{
		shmassert(locked_heap);
		// release_spinlock(&locked_heap->lock, 0, 1);
		shm_lock_release(&locked_heap->lock, __LINE__);
	}
	cleanup += rdtsc() - tmp;
	shmassert(SBOOL(current_free_list_shm) == false || item || prev);
	return unallocated_count;
}

static int cycles = 0;

#define CYCLES_LOG_DIVIDER 100

static int processed_blocks = 0;

#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
	static pulong ShmGetTickCount(void)
	{
		return GetTickCount();
	}
#elif (defined(P_OS_UNIX))
	#include <time.h>
	static pulong ShmGetTickCount(void)
	{
		struct timespec ts;
		if(clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
			return (pulong)(ts.tv_nsec / 1000000 + ts.tv_sec * 1000);
		}
		shmassert(false);
		return 0;
	}
#endif

static pulong start_ticks = 0;
bool reclaimer_debug_info = false;

extern void dump_mm_debug(void);

static void *
shm_reclaimer_thread(void *thread)
{
	if (reclaimer_debug_info) fprintf(stderr, "\nReclaimer thread working.\n");
	start_ticks = ShmGetTickCount();
	ShmThreadID myid = ShmGetCurrentThreadId();
	SHM_UNUSED(myid);
	while (p_atomic_int_get(&superblock->coordinator_data.halt) == 0)
	{
		// int rslt = shm_event_sleep_wait(&superblock->has_grabage_event, 10);
		int rslt = shm_event_wait(&superblock->has_garbage_event, 1, false);
		shmassert(rslt != RESULT_FAILURE);
		if (p_atomic_int_get(&superblock->coordinator_data.halt) != 0)
			return NULL;
		if (superblock->has_garbage && !superblock->stop_reclaimer)
		{
			cycles++;
			// if ((cycles % CYCLES_LOG_DIVIDER) == (CYCLES_LOG_DIVIDER - 1))
			int count = process_garbage();
			processed_blocks += count;
			// if ((cycles % CYCLES_LOG_DIVIDER) == (CYCLES_LOG_DIVIDER - 1))
			if (reclaimer_debug_info && ShmGetTickCount() - start_ticks > 2000)
			{
				start_ticks = ShmGetTickCount();
				fprintf(stderr, "\nReclamation cycles %d.\n", cycles);
				cycles = 0;

				fprintf(stderr, "Reclaimer freed %d blocks.\nGot_threads %9.3f ms.  Collection %9.3f ms.\n   Waited %9.3f ms.  Cleanup %9.3f, including %9.3f spinlock\n",
					processed_blocks, got_threads / 3500000.0, collected / 3500000.0, waited / 3500000.0, cleanup / 3500000.0, cleanup_wait / 3500000.0);
				got_threads = 0;
				collected = 0;
				waited = 0;
				cleanup = 0;
				cleanup_wait = 0;
				processed_blocks = 0;
				dump_mm_debug();
			}
		}
	}
	return NULL;
}

static void *
shm_keeper_thread(void *thread)
{
	fprintf(stderr, "Keeper thread working.\n");
	start_ticks = ShmGetTickCount();
	while (p_atomic_int_get(&superblock->coordinator_data.halt) == 0)
	{
		//int rslt = shm_event_sleep_wait(&superblock->has_new_blocks, 10);
		//shmassert(rslt != RESULT_FAILURE, NULL);
		Sleep(1000);
	}
	return NULL;
}

void
start_coordinator(void)
{
	assert(inited);
	if (!p_atomic_int_compare_and_exchange(&superblock->coordinator_data.taken, 0, 1))
		shmassert_msg(false, "reclaimer is already running");
	reclaimer_thread = p_uthread_create(shm_reclaimer_thread, NULL, true);
	// p_uthread_create_full(shm_buffer_test_read_thread, NULL, true, P_UTHREAD_PRIORITY_HIGH, 0, NULL);

	// keeper_thread = p_uthread_create(shm_keeper_thread, NULL, true);
}

void
stop_coordinator_and_wait(void)
{
	p_atomic_int_set(&superblock->coordinator_data.halt, true);
	shm_event_signal(&superblock->has_garbage_event);
	pint rslt = p_uthread_join(reclaimer_thread);
	shmassert(rslt == 0);
}

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

#include "shm_types.h"
#include <time.h>


// 62^n bytes possible different identifiers
void _make_filename(char *buf)
{
	// Create a random filename for the shared memory object.
	// number of random bytes to use for name
	int nbytes = (SHM_SAFE_NAME_LENGTH - sizeof SHM_NAME_PREFIX);
	shmassert(nbytes >= 2); // '_SHM_NAME_PREFIX too long'
	strcpy_s(buf, SHM_SAFE_NAME_LENGTH, SHM_NAME_PREFIX);
	for (int i = 0; i<nbytes; ++i)
		buf[isizeof(SHM_NAME_PREFIX) + i - 1] = "01234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+_"[(unsigned int)rand() % 64];
	buf[isizeof(SHM_NAME_PREFIX) + nbytes - 1] = '\0';
	return;
}

void
superblock_check_mmap_inited(int group_index)
{
	shmassert(group_index >= 0 && group_index < SHM_BLOCK_COUNT / SHM_BLOCK_GROUP_SIZE);
	if (!superblock_mmap[group_index])
	{
		// block not mapped into memory yet
		ChunkAllocationResult alloc_result  = shm_chunk_allocate((__ShmChunk*)&superblock->block_groups[group_index], false, SHM_FIXED_CHUNK_SIZE * SHM_BLOCK_GROUP_SIZE);
		shmassert(alloc_result.data);
		shm_chunk_register_handle(&superblock->block_groups[group_index], alloc_result.FileMapping,
			superblock->coordinator_process, false);
		superblock_mmap[group_index] = alloc_result.data;
	}
}

void
superblock_release_all_mmaps(bool is_coordinator)
{
	for (int idx = 0; idx < SHM_BLOCK_COUNT / SHM_BLOCK_GROUP_SIZE; idx++)
	{
		if (superblock_mmap[idx])
		{
			shm_chunk_release(&superblock->block_groups[idx], superblock_mmap[idx],
				is_coordinator);
		}
	}
}

vl void *
superblock_get_block(int index)
{
	if (!superblock)
		return NULL;
	int group_index = index / SHM_BLOCK_GROUP_SIZE;
	int itemindex = index % SHM_BLOCK_GROUP_SIZE;
	if (index >= superblock->block_count)
	{
		shmassert(false);
		return NULL;
	}
	superblock_check_mmap_inited(group_index);
	return superblock_mmap[group_index] + itemindex * SHM_FIXED_CHUNK_SIZE;
}

vl void *
superblock_get_block_noside(int index)
{
	if (!superblock)
		return NULL;
	int group_index = index / SHM_BLOCK_GROUP_SIZE;
	int itemindex = index % SHM_BLOCK_GROUP_SIZE;
	if (index >= superblock->block_count)
		return NULL;
	shmassert(superblock_mmap[group_index]);
	return superblock_mmap[group_index] + itemindex * SHM_FIXED_CHUNK_SIZE;
}

void
lock_taken(ThreadContext *self, ShmLock *lock, ShmPointer shm_lock)
{
	self->next = EMPTY_SHM;
	SHM_UNUSED(lock);
	SHM_UNUSED(shm_lock);
}

int
signal_thread(ThreadContext *self, ShmPointer target)
{
	ThreadContext *target_ptr = LOCAL(target);
	if (target_ptr)
	{
		shmassert(target_ptr->self == target);
		// target_ptr->ready = 1; // atomic
		// if (target_ptr->ready_event != 0)
		// 	SetEvent(target_ptr->ready_event);
		shm_event_signal(&target_ptr->ready);
		return RESULT_OK;
	}
	else
		return RESULT_INVALID;
}

int
thread_reset_preempted(ThreadContext *self)
{
	ShmPointer thread = p_atomic_shm_pointer_get(&self->thread_preempted);
	shmassert(thread != NONE_SHM);
	if (SBOOL(thread))
	{
		signal_thread(self, thread);
		p_atomic_shm_pointer_set(&self->thread_preempted, EMPTY_SHM);
		return RESULT_OK;
	}
	return RESULT_INVALID;
}

void
thread_reset_signal(ThreadContext *thread)
{
	shm_event_reset(&thread->ready);
}

bool
preempt_thread(ThreadContext *self, ThreadContext *target)
{
	shmassert(target->magic == SHM_THREAD_MAGIC);
	shmassert(p_atomic_shm_pointer_get(&target->thread_preempted) != NONE_SHM);
	// we might also try to signal lower priority waiter already stored target->thread_preempted,
	// so we can replace thread_preempted with our pointer and return true.
	if (p_atomic_shm_pointer_get(&target->thread_preempted) == self->self || PCAS2(&target->thread_preempted, self->self, EMPTY_SHM))
		return true;
	else
		return false;
}

#define COMPARED_LOW_HIGH 1
#define COMPARED_HIGH_LOW -1

// basically returns self->last_start - target->last_start
int
shm_thread_start_compare(ShmInt thread1, ShmInt thread2)
{
	if (thread1 == 0 || thread2 == 0 || thread1 == thread2)
		return 0;
	else if (thread1 - thread2 > 0)
		return 1;
	else
		return -1;
}

ThreadContext *
shm_pointer_to_thread(ShmPointer value)
{
	ThreadContext *thread = LOCAL(value);
	shmassert(thread->self == value);
	shmassert(thread->magic == SHM_THREAD_MAGIC);
	shmassert(thread->index >= 0 && thread->index < isizeof(ShmReaderBitmap));
	return thread;
}

int
take_read_lock__checks(ThreadContext *self, ShmLock *lock, ShmPointer next_writer, ShmPointer writer_lock,
	ShmInt *oldest_writer_out, bool *lock_is_mine_out)
{
	if (SBOOL(p_atomic_shm_pointer_get(&self->thread_preempted)))
		return RESULT_PREEMPTED;

	bool the_lock_is_mine = false;
	if (writer_lock == self->self)
	{
		the_lock_is_mine = true;
		writer_lock = LOCK_UNLOCKED;
	}
	if (lock_is_mine_out) *lock_is_mine_out = the_lock_is_mine;

	shmassert(self->index >= 0 && self->index < isizeof(ShmReaderBitmap));
	if ((atomic_bitmap_get(&lock->reader_lock) & atomic_bitmap_thread_mask(self->index)) &&
	(SBOOL(writer_lock) == false || the_lock_is_mine))
	{
		return RESULT_OK; // already taken exclusively
	}

	shmassert(self->last_start != 0);
	ShmInt oldest_writer = 0;
	if (SBOOL(writer_lock))
	{
		ThreadContext *that_thread = shm_pointer_to_thread(writer_lock);
		if (that_thread)
		{
			ShmInt that_thread_start = p_atomic_int_get(&that_thread->last_start);
			shmassert(self->last_start != that_thread_start);
			if (that_thread_start != 0)
			{
				if (oldest_writer == 0)
				{
					oldest_writer = that_thread_start;
				}
				else if (shm_thread_start_compare(oldest_writer, that_thread_start) == COMPARED_LOW_HIGH)
				{
					oldest_writer = that_thread_start;
				}
			}
		}
	}
	// ShmPointer next_writer = p_atomic_shm_pointer_get(&lock->next_writer);
	// shmassert(next_writer != self->self);
	if (!the_lock_is_mine && SBOOL(next_writer))
	{
		ThreadContext *that_thread = shm_pointer_to_thread(next_writer);
		if (that_thread)
		{
			ShmInt that_thread_start = p_atomic_int_get(&that_thread->last_start);
			shmassert(self->last_start != that_thread_start);
			if (that_thread_start != 0)
			{
				if (oldest_writer == 0)
				{
					oldest_writer = that_thread_start;
				}
				else if (shm_thread_start_compare(oldest_writer, that_thread_start) == COMPARED_LOW_HIGH)
				{
					oldest_writer = that_thread_start;
				}
			}
		}
	}
	if (oldest_writer_out) *oldest_writer_out = oldest_writer;
	// kinda optimization, we could've also do this check after acquiring the "reader_lock"
	if (!the_lock_is_mine && oldest_writer != 0)
	{
		if (shm_thread_start_compare(self->last_start, oldest_writer) == COMPARED_LOW_HIGH)
		{
			self->private_data->times_aborted++;
			return RESULT_PREEMPTED;
		}
	}

	return RESULT_INVALID;
}

// See take_write_lock comment for description of the very complex locking system
int
take_read_lock(ThreadContext *self, ShmLock *lock, ShmPointer shm_lock, bool *locked)
{
	ShmPointer writer_lock = p_atomic_shm_pointer_get(&lock->writer_lock);
	ShmPointer next_writer = p_atomic_shm_pointer_get(&lock->next_writer);
	ShmInt oldest_writer = 0;
	bool the_lock_is_mine = false;
	int check_result = take_read_lock__checks(self, lock, next_writer, writer_lock, &oldest_writer, &the_lock_is_mine);
	if (check_result != RESULT_INVALID)
		return check_result;
	// not preempted and there are no higher priority writers.

	// before preempting writers we need to publish our lock so other writers will not try to get writer lock again.
	bool old_value = atomic_bitmap_set(&lock->reader_lock, self->index);
	if (!old_value)
	{
		// p_atomic_int_add(&lock->readers_count, 1);
		self->private_data->read_locks_taken++;
		*locked = true;
	}

	// Preempt low priority writers.
	// Also we need to not forget to verify (in each branch) absence of unexpected writer locks placed before we published our reader lock.
	if (!the_lock_is_mine)
	{
		if (oldest_writer != 0 && shm_thread_start_compare(self->last_start, oldest_writer) == COMPARED_HIGH_LOW)
		{
			shmassert(SBOOL(writer_lock) || SBOOL(next_writer));
			// thread cannot get older
			bool can_signal_back = true;
			bool contended = false; // we can continue when next_writer reset was successfully, but writer_lock should be taken seriously
			if (SBOOL(next_writer))
			{
				// next_writer field is not so important, we can just overwrite it, but send signal in case next_writer thread is waiting for progress.
				if (PCAS2(&lock->next_writer, LOCK_UNLOCKED, next_writer))
				{
					ThreadContext *that_thread = shm_pointer_to_thread(next_writer);
					// can_signal_back = preempt_thread(self, that_thread) && can_signal_back;
					// actually, we don't care about next_writer as long as he's able to see our lock and won't make make any writes.
					shmassert(shm_thread_start_compare(self->last_start, that_thread->last_start) != COMPARED_LOW_HIGH); // coz threads cannot get older
					preempt_thread(self, that_thread);
					shm_event_signal(&that_thread->ready);
				}
				else
					return RESULT_REPEAT;
			}
			else if (SBOOL(p_atomic_shm_pointer_get(&lock->next_writer)))
				return RESULT_REPEAT;

			ShmPointer writer_lock_2 = p_atomic_shm_pointer_get(&lock->writer_lock);
			if (writer_lock_2 == self->self)
				writer_lock_2 = LOCK_UNLOCKED;

			if (writer_lock_2 != writer_lock) // we compare the field with value at the start of the function because the whole function takes short time to complete
				return RESULT_REPEAT;
			if (SBOOL(writer_lock))
			{
				// writer_lock cannot be modified that easily. We need to wait for writer to willingly release the lock.
				ThreadContext *that_thread = shm_pointer_to_thread(writer_lock);
				shmassert(shm_thread_start_compare(self->last_start, that_thread->last_start) != COMPARED_LOW_HIGH); // coz threads cannot get older
				can_signal_back = preempt_thread(self, that_thread) && can_signal_back;
				shm_event_signal(&that_thread->ready);
				contended = true;
			}
			else if (SBOOL(writer_lock_2))
				return RESULT_REPEAT;

			if (contended)
			{
				self->private_data->times_waiting++;
				if (can_signal_back)
					return RESULT_WAIT_SIGNAL;
				else
					return RESULT_WAIT;
			}
		}
		else if (SBOOL(p_atomic_shm_pointer_get(&lock->writer_lock)))
		{
			// There suddenly appeared a new writer_lock right before we published our read lock.
			// It's okay to have conflicting reader and writer lock active, but we cannot use our read lock unless we are sure nobody's using the writer lock and vice versa
			return RESULT_REPEAT;
		}
	}

	return RESULT_OK;
}

bool
shm_cell_have_write_lock(ThreadContext *thread, ShmLock *lock)
{
	return p_atomic_shm_pointer_get(&lock->writer_lock) == thread->self;
}

bool
shm_cell_have_read_lock(ThreadContext *thread, ShmLock *lock)
{
	return atomic_bitmap_check_me(&lock->reader_lock, thread->index);
}

void
shm_cell_check_write_lock(ThreadContext *thread, ShmLock *lock)
{
	// shmassert(atomic_bitmap_check_exclusive(&lock->reader_lock, thread->index));
	shmassert(shm_cell_have_write_lock(thread, lock));
	shmassert(p_atomic_int_get(&lock->writers_count) == 1);

	ShmInt readers_count = p_atomic_int_get(&lock->readers_count);
	ShmInt reference;
	if (shm_cell_have_read_lock(thread, lock))
		reference = 1;
	else
		reference = 0;

	shmassert(readers_count == reference);
}

void
shm_cell_check_read_lock(ThreadContext *thread, ShmLock *lock)
{
	// shmassert(atomic_bitmap_check_exclusive(&lock->reader_lock, thread->index));
	shmassert(shm_cell_have_read_lock(thread, lock));
	shmassert(p_atomic_int_get(&lock->readers_count) > 0);
	// On very rare occasions can the lock->readers_count be higher for a very short period of time with "thread->preempted" assigned
	if (shm_cell_have_write_lock(thread, lock))
		shmassert(p_atomic_int_get(&lock->writers_count) == 1);
	else
		shmassert(p_atomic_int_get(&lock->writers_count) == 0);
}

void
shm_cell_check_read_write_lock(ThreadContext *thread, ShmLock *lock)
{
	if (shm_cell_have_write_lock(thread, lock))
		shm_cell_check_write_lock(thread, lock);
	else
		shm_cell_check_read_lock(thread, lock);
}

void
_thread_set_pending_lock(ThreadContext *thread, ShmPointer shm_lock)
{
	p_atomic_shm_pointer_set(&thread->pending_lock, shm_lock);
	p_atomic_int_add(&thread->private_data->pending_lock_count, 1);
}

void
notify_next_writer(ThreadContext *thread, ShmLock *lock);

bool
thread_queue_to_lock(ThreadContext *thread, ShmLock *lock, ShmPointer container_shm)
{
	shmassert(thread->index >= 0 && thread->index < isizeof(ShmReaderBitmap));
	bool in_queue = atomic_bitmap_check_me(&lock->queue_threads, thread->index);
	if (!in_queue)
	{
		shmassert(thread->pending_lock != container_shm);
		// if (SBOOL(thread->pending_lock))
		// {
		// 	thread_unqueue_from_lock(thread);
		// }
		shmassert(SBOOL(thread->pending_lock) == false); // we always unqueue after abort or success (but not repeat)

		// shm_event_reset(&thread->ready);
		// p_atomic_int_add(&superblock->debug_lock_count, 1);
		_thread_set_pending_lock(thread, container_shm);
		atomic_bitmap_set(&lock->queue_threads, thread->index);
		return true;
	}
	else
		shmassert(thread->pending_lock == container_shm);

	return false;
}

bool
_thread_unqueue_from_lock(ThreadContext *thread, ShmLock *lock)
{
	bool rslt = PCAS2(&lock->next_writer, LOCK_UNLOCKED, thread->self);

	p_atomic_shm_pointer_set(&thread->pending_lock, EMPTY_SHM);
	p_atomic_int_dec_and_test(&thread->private_data->pending_lock_count);
	atomic_bitmap_reset(&lock->queue_threads, thread->index);
	return rslt;
}

void
thread_unqueue_from_lock(ThreadContext *thread)
{
	if (SBOOL(thread->pending_lock))
	{
		ShmContainer *container = LOCAL(thread->pending_lock);
		_thread_unqueue_from_lock(thread, &container->lock);
	}
}

void
update_tickets(ThreadContext *thread, vl ShmInt *cell)
{
	ShmInt ticket = superblock->ticket;
	*cell += ticket - thread->private_data->last_known_ticket;
	thread->private_data->last_known_ticket = ticket;
}

ShmPointer
lock_queue_find_highest_priority(volatile ShmReaderBitmap *bitmap, int self_index, int *found_index, ShmInt *found_last_start)
{
	ShmReaderBitmap contenders = atomic_bitmap_exclude_thread(*bitmap, self_index);
	ShmInt oldest_start = 0;
	int oldest_index = -1;
	for (int idx = 0; idx < MAX_THREAD_COUNT; ++idx)
	{
		if ((contenders & atomic_bitmap_thread_mask(idx)) != 0)
		{
			ThreadContext *thread = LOCAL(superblock->threads.threads[idx]);
			ShmInt thread_start = thread->last_start;
			if (thread_start != 0)
			{
				if (oldest_start == 0 || shm_thread_start_compare(thread_start, oldest_start) == COMPARED_HIGH_LOW)
				{
					oldest_start = thread_start;
					oldest_index = idx;
				}
			}
		}
	}
	if (found_index) *found_index = oldest_index;
	if (found_last_start) *found_last_start = oldest_start;
	if (oldest_index == -1)
		return EMPTY_SHM;
	else
		return superblock->threads.threads[oldest_index];
}

ShmInt
next_writer_get_last_start(ShmPointer next_writer)
{
	ThreadContext *thread = LOCAL(next_writer);
	shmassert(thread || !SBOOL(next_writer));
	if (thread)
		return thread->last_start;
	else
		return 0;
}

bool
atomic_bitmap_has_higher_priority(ShmReaderBitmap contenders, ShmInt last_start)
{
	for (int i = 0; i < 64; ++i)
	{
		if ((contenders & (UINT64_C(1) << i)) != 0)
		{
			ThreadContext *thread = LOCAL(superblock->threads.threads[i]);
			shmassert(last_start != thread->last_start);
			if (thread && shm_thread_start_compare(last_start, thread->last_start) == COMPARED_LOW_HIGH)
			{
				return true;
			}

		}
	}
	return false;
}

#define thread_debug_register_line(line) do {\
	self->private_data->last_writer_lock = lock->writer_lock; \
	self->private_data->last_writer_lock_pntr = lock; \
	self->private_data->last_operation = __LINE__; \
} while (0);

#define thread_debug_register_result(times_var, tickets_var) do {\
	self->private_data->times_var++; \
	update_tickets(self, &self->private_data->tickets_var); \
	self->private_data->last_writer_lock = lock->writer_lock; \
	self->private_data->last_writer_lock_pntr = lock; \
	self->private_data->last_operation = __LINE__; \
} while (0);

int
preempt_readers(ThreadContext *self, ShmLock *lock)
{
	ShmReaderBitmap contenders = atomic_bitmap_contenders(&lock->reader_lock, self->index);
	// should be checking this condition before calling preempt_readers_or_abort()
	if (false && atomic_bitmap_has_higher_priority(contenders, self->last_start))
	{
		if (shm_cell_have_write_lock(self, lock))
			lock->release_line = __LINE__;
		thread_debug_register_result(times_aborted4, tickets_aborted4);
		return RESULT_PREEMPTED;
	}
	bool preempted = false;
	ShmReaderBitmap signalled_readers = 0;
	ShmInt myindex = self->index;
	SHM_UNUSED(myindex);
	for (int idx = 0; idx < 64; ++idx)
	{
		// contenders exclude the current thread (self->index)
		if (atomic_bitmap_check_me(&contenders, idx))
		{
			ThreadContext *thread = LOCAL(superblock->threads.threads[idx]);
			if (thread)
				switch (shm_thread_start_compare(self->last_start, thread->last_start))
				{
				case COMPARED_HIGH_LOW:
				{
					// thread->last_start, but thread cannot get older
					preempt_thread(self, thread);
					preempted = true;
					signalled_readers |= atomic_bitmap_thread_mask(idx);
					break;
				}
				case COMPARED_LOW_HIGH:
					return RESULT_PREEMPTED;
					break;
				case 0:
					// just remember this case exists. For example, thread just ended its transaction and/or about to start a new one.
					break;
				}
		};
	}
	// shmassert(preempted, NULL); // contenders might be changed in the process so false assertions will appear on rare occasions
	if (preempted)
	{
		if ((signalled_readers & atomic_bitmap_get(&lock->reader_lock)) != 0)
			return RESULT_WAIT_SIGNAL; // readers still active and will signal us using lock->next_writer (you've already set next_writer here, right?)
		else
			return RESULT_OK;
		// we might still get some contending readers unless next_writer/writer_lock is set. And even then some higher priority reader might come in.
		// So RESULT_OK doesn't mean there are no contenders.
	}
	else
		return RESULT_OK;
}

bool random_flinch = false;

int
take_write_lock__checks(ThreadContext *self, ShmLock *lock, ShmPointer container_shm, bool strict)
{
	// assert the queueing consistency
	shmassert(atomic_bitmap_check_me(&lock->queue_threads, self->index) == (self->pending_lock == container_shm));

	if (SBOOL(p_atomic_shm_pointer_get(&self->thread_preempted)))
	{
		thread_debug_register_result(times_aborted1, tickets_aborted1);
		self->private_data->last_operation = __LINE__;
		self->private_data->last_operation_rslt = RESULT_PREEMPTED;
		return RESULT_PREEMPTED;
	}

	if (random_flinch && rand() % 128 == 3)
	{
		thread_debug_register_line(__LINE__);
		self->private_data->last_operation_rslt = RESULT_INVALID;
		return RESULT_PREEMPTED;
	}

	shmassert(self->index >= 0 && self->index < isizeof(ShmReaderBitmap));
	// we might be having contention with readers after we've got the lock, because someone haven't see our lock yet.
	if (atomic_bitmap_check_exclusive(&lock->reader_lock, self->index))
	{
		if (shm_cell_have_write_lock(self, lock))
		{
			shmassert(p_atomic_int_get(&lock->writers_count) <= 1);
			thread_debug_register_line(__LINE__);
			self->private_data->last_operation_rslt = RESULT_OK;
			return RESULT_OK; // already have the valid lock
		}
	}
	else
	{
		// don't even try to get the lock in case there are higher priority locks
		ShmReaderBitmap contenders = atomic_bitmap_contenders(&lock->reader_lock, self->index);
		for (int i = 0; i < 64; ++i)
		{
			if ((contenders & atomic_bitmap_thread_mask(i)) != 0)
			{
				ThreadContext *thread = LOCAL(superblock->threads.threads[i]);
				shmassert(self->last_start != thread->last_start);
				if (thread)
				{
					switch (shm_thread_start_compare(self->last_start, thread->last_start))
					{
					case COMPARED_LOW_HIGH:
						thread_debug_register_result(times_aborted2, tickets_aborted2);
						self->private_data->last_operation_rslt = RESULT_PREEMPTED;
						return RESULT_PREEMPTED;
						break;
					case COMPARED_HIGH_LOW:
						if (strict) // for postchecking right after we've got the lock
						{
							thread_debug_register_line(__LINE__);
							self->private_data->last_operation_rslt = RESULT_REPEAT;
							return RESULT_REPEAT;
						}
						break;
					}
				}
			}
		}
		if (random_flinch && rand() % 128 == 3)
		{
			thread_debug_register_line(__LINE__);
			self->private_data->last_operation_rslt = RESULT_INVALID;
			return RESULT_REPEAT;
		}
		/*if (atomic_bitmap_has_higher_priority(contenders, self->last_start))
		{
			// no assertion here because we might not have the lock
			if (shm_cell_have_write_lock(self, lock))
			{
				// lock->release_line = __LINE__;
				// p_atomic_pointer_set(&lock->writer_lock, LOCK_UNLOCKED);
				// self->private_data->write_locks_taken--;
			}
			thread_debug_register_result(times_aborted2, tickets_aborted2);
			self->private_data->last_operation_rslt = RESULT_PREEMPTED;
			return RESULT_PREEMPTED;
		}*/
	}

	// thread is not preempted, doesn't have the lock yet, there are no higher priority readers
	thread_debug_register_line(__LINE__);
	return RESULT_INVALID;
}

// Last modification 26.08.2020
// RESULT_WAIT means there are low priority writers still holding the lock and we need to wait for them to abort e.g. Sleep(0), yield(), ShmEvent.
// RESULT_REPEAT means something changed and we need to rerun this function.
// RESULT_ABORT means there are high priority threads or we just have to abort anyway.
// RESULT_PREEMPTED specifically for being preempted by another thread
// RESULT_WAIT and RESULT_REPEAT keep the acquired lock so other threads, especially readers, can see it.
//
// Reader-writer contention is resolved with following considerations:
// * If there's at least one reader older (higher priority) than writer, then readers are allowed to proceed and writer is aborted.
//   Reader->reader examination is expensive and undesirable for possible cohorting, so we rely on writer status, because writer has to examine readers anyway.
//   Active writer lock means there are no older readers (most probably).
// * Racing conditions may cause both reader and writer lock flags be active at the same moment.
//   Taking a strict next_writer barrier (aka intermediate lock) with subsequent reader cross-check
//   can help to completely avoid this,
//   but the benefits are not worth the trouble.
// * Usage of a write barrier (next_writer) for convoy-blocking new readers is not the best idea because
//   we don't know when the next writer will start its transaction,
//   so reading might eventually be blocked forever despite the fact we could have a good reader concurrency.
// * Contending locker acquires second lock, but cannot use it for data access (because the first lock is already in use).
//   Second lock is used for contention resolution, it can become usefull for data access after the contending lock is released.
// * Writer either aborts when there's older reader, or preempts every reader otherwise.
// * Receiving signals from every aborted reader will be expensive, so we should either spinlock with writer or send signal with LAST READER only.
//   Absense of other readers is easily determined because new readers usually cannot appear when high priority writer lock is set.
// * On contention with readers, a writer sets "NEXT_WRITER" (kinda "a writer waiting for readers") instead of taking "writer_lock",
//   so a higher priority writer can come and take over the whole lock.
//   "next_writer" is more like a hint than actual lock, but we still need to ensure it is cleared correctly,
//   so I'd prefer to set it as late as possible and only by owning thread.
//   Thus high priority writer may come and set lock->writer_lock directly while lock->next_writer waits for readers,
//   or overwrite lock->next_writer and become a new waiter (we don't even need to signal the old waiter in the later case, because queue mechanism will send a signal).
// * Detect higher priority contenders before locking, preempt low priority contenders after locking. Thus low priority contenders will be aware of higher priority contender.
//
//  Reader does:
//     if check_exclusive() return Ok;
//     if !check_highest_priority() return Preempted;
//     acquire_lock();
//     if preempt_contenders() return Wait;
//     else return Ok
//
//  Writer does:
//     if higher_priority_threads_exist() || preempted then return Preempted;
//     if exclusive_lock_owned() then return Ok;
//     queue_for_lock();
//     set_barrier();
//     if preempt_low_priorities()
//          return wait;
//     acquire_lock();
//     if preempt_low_priorities() {
//        if highest_priority_threads_exist() || preempted then
//            return Preempted;
//        if preempt_low_priorities() then
//            continue;
//        return Wait; // waiting algorythm:
//                     //     if higher_priority_threads_exist() || preempted then
//                     //         release_lock();
//                     //     preempt_low_priorities();
//                     //     wait_signal();
//     }
//     return Ok
int
take_write_lock(ThreadContext *self, ShmLock *lock, ShmPointer container_shm, bool *locked)
{
	shmassert(self->last_start != 0);
	int check_rslt = take_write_lock__checks(self, lock, container_shm, false);
	if (check_rslt != RESULT_INVALID)
		return check_rslt;
	// Thread is not preempted, doesn't have the lock yet, there are no higher priority readers...

	// It's possible to reach this place with "writer_lock = self" due to contending readers.
	// if (shm_cell_have_write_lock(self, lock) == false)
	// No, always queue to keep the conditions clear for any writer-contender.
	// Unqueue on transaction end (not on retain-retry) or next writer lock acquisition.
	{
		thread_queue_to_lock(self, lock, container_shm);
		thread_reset_signal(self); // prepare to wait for signal in queue
	}
	// Could've moved both queueing and writer priority checks into take_write_lock__checks,
	// but it's better to separate those strict conditions (take_write_lock__checks) with
	// more volatile conditions in the writer queue.

	// Remember: we cancel our transaction due to higher priority writer before taking lock,
	// but preempt other transactions after we took the lock.
	// We have to check the writer_lock, because each transaction can be queue to single lock only
	// i.e. has writer_lock but absent in queue_threads.
	ShmPointer current_writer_lock = p_atomic_shm_pointer_get(&lock->writer_lock);
	if (SBOOL(current_writer_lock) && current_writer_lock != self->self)
	{
		ThreadContext *that_thread = shm_pointer_to_thread(current_writer_lock);
		shmassert(that_thread);
		if (shm_thread_start_compare(self->last_start, that_thread->last_start) == COMPARED_LOW_HIGH)
		{
			self->private_data->last_wait_oldest = current_writer_lock;
			self->private_data->last_wait_oldest_index = that_thread->index;
			self->private_data->last_wait_queue = lock->queue_threads;
			self->private_data->last_wait_writer_lock = lock->writer_lock;
			self->private_data->last_wait_next_writer = lock->next_writer;
			thread_debug_register_line(__LINE__);
			self->private_data->last_operation_rslt = RESULT_PREEMPTED;
			return RESULT_PREEMPTED;
		}
	}
	ShmInt oldest_start;
	int oldest_index = -1;
	ShmPointer oldest = lock_queue_find_highest_priority(&lock->queue_threads, self->index, NULL, &oldest_start); // slow function
	if (SBOOL(oldest) && shm_thread_start_compare(self->last_start, oldest_start) == COMPARED_LOW_HIGH)
	{
		// there is a higher priority writer in the queue
		// signal_thread(self, oldest); - normally we should never need this
		self->private_data->last_wait_oldest = oldest;
		self->private_data->last_wait_oldest_index = oldest_index;
		self->private_data->last_wait_queue = lock->queue_threads;
		self->private_data->last_wait_writer_lock = lock->writer_lock;
		self->private_data->last_wait_next_writer = lock->next_writer;
		thread_debug_register_line(__LINE__);
		self->private_data->last_operation_rslt = RESULT_PREEMPTED;
		return RESULT_PREEMPTED; // Hm-m-m, this way we don't need the queue anymore, but a single highest priority thread.
	}
	// Thread is not preempted, doesn't have the lock yet, there are no higher priority readers...
	//     ...
	// ... and there are no higher priority writers.
	// Now we have two routes:
	// - There are contending low priority readers and we will wait with next_writer set
	// - Otherwise just take writer_lock

	// Code below writen by tired me, so you should check it twice.

	// We could place this code somewhere below, inside conditions, but it's so much easier to understand the code if we always set the next_writer.
	// We are most likely a highest priority thread here.
	ShmPointer next_writer = p_atomic_shm_pointer_get(&lock->next_writer); // we haven't read the next_writer yet
	if (next_writer != self->self)
	{
		if (SBOOL(next_writer))
		{
			ThreadContext *that_thread = shm_pointer_to_thread(next_writer);
			if (shm_thread_start_compare(self->last_start, that_thread->last_start) == COMPARED_LOW_HIGH)
			{
				thread_debug_register_line(__LINE__);
				self->private_data->last_operation_rslt = RESULT_REPEAT;
				return RESULT_REPEAT; // that's a new high priority thread, we can't CAS it away.
			}
		}
		if (random_flinch && rand() % 128 == 3)
		{
			thread_debug_register_line(__LINE__);
			self->private_data->last_operation_rslt = RESULT_INVALID;
			return RESULT_REPEAT;
		}
		if (!PCAS2(&lock->next_writer, self->self, next_writer))
		{
			self->private_data->last_operation = __LINE__;
			self->private_data->last_operation_rslt = RESULT_REPEAT;
			return RESULT_REPEAT;
		}
	}
	// Barrier is active, now we can check the contenders
	ShmReaderBitmap contenders = atomic_bitmap_contenders(&lock->reader_lock, self->index);
	if (contenders != 0)
	{
		int rslt = preempt_readers(self, lock);
		if (rslt != RESULT_OK)
		{
			self->private_data->last_writer_lock = lock->writer_lock;
			self->private_data->last_writer_lock_pntr = lock;
			self->private_data->last_operation = __LINE__;
			self->private_data->last_operation_rslt = rslt;
			return rslt; // either preempted by new reader or wait for readers to abort.
		}
		// else preempt_readers() detected no contenders on its exit, so we should try acquiring writer_lock
	}

	if (!PCAS2(&lock->writer_lock, self->self, LOCK_UNLOCKED) && !PCAS2(&lock->writer_lock, self->self, DEBUG_SHM))
	{
		self->private_data->last_writer_lock = lock->writer_lock;
		self->private_data->last_writer_lock_pntr = lock;
		self->private_data->last_operation = __LINE__;
		self->private_data->last_operation_rslt = RESULT_REPEAT;
		return RESULT_REPEAT;
	}
	self->private_data->write_locks_taken++;
	p_atomic_int_add(&superblock->debug_lock_count, 1);
	*locked = true;
	// lock_taken(self, lock, container_shm);
	self->private_data->last_writer_lock = lock->writer_lock;
	self->private_data->last_writer_lock_pntr = lock;
	self->private_data->last_operation = __LINE__;
	self->private_data->last_operation_rslt = RESULT_OK;
	if (random_flinch && rand() % 128 == 3)
	{
		thread_debug_register_line(__LINE__);
		self->private_data->last_operation_rslt = RESULT_INVALID;
		return RESULT_REPEAT;
	}

	// There's a small, but significant chance for a reader to set its reader lock right after it checked the lock->writer_lock.
	// It will abort its transaction really soon and will not restart it.
	contenders = atomic_bitmap_contenders(&lock->reader_lock, self->index);
	if (contenders != 0)
	{
		for (int idx = 0; idx < 64; ++idx)
		{
			if (atomic_bitmap_check_me(&contenders, idx))
			{
				ThreadContext *thread = LOCAL(superblock->threads.threads[idx]);
				if (thread && shm_thread_start_compare(self->last_start, thread->last_start) == COMPARED_HIGH_LOW)
				{
					// shmassert_msg(false, "Got new readers while writer_lock and next_writer barriers were active");
					return RESULT_WAIT;
				}
			}
		}
	}
	if (random_flinch && rand() % 128 == 3)
	{
		thread_debug_register_line(__LINE__);
		self->private_data->last_operation_rslt = RESULT_INVALID;
		return RESULT_WAIT;
	}

	int after_check_rslt = take_write_lock__checks(self, lock, container_shm, true);
	if (after_check_rslt == RESULT_INVALID)
		after_check_rslt = RESULT_OK; // not preempted and has exclusive lock (no other readers, even low priority ones)
	if (after_check_rslt != RESULT_OK)
	{
		self->private_data->last_operation = __LINE__;
		self->private_data->last_operation_rslt = after_check_rslt;
	}
	return after_check_rslt;
	///////////////////////////////////////////////////////////////////////////////////////////////
	// Legacy
	///////////////////////////////////////////////////////////////////////////////////////////////

	// lock->next_writer transaction start
	ShmPointer prev_next_writer = p_atomic_shm_pointer_get(&lock->next_writer);
	ShmInt prev_next_writer_start = next_writer_get_last_start(prev_next_writer);
	// let LR = locking right
	bool had_LR = prev_next_writer == self->self;
	bool got_LR = false;
	if (!had_LR)
	{
		if (shm_thread_start_compare(self->last_start, prev_next_writer_start) == COMPARED_LOW_HIGH)
		{
			thread_debug_register_result(times_aborted3, tickets_aborted3);
			// DebugPause();
			return RESULT_WAIT_SIGNAL; // we are in queue but we need to wait for the current lock->next_writer
		}

		if (!PCAS2(&lock->next_writer, self->self, prev_next_writer))
		{
			thread_debug_register_result(times_repeated, tickets_repeated);
			return RESULT_REPEAT; // lock->next_writer changed
		}
		got_LR = true;
	}
	// lock->next_writer transaction finish.

	// WHERE DO WE PLACE IT? Somewhere after we take the lock
	// the writer lock acts as a barrier, there's no use in preempting readers before
	// taking the lock because readers will keep coming again.
	if (shm_cell_have_write_lock(self, lock) || false)
	{
		shmassert(p_atomic_int_get(&lock->writers_count) <= 1);

		// preempt readers or abort
		thread_reset_signal(self);
		int readers_result = preempt_readers(self, lock);

		if (readers_result != RESULT_OK)
		{
			if (!atomic_bitmap_check_exclusive(&lock->reader_lock, self->index))
			{
				// readers are still active and have their
				thread_debug_register_result(times_waiting, tickets_waiting);
				return readers_result; // wait for the preempted threads
				                       // On rare occasions we might be waiting for a reader that will never signal us.
			}
		}
		// otherwise we don't expect new readers as documented in preempt_readers()
	}

	if (had_LR || got_LR)
	{
		// next_writer as a barrier is useless. next_writer check is useless two coz we just read it.
		// // cross-check with readers after locking
		// int readers_result = preempt_readers_or_abort(self, contenders, lock);

		// if (p_atomic_shm_pointer_get(&lock->next_writer) != self->self)
		// {
		// 	thread_debug_register_result(times_aborted5, tickets_aborted5);
		// 	return RESULT_PREEMPTED; // about to be preempted
		// }

		// transform the preliminary lock into the actual lock
		if (PCAS2(&lock->writer_lock, self->self, LOCK_UNLOCKED) || PCAS2(&lock->writer_lock, self->self, DEBUG_SHM))
		{
			self->private_data->write_locks_taken++;

			shmassert(EMPTY_SHM == p_atomic_shm_pointer_get(&lock->transaction_data));

			shmassert(lock->writers_count == 0);

			// // cross-check with next_writer under lock
			// if (p_atomic_shm_pointer_get(&lock->next_writer) != self->self)
			// {
			//	// about to be preempted. -- So what? Putting this check after every line hurts readability.
			//	signal_thread(self, p_atomic_shm_pointer_get(&lock->next_writer));
			//	p_atomic_pointer_set(&lock->writer_lock, LOCK_UNLOCKED); // we are still in the lock->queue_threads
			//	self->private_data->write_locks_taken--;
			//		thread_debug_register_result(times_aborted6, tickets_aborted6);
			//	return RESULT_PREEMPTED;
			// }

			bool next_writer_released = false;
			// Find a new next_writer
			// Usually we are the highest priority writer unless lock->next_writer was changed by higher priority writer.
			for (int newnext_cycle = 0; newnext_cycle <= 5; ++newnext_cycle)
			{
				shmassert_msg(newnext_cycle < 5, "Too long looping to select a new lock->next_writer");
				int new_next_index = -1;
				ShmPointer new_next = lock_queue_find_highest_priority(&lock->queue_threads, self->index, &new_next_index, NULL);
				if (new_next_index != -1)
				{
					// CAS on lock->next_writer is the last step in the _thread_release_pending_lock() after excluding itself from lock->queue_threads,
					// so we need to make sure this new next_writer is not going through these steps right now, otherwise it might never release the lock->next_writer.
					if (atomic_bitmap_check_me(&lock->queue_threads, new_next_index))
					{
						next_writer_released = PCAS2(&lock->next_writer, new_next, self->self);
					}
					else
					{
						continue;
					}
				}
				break;
			}

			// leave the queue
			bool next_writer_released2 = _thread_unqueue_from_lock(self, lock);
			next_writer_released = next_writer_released2 || next_writer_released;
			if (!next_writer_released)
			{
				// about to be preempted
				signal_thread(self, p_atomic_shm_pointer_get(&lock->next_writer));
				p_atomic_pointer_set(&lock->writer_lock, LOCK_UNLOCKED); // we are not in queue now
				self->private_data->write_locks_taken--;
				thread_debug_register_result(times_aborted7, tickets_aborted7);
				return RESULT_PREEMPTED;
			}

			p_atomic_int_add(&superblock->debug_lock_count, 1);

			p_atomic_int_add(&lock->writers_count, 1);
			lock_taken(self, lock, container_shm);
			self->private_data->last_writer_lock_pntr = lock;
			self->private_data->last_writer_lock = lock->writer_lock;
			self->private_data->last_operation = __LINE__;
			// cross-check under lock
			if (atomic_bitmap_check_exclusive(&lock->reader_lock, self->index))
			{
				shm_cell_check_write_lock(self, lock);
				return RESULT_OK;
			}
			else
				// someone got a reading lock while we were acquiring writing lock
				return RESULT_REPEAT;
		}
		else
		{
			ShmPointer writer_lock = p_atomic_shm_pointer_get(&lock->writer_lock);
			ThreadContext *thread = LOCAL(writer_lock);
			if (thread)
			{
				shmassert(self->last_start != thread->last_start);
				switch (shm_thread_start_compare(self->last_start, thread->last_start))
				{
				case COMPARED_HIGH_LOW:
					// thread cannot get older
					thread_debug_register_result(times_waiting2, tickets_waiting2);
					if (preempt_thread(self, thread))
						return RESULT_WAIT;
					else
						return RESULT_WAIT;
					break;
				case COMPARED_LOW_HIGH:
					// no assertion here because we might not have the lock
					if (shm_cell_have_write_lock(self, lock))
					{
						lock->release_line = __LINE__;
						signal_thread(self, p_atomic_shm_pointer_get(&lock->next_writer));
						p_atomic_pointer_set(&lock->writer_lock, LOCK_UNLOCKED);
						self->private_data->write_locks_taken--;
					}
					thread_debug_register_result(times_aborted8, tickets_aborted8);
					return RESULT_PREEMPTED;
					// return RESULT_WAIT;
					break;
				}
				thread_debug_register_result(times_repeated, tickets_repeated);
				return RESULT_REPEAT; // volatility, quickly attempt to run this routine once again
			}
			else
			{
				thread_debug_register_result(times_repeated, times_repeated);
				return RESULT_REPEAT; // volatility, quickly attempt to run this routine once again
			}
		}
	}
	else
	{
		thread_debug_register_result(times_aborted9, tickets_aborted9);
		return RESULT_ABORT;
	}

}

// when we don't want to wait for contention on RESULT_WAIT or RESULT_REPEAT from take_write_lock
/*void
untake_write_lock(ThreadContext *self, ShmLock *lock, ShmPointer shm_lock)
{
	// if (shm_cell_have_write_lock(self, lock) && p_atomic_int_get(&lock->writer_lock_ensured) == 0)
	{
		shmassert(lock->writer_lock_ensured == 0, NULL);
		lock->release_line = __LINE__;
		p_atomic_pointer_set(&lock->writer_lock, LOCK_UNLOCKED);
	}
}*/

void
notify_next_writer(ThreadContext *thread, ShmLock *lock)
{
	ThreadContext *that_thread = NULL;
	ShmPointer next_writer = p_atomic_shm_pointer_get(&lock->next_writer);
	if (SBOOL(next_writer))
		that_thread = shm_pointer_to_thread(next_writer); // next_writer has the highest priority most likely
	else
	{
		ShmPointer oldest = lock_queue_find_highest_priority(&lock->queue_threads, thread->index, NULL, NULL); // slow function
		if (SBOOL(oldest))
			that_thread = shm_pointer_to_thread(oldest);
	}
	// ensure the thread has exclusive access
	if (that_thread)
	{
		ShmReaderBitmap contenders = atomic_bitmap_contenders(&lock->reader_lock, that_thread->index);
		if (contenders == 0)
			shm_event_signal(&that_thread->ready);
	}
	// next_writer might still run away and never take our lock due to
	// some kind of timeout or conditional execution,
	// so we might try to implement infinite cycle here running until consistency reached.
}

void
_shm_cell_unlock(ThreadContext *thread, ShmLock *lock, ShmInt type)
{
	shmassert(type == TRANSACTION_ELEMENT_READ || type == TRANSACTION_ELEMENT_WRITE);
	bool test = false;
	if (TRANSACTION_ELEMENT_READ == type)
		test = atomic_bitmap_check_me(&lock->reader_lock, thread->index);
	else
		test = shm_cell_have_write_lock(thread, lock);

	shmassert_msg(test, "Trying to unlock the cell we don't own.");

	bool had_lock = false;
	if (type == TRANSACTION_ELEMENT_READ)
	{
		had_lock = atomic_bitmap_reset(&lock->reader_lock, thread->index);
		// if (had_lock)
		shmassert(had_lock);
		notify_next_writer(thread, lock);

		thread->private_data->read_locks_taken--;
		// p_atomic_int_dec_and_test(&lock->readers_count);
		// shmassert(p_atomic_int_get(&lock->readers_count) >= 0);
	}
	else
	{
		if (shm_cell_have_write_lock(thread, lock))
		{
			// p_atomic_pointer_set(&lock->writer_lock, LOCK_UNLOCKED);
			lock->prev_lock = lock->writer_lock;
			// p_atomic_int_set(&lock->writer_lock_ensured, 0); // always set/reset under writer_lock

			// _thread_release_pending_lock(thread, lock);
			shmassert(thread->pending_lock == EMPTY_SHM);
			//_thread_unqueue_from_lock(thread, lock);

			p_atomic_int_dec_and_test(&superblock->debug_lock_count);
			// p_atomic_int_dec_and_test(&lock->writers_count);
			thread->private_data->write_locks_taken--;

			shmassert(lock->writers_count == 0);
			shmassert_msg(lock->writer_lock == thread->self, "Our lock has been taken by someone else.");
			lock->release_line = __LINE__;
			p_atomic_pointer_set(&lock->writer_lock, LOCK_UNLOCKED);
			// first indicate there's nobody home, then signal to those who have begun waiting right before we left the home.
			// signal_thread(thread, lock->next_writer);

			notify_next_writer(thread, lock);
			had_lock = true;

		}
	}
	if (!had_lock)
		shmassert_msg(had_lock, "Unlocking the cell we don't own.");
}

static bool allocation_failed = false;

struct alloc_more_closure {
	vl ShmInt *new_index;
	ShmInt old_index;
};

int
superblock_alloc_more_cb(void *data)
{
	struct alloc_more_closure *closure = data;
	// if (superblock->block_count > oldcount)
	if (closure->old_index != -2 && p_atomic_int_get(closure->new_index) != closure->old_index)
		return RESULT_REPEAT;
	else
		return RESULT_INVALID;
}

static bool allocation_rand_debug[0x10] = { false, };
static int allocation_rand_debug_index = 0;

// Atomically claims the last unused index and sets new_index to this index and initializes ShmChunkHeader (only then atomic operation ends)
// "oldcount" should be "-2" to disable the check
int
superblock_alloc_more(ShmPointer thread, int type, vl ShmInt *new_index, int old_index)
{
	if (superblock->block_count >= SHM_BLOCK_COUNT)
	{
		if (!allocation_failed)
		{
			allocation_failed = TRUE;
			fprintf(stderr, "Shared block count limit exceeded. superblock_alloc_more failed, crash imminent.\n");
		}
		return RESULT_FAILURE;
	}
	// The call must abort in case there was an unexpected allocated in other thread (kinda compare-and-swap).
	// We can't read old value here, because decision to call superblock_alloc_more was made from outer function.
	// ShmInt oldcount = superblock->block_count;

	// take the lock
	/*while (InterlockedCompareExchange(&superblock->lock, 1, 0) != 0) {
	if (superblock->block_count > oldcount)
	{
	// new block has been allocated by someone else. Return it
	return 0;
	}
	};*/
	// if (superblock->block_count > oldcount)
	// take_spinlock(CAS2, &superblock->lock, thread, 0, {
	// 	if (superblock->block_count > oldcount)
	// 		return RESULT_ABORT; // somebody have allocated block concurrently. Go into next iteration outside this function
	// });

	struct alloc_more_closure closure = { new_index, old_index };
	int rslt = shm_lock_acquire_with_cb(&superblock->lock, superblock_alloc_more_cb, &closure);
	shmassert(rslt != RESULT_INVALID);
	if (rslt != RESULT_OK && rslt != RESULT_INVALID)
	{
		if (shm_lock_owned(&superblock->lock))
			shm_lock_release(&superblock->lock, __LINE__);
		if (result_is_repeat(rslt))
			return RESULT_REPEAT;
		return rslt;
	}
	shmassert(shm_lock_owned(&superblock->lock));
	if (alloc_flinch)
	{
		bool should_flinch = rand() % 4 == 1; // shared memory allocations are very rare
		allocation_rand_debug[allocation_rand_debug_index] = should_flinch;
		allocation_rand_debug_index++;
		allocation_rand_debug_index &= 0x10 - 1;
		if (should_flinch)
		{
			shm_lock_release(&superblock->lock, __LINE__);
			return RESULT_REPEAT;
		}
	}
	// same check as in superblock_alloc_more_cb, to ensure nobody accured a new block before we've got the lock.
	if (closure.old_index != -2 && p_atomic_int_get(closure.new_index) != closure.old_index)
	{
		shm_lock_release(&superblock->lock, __LINE__);
		return RESULT_REPEAT;
	}
	ShmInt newindex = superblock->block_count;

	// if (superblock->block_count > oldcount)
	// {
	//	release_spinlock(&superblock->lock, 0, thread);
	//	// new block has been allocated by someone else. Return it
	//	return 0;
	// }

	rslt = RESULT_OK;
	int group_idx = newindex / SHM_BLOCK_GROUP_SIZE;
	int itemindex = newindex % SHM_BLOCK_GROUP_SIZE;
	ShmChunkHeader *newblock = NULL;
	if (itemindex == 0)
	{
		// allocating a new group
		// ShmChunkHeader *newblock = allocate_block((__ShmChunk *)NULL, true, SHM_FIXED_CHUNK_SIZE);
		ChunkAllocationResult alloc_result = shm_chunk_allocate((__ShmChunk*)&superblock->block_groups[group_idx], true, SHM_FIXED_CHUNK_SIZE * SHM_BLOCK_GROUP_SIZE);
		shmassert(alloc_result.data);
		shm_chunk_register_handle(&superblock->block_groups[group_idx], alloc_result.FileMapping,
			superblock->coordinator_process, false);
		newblock = alloc_result.data;
		if (newblock)
		{
			newblock->type = type;
			newblock->used = SHM_FIXED_CHUNK_HEADER_SIZE;
			superblock_mmap[group_idx] = (vl char *)newblock;
		}
	}
	else
	{
		// using a block from existing group
		vl char *block = superblock_mmap[group_idx];
		if (!block)
		{
			superblock_check_mmap_inited(group_idx);
			block = superblock_mmap[group_idx];
		}
		if (block)
		{
			newblock = (ShmChunkHeader *)(intptr_t)(block + SHM_FIXED_CHUNK_SIZE * itemindex);
			newblock->type = type;
			newblock->used = SHM_FIXED_CHUNK_HEADER_SIZE;
		}
	}

	if (newblock)
	{
		p_atomic_int_set(&superblock->block_count, newindex + 1); // success
		if (new_index)
			*new_index = newindex;
	}
	else
	{
		rslt = RESULT_FAILURE;
	}

	// release_spinlock(&superblock->lock, 0, thread);
	shm_lock_release(&superblock->lock, __LINE__);
	return rslt;
}

// Transactions

ShmTransactionElement *
thread_register_lock(ThreadContext *thread, ShmLock *lock, ShmPointer container_shm, int container_type, ShmInt type)
{
	shmassert(type == TRANSACTION_ELEMENT_READ || type == TRANSACTION_ELEMENT_WRITE);
	bool owned = false;
	if (type == TRANSACTION_ELEMENT_WRITE)
	{
		shmassert_msg(lock->writer_lock == thread->self, "Trying to register a write lock we don't own.");
		owned = lock->writer_lock == thread->self;
	}
	else if (type == TRANSACTION_ELEMENT_READ)
	{
		shmassert_msg(atomic_bitmap_check_me(&lock->reader_lock, thread->index), "Trying to register a read lock we don't own.");
	}
	SHM_UNUSED(owned);

	// verify the container is not in the list yet
	ShmTransactionElement *element = thread->current_transaction;
	while (element)
	{
		shmassert(element->type != type || element->container != container_shm);
		element = element->next;
	}

	// append the lock into the locks list
	if (type == TRANSACTION_ELEMENT_WRITE)
	{
		shmassert(!SBOOL(p_atomic_shm_pointer_get(&lock->transaction_data)));
		// lock->transaction_data = newone_shm;
	}

	ShmPointer newone_shm;
	ShmTransactionElement *newone = (ShmTransactionElement *)get_mem(thread, &newone_shm, sizeof(ShmTransactionElement),
		SHM_TRANSACTION_ELEMENT_DEBUG_ID);

	newone->owner = thread->self;
	newone->type = type;
	newone->container_type = container_type;
	newone->container = container_shm;

	newone->next = thread->current_transaction;
	newone->next_shm = thread->current_transaction_shm;
	thread->current_transaction = newone;
	thread->current_transaction_shm = newone_shm;

	return newone;

	// if (type == TRANSACTION_ELEMENT_WRITE)
	// {
	// 	p_atomic_pointer_set(&lock->transaction_data, newone_shm);  // transaction_data is more like a flag than an actual pointer
	// }
}

void
thread_unregister_last_lock(ThreadContext *thread, ShmTransactionElement *element, ShmLock *lock, ShmPointer container_shm, int container_type, ShmInt type)
{
	SHM_UNUSED(lock);
	ShmPointer to_release = thread->current_transaction_shm;
	shmassert(LOCAL(to_release) == element);
	shmassert(element->container_type == container_type);
	shmassert(element->container == container_shm);
	shmassert(element->type == type);

	thread->current_transaction = element->next;
	thread->current_transaction_shm = element->next_shm;
	free_mem(thread, to_release, sizeof(ShmTransactionElement));
}

// here lock_shm is a pointer to the parent structure, aligned to the correct memory manager border
int
transaction_lock_read(ThreadContext *thread, ShmLock *lock, ShmPointer container_shm, int container_type, bool *lock_taken)
{
	shmassert(thread != NULL);
	shmassert_msg(thread->transaction_mode != TRANSACTION_NONE, "thread->transaction_mode != TRANSACTION_NONE");
	shmassert_msg(thread->transaction_mode != TRANSACTION_TRANSIENT, "previous transient transaction is not finished correctly");
	if (thread->transaction_mode == TRANSACTION_IDLE)
		p_atomic_int_set(&thread->transaction_mode, TRANSACTION_TRANSIENT); // reclaimer can also read the variable

	if (thread->async_mode)
	{
		switch (thread->transaction_lock_mode)
		{
			case LOCKING_ALL:
			case LOCKING_WRITE:
			{
				// totally unstested
				bool had_write_lock = shm_cell_have_write_lock(thread, lock);
				SHM_UNUSED(had_write_lock);
				bool had_read_lock = atomic_bitmap_check_me(&lock->reader_lock, thread->index);
				bool locked = false;
				int rslt = take_read_lock(thread, lock, container_shm, &locked);
				if (rslt == RESULT_OK && locked)
				{
					shmassert(!had_read_lock);
					if (!had_read_lock)
						thread_register_lock(thread, lock, container_shm, container_type, TRANSACTION_ELEMENT_READ);
					if (lock_taken)
						*lock_taken = true;
					rslt = RESULT_OK;
				}

				if (rslt == RESULT_OK)
				{
					shmassert(atomic_bitmap_check_me(&lock->reader_lock, thread->index));
					shmassert(p_atomic_shm_pointer_get(&lock->writer_lock) == 0 || p_atomic_shm_pointer_get(&lock->writer_lock) == thread->self);
				}
				return rslt;
				break;
			}
			case LOCKING_NONE:
				break;
			default:
				shmassert_msg(false, "Invalid thread->transaction_lock_mode");
				return RESULT_FAILURE;
		}
	}
	else
	{
		switch (thread->transaction_lock_mode)
		{
			case LOCKING_ALL:
			case LOCKING_WRITE:
			{
				bool had_write_lock = shm_cell_have_write_lock(thread, lock);
				bool had_read_lock = atomic_bitmap_check_me(&lock->reader_lock, thread->index);
				if (had_write_lock)
				{
					// write lock implies read access
					int check_result = take_write_lock__checks(thread, lock, container_shm, false);
					shmassert(check_result != RESULT_FAILURE);
					if (RESULT_OK == check_result || RESULT_INVALID == check_result)
						return RESULT_OK;
					else
						return check_result;
				} else if (had_read_lock)
				{
					// we don't need to enter the negotiation cycle because we've already reached exclusive ownership once.
					ShmPointer writer_lock = p_atomic_shm_pointer_get(&lock->writer_lock);
					ShmPointer next_writer = p_atomic_shm_pointer_get(&lock->next_writer);
					int check_result = take_read_lock__checks(thread, lock, next_writer, writer_lock, NULL, NULL);
					shmassert(check_result != RESULT_FAILURE);
					if (RESULT_OK == check_result || RESULT_INVALID == check_result)
						return RESULT_OK;
					else
						return check_result;
				}
				else
				{
					// had_read_lock = false
					// had_write_lock = false
					bool got_new_lock = false;
					int final_result = RESULT_INVALID;
					int cycle_number = 0;
					int orig_rslt = RESULT_INVALID;
					do {
						cycle_number++;
						bool locked = false;
						int rslt = orig_rslt = take_read_lock(thread, lock, container_shm, &locked);
						shmassert(rslt != RESULT_FAILURE);
						shmassert(!got_new_lock || !locked); // impossible to obtain lock twice in a row
						got_new_lock = got_new_lock || locked;
						shmassert(got_new_lock == shm_cell_have_read_lock(thread, lock));

						if (result_is_abort(rslt))
						{
							// if (registered_lock != NULL)
							// {
							// 	shmassert(!had_read_lock);
							// 	thread_unregister_last_lock(thread, registered_lock, lock, container_shm, container_type, TRANSACTION_ELEMENT_READ);
							// 	registered_lock = NULL;
							// }
							if (got_new_lock && shm_cell_have_read_lock(thread, lock))
							{
								got_new_lock = false;
								_shm_cell_unlock(thread, lock, TRANSACTION_ELEMENT_READ);
							}
							shmassert(got_new_lock == false);
							final_result = rslt;
							break;
						}
						if (RESULT_OK == rslt)
						{

							final_result = RESULT_OK;
							break;
						}
						if (rslt == RESULT_WAIT_SIGNAL)
						{
							ShmPointer writer_lock = p_atomic_shm_pointer_get(&lock->writer_lock);
							ShmPointer next_writer = p_atomic_shm_pointer_get(&lock->next_writer);
							int check_result = take_read_lock__checks(thread, lock, next_writer, writer_lock, NULL, NULL);
							// short-cycle condition check because second call of take_write_lock might return RESULT_WAIT instead of RESULT_WAIT_SIGNAL
							if (RESULT_OK == check_result || RESULT_INVALID == check_result || RESULT_WAIT_SIGNAL == rslt)
								; // good
							else if (RESULT_WAIT == rslt || RESULT_REPEAT == rslt)
								continue;
							else
							{
								// RESULT_ABORT, RESULT_PREEMPTED, RESULT_FAILURE
								shmassert(RESULT_FAILURE != check_result);
								if (got_new_lock && shm_cell_have_read_lock(thread, lock))
								{
									got_new_lock = false;
									_shm_cell_unlock(thread, lock, TRANSACTION_ELEMENT_READ);
								}
								shmassert(got_new_lock == false);
								final_result = check_result;
								break;
							}

							// This call is very problematic and we really need to ensure another thread would wake us up
							int wait_rslt = shm_event_wait(&thread->ready, 1, DEBUG_SHM_EVENTS);
							shmassert(wait_rslt != RESULT_FAILURE);
						}
						else if (RESULT_WAIT == rslt || RESULT_REPEAT == rslt)
							continue;
						else
							shmassert(false);
					}
					while (1);
					// we only come out of cycle after we have a determinted state of the lock
					if (got_new_lock)
						thread_register_lock(thread, lock, container_shm, container_type, TRANSACTION_ELEMENT_READ);

					if (final_result == RESULT_OK)
					{
						if (lock_taken)
							*lock_taken = true;
						// count the lock only after we've ensured exclusive access at least once.
						p_atomic_int_add(&lock->readers_count, 1);
						shm_cell_check_read_lock(thread, lock);
					}
					else
						shmassert(shm_cell_have_read_lock(thread, lock) == false);

					return final_result;
				}
				break;
			}
			case LOCKING_NONE:
				break;
			default:
				shmassert_msg(false, "Invalid thread->transaction_lock_mode");
				return RESULT_FAILURE;
		}
	}
	return RESULT_OK;
}

int last_take_write_lock_result = 0;

int
transaction_lock_write(ThreadContext *thread, ShmLock *lock, ShmPointer lock_shm, int container_type, bool *lock_taken)
{
	shmassert(thread != NULL);
	shmassert_msg(thread->transaction_mode != TRANSACTION_NONE, "thread->transaction_mode != TRANSACTION_NONE");
	shmassert_msg(thread->transaction_mode != TRANSACTION_TRANSIENT, "previous transient transaction is not finished correctly");
	if (thread->transaction_mode == TRANSACTION_IDLE)
		p_atomic_int_set(&thread->transaction_mode, TRANSACTION_TRANSIENT); // reclaimer can also read the variable

	// LOCKING_TRANSIENT usually does not hold more than one lock at a time
	if (thread->async_mode)
	{
		int rslt = RESULT_OK;
		switch (thread->transaction_lock_mode)
		{
			case LOCKING_NONE:
				break;
			case LOCKING_WRITE:
			case LOCKING_ALL:
				// totally untested
				{
					bool had_write_lock = shm_cell_have_write_lock(thread, lock);
					bool had_read_lock = atomic_bitmap_check_me(&lock->reader_lock, thread->index);
					SHM_UNUSED(had_read_lock);
					if (!had_write_lock)
					{
						bool locked = false;
						int orig_rslt = rslt = take_write_lock(thread, lock, lock_shm, &locked);
						SHM_UNUSED(orig_rslt);
						shmassert(!had_write_lock || locked);
						if (rslt == RESULT_OK)
						{
							shm_cell_check_write_lock(thread, lock);
							if (!had_write_lock)
							{
								thread_register_lock(thread, lock, lock_shm, container_type, TRANSACTION_ELEMENT_WRITE);
								if (lock_taken) *lock_taken = true;
							}
						}

						if (rslt == RESULT_OK)
							shm_cell_check_write_lock(thread, lock);
						return rslt;
					}
					else
						return RESULT_OK;
				}

				break;
			default:
				shmassert_msg(false, "Invalid thread->transaction_lock_mode");
				return RESULT_FAILURE;
		}
	}
	else
	{
		int rslt = RESULT_OK;
		// from take_spinlock
		// int backoff = 64;
		switch (thread->transaction_lock_mode)
		{
			case LOCKING_NONE:
				break;
			case LOCKING_WRITE:
			case LOCKING_ALL:
			{
				bool had_write_lock = shm_cell_have_write_lock(thread, lock);
				bool had_read_lock = shm_cell_have_read_lock(thread, lock);
				SHM_UNUSED(had_read_lock);
				if (had_write_lock)
				{
					int rslt = take_write_lock__checks(thread, lock, lock_shm, false); // we don't care about low priorities here, we had this lock long time ago
					// having reading low-priority contenders is fine for us because we've got exclusive lock first.
					if (rslt != RESULT_PREEMPTED)
						return RESULT_OK;
					else
						return RESULT_PREEMPTED; // lock had to be registered by now
				}
				else
				{
					bool got_new_lock = false;
					int cycle_number = 0;
					int orig_rslt = RESULT_INVALID;
					int final_result = RESULT_INVALID;
					do {
						cycle_number++;
						bool locked = false;
						orig_rslt = rslt = take_write_lock(thread, lock, lock_shm, &locked);
						shmassert(rslt != RESULT_FAILURE);
						shmassert(!got_new_lock || !locked); // impossible to obtain lock twice in a row
						got_new_lock = got_new_lock || locked;
						shmassert(got_new_lock == shm_cell_have_write_lock(thread, lock));
						thread->private_data->last_take_write_lock_result = rslt;
						if (result_is_abort(rslt))
						{
							// if (!had_write_lock && new_write_lock)
							// {
							//     shmassert(registered_lock);
							//     thread_unregister_last_lock(thread, registered_lock, lock, lock_shm, container_type, TRANSACTION_ELEMENT_WRITE);
							//     registered_lock = NULL;
							//     new_write_lock = false;
							// }
							// unqueue and possibly unlock
							thread_unqueue_from_lock(thread); // we can unqueue any time, but should signal only after unqueueing and preferably after releasing the lock.
							if (got_new_lock && shm_cell_have_write_lock(thread, lock))
							{
								got_new_lock = false;
								_shm_cell_unlock(thread, lock, TRANSACTION_ELEMENT_WRITE); // not be registered yet
							}
							final_result = rslt;
							break;
						}
						if (RESULT_OK == rslt)
						{
							final_result = RESULT_OK;
							break;
						}
						if (rslt == RESULT_WAIT_SIGNAL)
						{
							int check_result = take_write_lock__checks(thread, lock, lock_shm, false);
							// short-cycle condition check because second call of take_write_lock might return RESULT_WAIT instead of RESULT_WAIT_SIGNAL
							if (RESULT_OK == check_result || RESULT_INVALID == check_result || RESULT_WAIT_SIGNAL == check_result)
								; // good
							else if (RESULT_WAIT == check_result || RESULT_REPEAT == check_result)
								continue;
							else
							{
								// RESULT_ABORT, RESULT_PREEMPTED, RESULT_FAILURE
								shmassert(RESULT_FAILURE != check_result);
								// same as a regular abort: unqueue and possibly unlock
								thread_unqueue_from_lock(thread); // we can unqueue any time, but should signal only after unqueueing and preferably after releasing the lock.
								if (got_new_lock && shm_cell_have_write_lock(thread, lock))
								{
									got_new_lock = false;
									_shm_cell_unlock(thread, lock, TRANSACTION_ELEMENT_WRITE); // not be registered yet
								}
								final_result = check_result;
								shmassert(got_new_lock == false);
								break;
							}
							// This call is very problematic and we really need to ensure another thread would wake us up
							int wait_rslt = shm_event_wait(&thread->ready, 1, DEBUG_SHM_EVENTS); // debug
							// int wait_rslt = shm_event_wait(&thread->ready, 1, false);
							shmassert(wait_rslt != RESULT_FAILURE);
						}
						else if (RESULT_WAIT == rslt || RESULT_REPEAT == rslt)
							continue;
						else
							shmassert(false);
						/*
						// from take_spinlock
						if (backoff <= SPINLOCK_MAX_BACKOFF)
						{
							for (int i = 0; i < backoff; ++i)
								SHM_SMT_PAUSE;
							backoff *= 2;
						}
						else if (backoff <= SPINLOCK_MAX_SLEEP_BACKOFF)
						{
							Sleep(0);
							backoff *= 2;
							// int wait_rslt = shm_event_wait(&thread->ready, 2);
							// shmassert(wait_rslt != RESULT_FAILURE, NULL);
						}
						else
							break;*/
					} while (1);
					SHM_UNUSED(orig_rslt);

					if (got_new_lock)
						thread_register_lock(thread, lock, lock_shm, container_type, TRANSACTION_ELEMENT_WRITE);

					// Just unqueue on success
					thread_unqueue_from_lock(thread);
					// We can unqueue any time, but should signal waiters only after unqueueing and preferably after releasing the lock.
					// notify_next_writer(thread, lock);

					shmassert_msg(final_result != RESULT_INVALID, "something very wrong happened");
					if (final_result == RESULT_OK)
					{
						if (lock_taken) *lock_taken = true;
						p_atomic_int_add(&lock->writers_count, 1);
						shm_cell_check_write_lock(thread, lock);
					}
					else
						shmassert(shm_cell_have_write_lock(thread, lock) == false);

					return final_result;
				}
			}
			break;
			default:
				shmassert_msg(false, "Invalid thread->transaction_lock_mode");
				return RESULT_FAILURE;
		}
	}

	return RESULT_OK;
}

// status is a status of the last locking operation
int
transaction_unlock_local(ThreadContext *thread, ShmLock *lock, ShmPointer lock_shm, int status, bool lock_taken)
{
	return RESULT_OK;
	shmassert(thread->transaction_mode != LOCKING_NONE);
	if ( ! lock_taken)
		return RESULT_OK;
	if (thread->transaction_mode == TRANSACTION_TRANSIENT)
	{
		shmassert_msg(shm_cell_have_write_lock(thread, lock) ||
			atomic_bitmap_check_me(&lock->reader_lock, thread->index), "Thread does not own the lock.");
		// lock->id = 0;
		commit_transaction(thread, NULL);
	}
	else // LOCKING_WRITER || LOCKING_ALL
	{
		// handle the wait or abort status at higher level
	}

	return RESULT_OK;
}

int
transaction_push_mode(ThreadContext *thread, int transaction_mode, int transaction_lock_mode)
{
	shmassert_msg(thread->private_data->transaction_stack->count < TRANSACTION_STACK_SIZE,
		"thread->transaction_stack->count < TRANSACTION_STACK_SIZE");

	int idx = thread->private_data->transaction_stack->count; // this way we can at least catch some misuse
	thread->private_data->transaction_stack->modes[idx].transaction_mode = thread->transaction_mode;
	thread->private_data->transaction_stack->modes[idx].transaction_lock_mode = thread->transaction_lock_mode;
	thread->private_data->transaction_stack->count++;

	thread->transaction_mode = transaction_mode;
	thread->transaction_lock_mode = transaction_lock_mode;
	return idx;
}

int
transaction_pop_mode(ThreadContext *thread, int *transaction_mode, int *transaction_lock_mode)
{
	shmassert_msg(thread->private_data->transaction_stack->count > 0, "thread->private_data->transaction_stack->count > 0");

	int idx = thread->private_data->transaction_stack->count - 1; // this way we can at least catch some misuse
	thread->transaction_mode = thread->private_data->transaction_stack->modes[idx].transaction_mode;
	thread->transaction_lock_mode = thread->private_data->transaction_stack->modes[idx].transaction_lock_mode;
	thread->private_data->transaction_stack->count--;
	if (transaction_mode)
		*transaction_mode = thread->transaction_mode;
	if (transaction_lock_mode)
		*transaction_lock_mode = thread->transaction_lock_mode;

	return thread->private_data->transaction_stack->count;
}

int
transaction_parent_mode(ThreadContext *thread, int *transaction_mode, int *transaction_lock_mode)
{
	int idx = thread->private_data->transaction_stack->count - 1;
	if (thread->private_data->transaction_stack->count > 0)
	{
		int idx = thread->private_data->transaction_stack->count - 1;
		if (transaction_mode)
			*transaction_mode = thread->private_data->transaction_stack->modes[idx].transaction_mode;
		if (transaction_lock_mode)
			*transaction_mode = thread->private_data->transaction_stack->modes[idx].transaction_lock_mode;
	}
	else
	{

	}
	return idx;
}

int
transaction_length(ThreadContext *thread)
{
	int cnt = 0;
	ShmTransactionElement *element = thread->current_transaction;
	while (element)
	{
		shmassert_msg(element->owner == NONE_SHM || element->owner == thread->self,
			"element->owner == NONE_SHM || element->owner == thread->self");
		cnt++;
		shmassert(LOCAL(element->next_shm) == element->next);
		element = element->next;
	}
	return cnt;
}

void
thread_refresh_ticket(ThreadContext *thread)
{
	int ticket = p_atomic_int_add(&superblock->ticket, 1);
	if (ticket != 0) // zero is reserved
		thread->last_start = ticket;
	else
		thread->last_start = p_atomic_int_add(&superblock->ticket, 1);

	superblock->ticket_history[thread->last_start % 64] = thread->self;
}

int
transaction_end(ThreadContext *thread, bool rollback)
{
	ShmTransactionElement *element = thread->current_transaction;
	while (element)
	{
		shmassert_msg(element->owner == NONE_SHM || element->owner == thread->self,
			"element->owner == NONE_SHM || element->owner == thread->self");
		if (element->type == TRANSACTION_ELEMENT_WRITE)
		{
			shm_types_transaction_end(thread, element, rollback);
		}
		element = element->next;
	}
	return RESULT_OK;
}

ShmTransactionElement *
transaction_unlock_all(ThreadContext *thread)
{
	shmassert(LOCAL(thread->current_transaction_shm) == thread->current_transaction);
	ShmTransactionElement *rslt = thread->current_transaction;
	while (thread->current_transaction)
	{
		ShmTransactionElement *element = thread->current_transaction;

		shmassert(element->type == TRANSACTION_ELEMENT_READ || element->type == TRANSACTION_ELEMENT_WRITE);

		if (element->container_type != CONTAINER_NONE)
		{
			ShmContainer *container = LOCAL(element->container);
			if (element->type == TRANSACTION_ELEMENT_READ)
			{
				p_atomic_int_dec_and_test(&container->lock.readers_count);
				shmassert(p_atomic_int_get(&container->lock.readers_count) >= 0);
				int ref = shm_cell_have_write_lock(thread, &container->lock) ? 1 : 0;
				shmassert(p_atomic_int_get(&container->lock.writers_count) == ref);
			}
			else
			{
				p_atomic_int_dec_and_test(&container->lock.writers_count);
				shmassert(p_atomic_int_get(&container->lock.writers_count) == 0);
				int ref = shm_cell_have_read_lock(thread, &container->lock) ? 1 : 0;
				shmassert(p_atomic_int_get(&container->lock.readers_count) == ref);
			}
		}
		shm_types_transaction_unlock(thread, element);

		ShmPointer to_release = thread->current_transaction_shm;
		SHM_UNUSED(to_release);
		thread->current_transaction_shm = thread->current_transaction->next_shm;
		thread->current_transaction = thread->current_transaction->next;
		// free_mem(thread, to_release, sizeof(ShmTransactionElement));
		shmassert(LOCAL(thread->current_transaction_shm) == thread->current_transaction);
	}
	return rslt;
}

// is_initial determines whether it is the first try on this transaction or a retry
int
start_transaction(ThreadContext *thread, int mode, int locking_mode, int is_initial, int *recursion_count)
{
	shmassert(mode != TRANSACTION_NONE);
	shmassert(mode != TRANSACTION_TRANSIENT);

	shmassert_msg(thread->transaction_mode != TRANSACTION_TRANSIENT, "Transient transaction is not complete, but a new transaction is started");
	int newmode = thread->transaction_mode;
	if (mode > thread->transaction_mode)
	{
		newmode = mode;
	}
	int newlocking = thread->transaction_lock_mode;
	if (locking_mode > thread->transaction_lock_mode)
	{
		newlocking = locking_mode;
	}
	if (thread->fresh_starts || thread->transaction_mode < TRANSACTION_PERSISTENT)
	{
		if (superblock->debug_lock_count > superblock->debug_max_lock_count)
			fprintf(stderr, "superblock->debug_lock_count %d out of max %d\n", superblock->debug_lock_count, superblock->debug_max_lock_count);
		if (thread->private_data->pending_lock_count > 1)
			fprintf(stderr, "more than one pending lock\n");
		shmassert(superblock->debug_lock_count <= superblock->debug_max_lock_count);
		shmassert(thread->private_data->pending_lock_count <= 1);

		shmassert(thread->pending_lock == EMPTY_SHM);
		thread_unqueue_from_lock(thread); // Indicate we are not in queue as soon as possible.
										  // Transaction end is a better place, but we still need it here for transient->persistent transition.
		if (is_initial) // for scheduling/prioritization)
		{
			thread_refresh_ticket(thread);
		}
		thread_reset_preempted(thread); // reset the "preempted" flag the last, because non-running transaction cannot be preempted
										// and we start registering preemption only after transaction really started.
	}
	int rslt = transaction_push_mode(thread, newmode, newlocking); // always push for corresponding pop
	if (recursion_count)
		*recursion_count = rslt;

	// if (is_initial) // for scheduling/prioritization
	// 	thread->last_start = GetTickCount(); // rel fence, atomic

	return RESULT_OK;
}

void
continue_transaction(ThreadContext *thread)
{
	shm_event_reset(&thread->ready);
}

int
commit_transaction(ThreadContext *thread, int *recursion_count)
{
	int mode = -1;
	int rslt = transaction_parent_mode(thread, &mode, NULL);
	if (recursion_count)
		*recursion_count = rslt;

	// if (thread->fresh_starts)
	// 	if (p_atomic_shm_pointer_get(&thread->thread_preempted) != 0)
	// 		p_atomic_shm_pointer_set(&thread->thread_preempted, 0);
	if (mode <= TRANSACTION_TRANSIENT)
	{
		// persistent transactions are aborted as a whole.
		shmassert(thread->pending_lock == EMPTY_SHM);
		thread_unqueue_from_lock(thread);
		transaction_end(thread, false);
		ShmTransactionElement *debug = transaction_unlock_all(thread);
		SHM_UNUSED(debug);
		thread_reset_preempted(thread);
		// can mark transaction as finished here, no local pointers allowed afterwards
		shmassert(thread->private_data->write_locks_taken == 0);
		shmassert(thread->private_data->read_locks_taken == 0);
	}
	transaction_pop_mode(thread, NULL, NULL);
	shmassert(mode == thread->transaction_mode);
	// Reclaimer will check the mode once again after setting the test_finished,
	// so we modify them in reverse order here to ensure reclaimer won't be waiting for inactive thread.
	if (mode <= TRANSACTION_TRANSIENT)
		p_atomic_int_set(&thread->test_finished, 0);
	return rslt;
}

int
abort_transaction(ThreadContext *thread, int *recursion_count)
{
	// Reclaimer uses transaction mode to determine the thread is idle.
	// Thus we lower the transaction mode only after we've released its pointers
	int mode = -1;
	int rslt = transaction_parent_mode(thread, &mode, NULL);
	if (recursion_count)
		*recursion_count = rslt;
	shmassert(thread->transaction_mode != TRANSACTION_TRANSIENT);
	if (thread->fresh_starts || thread->transaction_mode <= TRANSACTION_TRANSIENT)
	{
		shmassert(thread->pending_lock == EMPTY_SHM);
		thread_unqueue_from_lock(thread);
	}
	// // unconditionally, release locks for other waiters ASAP
	// Okay, we can actually have aborts from transient transactions which should not lead to a global abort
	if (thread->transaction_mode <= TRANSACTION_TRANSIENT)
	{
		transaction_end(thread, true);
		ShmTransactionElement *debug = transaction_unlock_all(thread);
		SHM_UNUSED(debug);
		thread_reset_preempted(thread); // we don't need to do this when transaction ends, but anyways.
		shmassert(thread->private_data->read_locks_taken == 0);
		shmassert(thread->private_data->write_locks_taken == 0);
	}
	transaction_pop_mode(thread, NULL, NULL);
	// Reclaimer will check the mode once again after setting the test_finished,
	// so we modify them in reverse order here to ensure reclaimer won't be waiting for inactive thread.
	if (mode <= TRANSACTION_TRANSIENT)
		p_atomic_int_set(&thread->test_finished, 0);

	return RESULT_OK;
}

int
abort_transaction_retaining(ThreadContext *thread)
{
	// int mode;
	// int rslt = transaction_pop_mode(thread, &mode, NULL);
	// if (recursion_count)
	// 	*recursion_count = rslt;
	int parent_mode;
	if (transaction_parent_mode(thread, &parent_mode, NULL) < 0)
		parent_mode = TRANSACTION_NONE;
	if (thread->fresh_starts || parent_mode <= TRANSACTION_TRANSIENT)
	{
		shmassert(thread->pending_lock == EMPTY_SHM);
		thread_unqueue_from_lock(thread);
	}
	if (parent_mode <= TRANSACTION_TRANSIENT)
	{
		transaction_end(thread, true);
		ShmTransactionElement *debug = transaction_unlock_all(thread);
		SHM_UNUSED(debug);
		thread_reset_preempted(thread);
		shmassert(thread->private_data->write_locks_taken == 0);
		shmassert(thread->private_data->read_locks_taken == 0);
	}
	if (parent_mode <= TRANSACTION_TRANSIENT)
		p_atomic_int_set(&thread->test_finished, 0);

	return RESULT_OK;
}

int
abort_transaction_retaining_debug_preempt(ThreadContext *thread)
{
	// thread->thread_preempted = 0;
	transaction_end(thread, true);
	ShmTransactionElement *debug = transaction_unlock_all(thread);
	SHM_UNUSED(debug);
	thread_reset_preempted(thread);
	shmassert(thread->private_data->write_locks_taken == 0);
	shmassert(thread->private_data->read_locks_taken == 0);

	p_atomic_int_set(&thread->test_finished, 0);

	return RESULT_OK;
}

bool transient_pause = false;

// similar to abort_transaction_retaining
int
transient_commit(ThreadContext *thread)
{
	// We also need to register a transient operation start (including shm_pointer_acq)
	// so the coordinator won't be waiting for us.
	int mode = thread->transaction_mode;
	shmassert_msg(mode >= TRANSACTION_TRANSIENT, "Commiting inactive transaction");
	if (mode == TRANSACTION_TRANSIENT)
	{
		if (transient_pause) Sleep(0);
		shmassert(thread->pending_lock == EMPTY_SHM);
		thread_unqueue_from_lock(thread);
		transaction_end(thread, false);
		ShmTransactionElement *debug = transaction_unlock_all(thread);
		SHM_UNUSED(debug);
		thread_refresh_ticket(thread); // here we reset the ticket at commit time rather than start because transient transaction has no apparent start
		thread_reset_preempted(thread);
		shmassert(thread->private_data->write_locks_taken == 0);
		shmassert(thread->private_data->read_locks_taken == 0);

		// Same as comit_transaction/abort_transaction, change transaction_mode after releasing data, but before resetting flag.
		p_atomic_int_set(&thread->transaction_mode, TRANSACTION_IDLE);
		p_atomic_int_set(&thread->test_finished, 0);

		return RESULT_OK;
	}
	return RESULT_INVALID;
}

int
transient_abort(ThreadContext *thread)
{
	// same as transient_commit except transaction_end
	int mode = thread->transaction_mode;
	shmassert_msg(mode >= TRANSACTION_TRANSIENT, "Commiting inactive transaction");
	if (mode == TRANSACTION_TRANSIENT)
	{
		shmassert(thread->pending_lock == EMPTY_SHM);
		thread_unqueue_from_lock(thread);
		transaction_end(thread, true);
		ShmTransactionElement *debug = transaction_unlock_all(thread);
		SHM_UNUSED(debug);
		thread_reset_preempted(thread);
		shmassert(thread->private_data->write_locks_taken == 0);
		shmassert(thread->private_data->read_locks_taken == 0);

		// Same as comit_transaction/abort_transaction, change transaction_mode after releasing data, but before resetting flag.
		p_atomic_int_set(&thread->transaction_mode, TRANSACTION_IDLE);
		p_atomic_int_set(&thread->test_finished, 0);

		return RESULT_OK;
	}
	return RESULT_INVALID;
}

void
transient_check_clear(ThreadContext *thread)
{
	if (thread->transaction_mode <= TRANSACTION_TRANSIENT)
	{
		shmassert(SBOOL(thread->current_transaction_shm) == false);
		shmassert(thread->current_transaction == NULL);
	}
}

int
init_thread_context(ThreadContext **context)
{
	shmassert(superblock);
	int index = -1;
	int heap_index = -1;
	// those two are linked anyway
	take_spinlock(CAS2, &superblock->superheap.lock, 1, 0, {
		if (CAS2(&superblock->superheap.lock, 1, 2))
		{
			// initial
			heap_index = 0;
			break;
		}
	});
	take_spinlock(CAS2, &superblock->threads.lock, 1, 0, {
		if (CAS2(&superblock->threads.lock, 1, 2))
		{
			// initial
			index = 0;
			break;
		}
	});
	shmassert(superblock->superheap.lock == 1);
	shmassert(superblock->threads.lock == 1);

	if (heap_index == -1)
	{
		for (int i = 0; i < MAX_THREAD_COUNT; ++i)
		{
			if (superblock->superheap.heaps[i].thread == 0)
			{
				heap_index = i;
				break;
			}
		}
	};
	if (index == -1)
	{
		for (int i = 0; i < MAX_THREAD_COUNT; ++i)
		{
			if (superblock->threads.threads[i] == 0)
			{
				index = i;
				break;
			}
		}
	};

	int rslt = RESULT_FAILURE;
	*context = 0;
	if (index >= 0 && heap_index >= 0)
	{
		ShmPointer self = EMPTY_SHM;
		ThreadContext *thread = (ThreadContext *)get_mem(NULL, &self, sizeof(ThreadContext), SHM_THREAD_CONTEXT_ID);
		memset(CAST_VL(thread), 0, sizeof(ThreadContext));
		thread->magic = SHM_THREAD_MAGIC;
		thread->self = self;
		thread->private_data = calloc(1, sizeof(ThreadContextPrivate));

		superblock->threads.threads[index] = self;
		superblock->superheap.heaps[heap_index].thread = self; // claiming the heap slot
		// superblock->superheap.heaps[heap_index].size = sizeof(ShmHeap); - inited in init_superblock
		thread->index = index;
		shmassert(thread->index >= 0 && thread->index < isizeof(ShmReaderBitmap));

		thread->thread_state = THREAD_NORMAL;
		thread->thread_preempted = EMPTY_SHM;
		thread->transaction_mode = LOCKING_NONE;
		// thread->last_start = GetTickCount();
		thread->last_start = 0;
		thread->test_finished = FALSE;
		thread->async_mode = FALSE;
		thread->free_list = DEBUG_SHM;
		thread->heap = pack_shm_pointer((intptr_t)&superblock->superheap.heaps[heap_index] - (intptr_t)superblock, SHM_INVALID_BLOCK);
		shmassert(shm_pointer_to_pointer_root(thread->heap) == &superblock->superheap.heaps[heap_index]);

		thread->local_vars = (LocalReferenceBlock *)get_mem(NULL, &thread->local_vars_shm, sizeof(LocalReferenceBlock), THREAD_LOCAL_VARS_DEBUG_ID);
		memset(thread->local_vars, 0, sizeof(LocalReferenceBlock));

		// thread->transaction_stack = (ShmTransactionStack *)get_mem(NULL, &thread->transaction_stack_shm, sizeof(ShmTransactionStack));
		thread->private_data->transaction_stack = calloc(1, sizeof(ShmTransactionStack));
		thread->private_data->transaction_stack->count = 0;

		thread->current_transaction_shm = EMPTY_SHM;

		thread->pending_lock = EMPTY_SHM;

		// thread->ready = 0;
		// thread->ready_event = 0;
		shm_event_init(&thread->ready);

		thread->waiting_for_lock = EMPTY_SHM;
		thread->next = EMPTY_SHM;

		// moved above
		// superblock->threads.threads[index] = self;

		// superblock->superheap.heaps[heap_index].lock = 0;
		shm_simple_lock_init(&superblock->superheap.heaps[heap_index].lock); // it won't be used earlier anyway
		superblock->superheap.heaps[heap_index].owner = ShmGetCurrentThreadId();
		superblock->superheap.heaps[heap_index].count = 0;
		superblock->superheap.heaps[heap_index].large_segment = EMPTY_SHM;
		superblock->superheap.heaps[heap_index].fixed_sectors.head = EMPTY_SHM;
		superblock->superheap.heaps[heap_index].fixed_sectors.tail = EMPTY_SHM;
		superblock->superheap.heaps[heap_index].flex_sectors.head = EMPTY_SHM;
		superblock->superheap.heaps[heap_index].flex_sectors.tail = EMPTY_SHM;

		*context = thread;
		rslt = RESULT_OK;
	}

	if (superblock->threads.lock == 1)
		superblock->threads.lock = 0;
	if (superblock->superheap.lock == 1)
		superblock->superheap.lock = 0;
	return rslt;
}

void
shm_thread_reset_debug_counters(ThreadContext *thread)
{
	thread->private_data->times_aborted = 0;
	thread->private_data->times_waiting = 0;
	thread->private_data->times_waiting2 = 0;
	thread->private_data->times_repeated = 0;
	thread->private_data->times_aborted1 = 0;
	thread->private_data->times_aborted2 = 0;
	thread->private_data->times_aborted3 = 0;
	thread->private_data->times_aborted4 = 0;
	thread->private_data->times_aborted5 = 0;
	thread->private_data->times_aborted6 = 0;
	thread->private_data->times_aborted7 = 0;
	thread->private_data->times_aborted8 = 0;
	thread->private_data->times_aborted9 = 0;
	thread->private_data->tickets_aborted = 0;
	thread->private_data->tickets_waiting = 0;
	thread->private_data->tickets_waiting2 = 0;
	thread->private_data->tickets_repeated = 0;
	thread->private_data->tickets_aborted1 = 0;
	thread->private_data->tickets_aborted2 = 0;
	thread->private_data->tickets_aborted3 = 0;
	thread->private_data->tickets_aborted4 = 0;
	thread->private_data->tickets_aborted5 = 0;
	thread->private_data->tickets_aborted6 = 0;
	thread->private_data->tickets_aborted7 = 0;
	thread->private_data->tickets_aborted8 = 0;
	thread->private_data->tickets_aborted9 = 0;
}

// ShmPointer routines

ShmWord
shm_pointer_get_block(ShmPointer pntr)
{
	return (pntr) >> SHM_OFFSET_BITS;
}

ShmWord
shm_pointer_get_offset(ShmPointer pntr)
{
	return (pntr) & SHM_INVALID_OFFSET; // 18 bits
}

bool
shm_pointer_is_valid(ShmPointer pntr)
{
	return SBOOL(pntr) && shm_pointer_get_offset(pntr) != SHM_INVALID_OFFSET &&
	       shm_pointer_get_offset(pntr) > SHM_FIXED_CHUNK_HEADER_SIZE; // only internal routines can read the header
}

bool
SBOOL(ShmPointer pntr)
{
	return shm_pointer_get_block(pntr) != SHM_INVALID_BLOCK && pntr != NONE_SHM;
}

void *
shm_pointer_to_pointer_root(ShmPointer pntr)
{
	ShmWord idx = shm_pointer_get_block(pntr);
	vl char *block;
	if (idx == SHM_INVALID_BLOCK)
		block = (vl char *)superblock;
	else
	{
		// block = superblock_get_block(idx);
		block = NULL;
		shmassert(false);
	}

	if (!block)
		return NULL;
	ShmWord offset = shm_pointer_get_offset(pntr);
	if (idx == SHM_INVALID_BLOCK)
	{
		shmassert(offset < isizeof(ShmSuperblock));
		if (offset >= isizeof(ShmSuperblock))
			return NULL;
	}
	else
	{
		// shmassert(offset < superblock->blocks[idx].size, NULL);
		// if (offset >= superblock->blocks[idx].size)
		// 	return NULL;

		shmassert(offset < SHM_FIXED_CHUNK_SIZE);
		if (offset >= SHM_FIXED_CHUNK_SIZE)
			return NULL;
	}
	return CAST_VL(block + offset);
}

void *
shm_pointer_to_pointer_unsafe(ShmPointer pntr)
{
	ShmWord idx = shm_pointer_get_block(pntr);
	vl char *block;
	if (idx == SHM_INVALID_BLOCK)
		return NULL;
	else
		block = superblock_get_block(idx);

	if (!block)
		return NULL;

	ShmWord offset = shm_pointer_get_offset(pntr);
	if (idx == SHM_INVALID_BLOCK)
		return NULL;
	else
	{
		shmassert(offset < SHM_FIXED_CHUNK_SIZE);
		if (offset >= SHM_FIXED_CHUNK_SIZE)
			return NULL;
	}
	return CAST_VL(block + offset);
}

void *
shm_pointer_to_pointer(ShmPointer pntr)
{
	if (!shm_pointer_is_valid(pntr))
		return NULL;
	return shm_pointer_to_pointer_unsafe(pntr);
}


void *
shm_pointer_to_pointer_no_side(ShmPointer pntr)
{
	ShmWord idx = shm_pointer_get_block(pntr);
	vl char *block;
	block = superblock_mmap[idx / SHM_BLOCK_GROUP_SIZE];
	if (!block)
		return NULL;

	ShmWord offset = shm_pointer_get_offset(pntr);
	if (idx == SHM_INVALID_BLOCK)
	{
		return NULL;
	}
	else
	{
		// shmassert(offset < superblock->blocks[idx].size, NULL);
		// if (offset >= superblock->blocks[idx].size)
		// 	return NULL;
		shmassert(offset < SHM_FIXED_CHUNK_SIZE);
		if (offset >= SHM_FIXED_CHUNK_SIZE)
			return NULL;
	}
	return CAST_VL(block + SHM_FIXED_CHUNK_SIZE * (idx % SHM_BLOCK_GROUP_SIZE) + offset);
}

ShmPointer
pack_shm_pointer(ShmWord offset, ShmWord block)
{
	shmassert(offset >= 0);
	shmassert(offset <= SHM_INVALID_OFFSET);
	shmassert(block >= 0);
	shmassert(block <= SHM_INVALID_BLOCK);
	uintptr_t _offset = (uintptr_t)offset & SHM_INVALID_OFFSET;
	uintptr_t _block = (uintptr_t)block & SHM_INVALID_BLOCK;
	return (_block << SHM_OFFSET_BITS) | _offset;
}

ShmPointer
pointer_to_shm_pointer(void *pntr, int block)
{
	shmassert(block <= SHM_INVALID_BLOCK);
	vl char *block_base = superblock_get_block_noside(block);
	int offset = (char*)pntr - block_base;
	shmassert(offset >= 0);
	shmassert(offset < SHM_FIXED_CHUNK_SIZE);
	return pack_shm_pointer(offset, block);
}

ShmPointer
shm_pointer_shift(ShmPointer pointer, int offset)
{
	int block = shm_pointer_get_block(pointer);
	int orig_offset = shm_pointer_get_offset(pointer);
	orig_offset = orig_offset + offset;
	if (orig_offset >= SHM_INVALID_OFFSET)
		return EMPTY_SHM;
	return pack_shm_pointer(orig_offset, block);
}
// End of ShmPointer routines

vl char *
debug_id_to_str(int id)
{
	id &= 0xFFFFFF; // filter off the "0x1D >> 24" part
	shmassert(id >= 0 && id < MAX_DEBUG_ID);
	return superblock->type_debug_ids[id];
}

int
init_superblock(const char *id)
{
	uint32_t v1 = (((uintptr_t)SHM_INVALID_BLOCK) << SHM_OFFSET_BITS) +
		SHM_INVALID_OFFSET;
	uint32_t v2 = ~(uint32_t)0;
	shmassert(v1 == v2);

	init_mm_maps();
	init_coordinator();
#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
	init_private_events();
#endif

	long memsize = sizeof(ShmSuperblock);
	superblock_desc.size = memsize;
	bool existing = !!id;
	if (existing)
	{
		memclear(&superblock_desc.id[0], SHM_SAFE_NAME_LENGTH + 1);
		strcpy_s(CAST_VL(&superblock_desc.id[0]), SHM_SAFE_NAME_LENGTH + 1, id);
	}
	srand((unsigned)time(NULL)); // otherwise we will be getting same names every time
	// superblock = allocate_block((__ShmChunk *)&superblock_desc, !existing, memsize, true, NULL, &hFileMapping);
	ChunkAllocationResult alloc_result = shm_chunk_allocate((__ShmChunk*)&superblock_desc, !existing, memsize);
	superblock = alloc_result.data;
	if (!superblock)
		return RESULT_FAILURE;
	ShmProcessID coordinator_process = 0;
	bool i_am_the_coordinator = false;
	if (existing)
		coordinator_process = superblock->coordinator_process;
	else
	{
		// superblock->coordinator_process is not initialized yet
		i_am_the_coordinator = !existing;
	}
	shm_chunk_register_handle(&superblock_desc, alloc_result.FileMapping, coordinator_process, i_am_the_coordinator);

	if (!existing) {
		memclear(superblock, sizeof(ShmSuperblock));
		superblock->type = SHM_BLOCK_TYPE_SUPER;

		superblock->coordinator_process = ShmGetCurrentProcessId();
		memclear(&superblock->coordinator_data, sizeof(ShmCoordinatorData));
		shm_simple_lock_init(&superblock->lock);

		superblock->block_count = 0;
		superblock->ticket = 0;
		superblock->mm_last_used_root_sector = (ShmInt)-1;
		superblock->last_available_event_id = 0;
		// memclear(superblock->block_groups);
		superblock->threads.size = sizeof(ShmThreads);
		superblock->threads.count = MAX_THREAD_COUNT;
		superblock->threads.lock = 2;

		superblock->superheap.size = sizeof(ShmSuperheap);
		superblock->superheap.count = MAX_THREAD_COUNT;
		superblock->superheap.lock = 2;
		for (int i = 0; i < MAX_THREAD_COUNT; ++i)
		{
			superblock->superheap.heaps[i].size = sizeof(ShmHeap);
		}
		superblock->superheap.self = pack_shm_pointer((intptr_t)&superblock->superheap - (intptr_t)superblock, SHM_INVALID_BLOCK);

		superblock->has_garbage = 0;
		// event initialization uses superblock variable, so we do it last.
		shm_event_init(&superblock->has_garbage_event);

		superblock->root_container = EMPTY_SHM; // need thread context for this one
		superblock->debug_max_lock_count = 100;

		init_debug_type_ids(&superblock->type_debug_ids);

		start_coordinator();
	}
	return RESULT_OK;
}

void
release_superblock(bool is_coordinator)
{
	bool is_coordinator2 = ShmGetCurrentProcessId() == superblock->coordinator_process;
	shmassert(is_coordinator2 == is_coordinator);
	if (is_coordinator)
		stop_coordinator_and_wait(); // should also check for all the threads to be stopped

	superblock_release_all_mmaps(is_coordinator);
	shm_chunk_release(&superblock_desc, superblock, is_coordinator);
}

ShmRefcountedBlock *
shm_pointer_to_refcounted(ThreadContext *thread, ShmPointer pointer, bool strict_refcounted, bool strict_check_type)
{
	if (SBOOL(pointer) == false)
		return NULL;
	ShmRefcountedBlock *ref_block = (ShmRefcountedBlock *)LOCAL(pointer);
	shmassert(ref_block);
	ShmInt type = ref_block->type;
	shmassert(!(CHECK_FLAG(type, SHM_TYPE_FLAG_CONTAINED) && CHECK_FLAG(type, SHM_TYPE_FLAG_REFCOUNTED))); // mutually exclusive
	if (CHECK_FLAG(type, SHM_TYPE_FLAG_CONTAINED))
	{
		// Container registers the total reference count for all the contained blocks instead.
		ref_block = LOCAL(((ShmContainedBlock*)ref_block)->container);
		shmassert(CHECK_FLAG(type, SHM_TYPE_FLAG_REFCOUNTED));
	}
	else
	{
		if (strict_refcounted)
			shmassert(CHECK_FLAG(type, SHM_TYPE_FLAG_REFCOUNTED));
		else if (!CHECK_FLAG(type, SHM_TYPE_FLAG_REFCOUNTED))
			return NULL;

		if (strict_check_type)
			shmassert(ref_block->type & SHM_TYPE_FLAG_REFCOUNTED ||
			         ref_block->type == SHM_TYPE_LIST_BLOCK ||
			         ref_block->type == SHM_TYPE_LIST_CHANGES ||
			         ref_block->type == SHM_TYPE_QUEUE_CHANGES ||
			         ref_block->type == SHM_TYPE_DICT_DELTA);
	}

	shmassert(((puint)ref_block->type & SHM_TYPE_RELEASE_MARK) == 0); // otherwise it was released
	if (thread)
	{
		ShmInt current_refcount = p_atomic_int_get(&ref_block->refcount);
		if (current_refcount == 0)
			shmassert_msg(thread->transaction_mode >= TRANSACTION_TRANSIENT,
			             "Mister, don't use unacquired references outside transaction");
	}

	return ref_block;
}

vl void *
shm_pointer_acq(ThreadContext *thread, ShmPointer pointer)
{
	// shm_pointer_to_refcounted will complain if the item revived outside transaction
	ShmRefcountedBlock *block = shm_pointer_to_refcounted(thread, pointer, true, false);

	shmassert(block);
	ShmInt oldval = p_atomic_int_add(&block->refcount, 1);
	if (0 == oldval)
	{
		// Item revived, register this fact so the coordinator will wait for our release.
		// Of course that implies we are in transaction right now, otherwise we are not allowed to see this pointer at all.
		p_atomic_int_add(&block->revival_count, 1);
	}
	return block;
}

// true if item released
bool
shm_pointer_release(ThreadContext *thread, ShmPointer pointer)
{
	ShmRefcountedBlock *block = shm_pointer_to_refcounted(thread, pointer, true, false);
	if (block == NULL)
		return false;

	ShmInt final_cnt = p_atomic_int_dec(&block->refcount);
	shmassert(final_cnt >= 0);
	if (final_cnt == 0)
	{
		shm_refcounted_block_before_release(thread, pointer, block);
		free_mem(thread, pointer, block->size);
	}
	return final_cnt == 0;
}

int
shm_pointer_refcount(ThreadContext *thread, ShmPointer pointer)
{
	ShmRefcountedBlock *block = shm_pointer_to_refcounted(thread, pointer, true, false);

	return p_atomic_int_get(&block->refcount);
}

LocalReference *
thread_local_ref(ThreadContext *thread, ShmPointer pointer)
{
	shmassert((thread->local_vars->count < LOCAL_REFERENCE_BLOCK_SIZE));
	//thread_local.local_references[thread_local.last_free_local_ref] = pointer;
	//thread_local.last_free_local_ref ++ ;
	//return &thread_local.local_references[thread_local.last_free_local_ref];

	int idx = thread->local_vars->count;
	LocalReference *rslt = &(thread->local_vars->references[idx]);
	// Increment count only when the item is completely valid, decrement count before we invalidate the item,
	// so a reader from outside won't corrupt memory.
	rslt->shared = pointer;
	rslt->local = NULL;
	rslt->owned = FALSE;
	thread->local_vars->count += 1;
	return rslt;
}

void
thread_local_clear_ref(ThreadContext *thread, LocalReference *reference)
{
	shmassert((char*)&thread->local_vars->references[0] <= (char*)reference &&
		(char*)reference <= (char*)&thread->local_vars->references[LOCAL_REFERENCE_BLOCK_SIZE]
	);
	// more strict assertion
	shmassert(thread->local_vars->count > 0);
	shmassert( (char*)reference <= (char*)&thread->local_vars->references[thread->local_vars->count - 1]
	);
	//if (thread_local.last_free_local_ref > 0 && &thread_local.local_references[thread_local.last_free_local_ref - 1] == pointer)
	//	thread_local.last_free_local_ref--;
	ShmPointer ref = reference->shared;
	reference->local = NULL;
	reference->shared = EMPTY_SHM;
	if (reference->owned)
	{
		reference->owned = FALSE;
		bool rslt = shm_pointer_release(thread, ref);
		SHM_UNUSED(rslt);
		// assert(rslt >= 0);
	}
}

void
thread_local_clear_refs(ThreadContext *thread)
{
	//long tmp = thread_local.last_free_local_ref;
	//memset(&thread_local.local_references[0], 0, tmp * sizeof(ShmPointer));
	//thread_local.last_free_local_ref = 0;

	while (thread->local_vars->count > 0)
	{
		int idx = thread->local_vars->count - 1;
		thread_local_clear_ref(thread, & thread->local_vars->references[idx]);
		thread->local_vars->count--;
	}
}

PShmPointer
shm_pointer_empty(ThreadContext *thread, PShmPointer pointer)
{
	ShmPointer tmp = *pointer;
	*pointer = EMPTY_SHM;
	shm_pointer_release(thread, tmp);
	return pointer;
}

PShmPointer
shm_pointer_empty_atomic(ThreadContext *thread, PShmPointer pointer)
{
	ShmPointer tmp = p_atomic_shm_pointer_get(pointer);
	p_atomic_shm_pointer_set(pointer, EMPTY_SHM);
	shm_pointer_release(thread, tmp);
	return pointer;
}

PShmPointer
shm_next_pointer_empty(ThreadContext *thread, PShmPointer pointer)
{
	ShmPointer tmp = *pointer;
	*pointer = NONE_SHM;
	shm_pointer_release(thread, tmp);
	return pointer;
}

ShmPointer
shm_pointer_copy(ThreadContext *thread, PShmPointer dest, ShmPointer source)
{
	ShmPointer tmp_rel = *dest;
	if (SBOOL(source))
		shm_pointer_acq(thread, source);

	*dest = source;

	if (SBOOL(tmp_rel))
		shm_pointer_release(thread, tmp_rel);
	return source;
}

PShmPointer
shm_pointer_move(ThreadContext *thread, PShmPointer pointer, PShmPointer newval)
{
	ShmPointer tmp = *pointer;
	*pointer = *newval;
	*newval = EMPTY_SHM;
	shm_pointer_release(thread, tmp);
	return pointer;
}

PShmPointer
shm_pointer_move_atomic(ThreadContext *thread, PShmPointer pointer, PShmPointer newval)
{
	ShmPointer tmp = p_atomic_shm_pointer_get(pointer);
	p_atomic_shm_pointer_set(pointer, p_atomic_shm_pointer_get(newval));
	p_atomic_shm_pointer_set(newval, EMPTY_SHM);
	shm_pointer_release(thread, tmp);
	return pointer;
}

PShmPointer
shm_next_pointer_move(ThreadContext *thread, PShmPointer pointer, PShmPointer newval)
{
	ShmPointer tmp = *pointer;
	*pointer = *newval;
	*newval = NONE_SHM;
	shm_pointer_release(thread, tmp);
	return pointer;
}

// tabs-spaces checked

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

int
init_private_events()
{
	private_events = malloc(sizeof(ShmPrivateEvents));
	memset((void*)private_events, 0xFF, sizeof(ShmPrivateEvents));
	private_events->count = 0;
	return RESULT_OK;
}

// Duplicate the handle into coordinator's process
ShmHandle
handle_to_coordinator(ShmProcessID coordinator_process, ShmHandle handle, bool close)
{
	HANDLE proc = OpenProcess(PROCESS_DUP_HANDLE, 0, coordinator_process);
	HANDLE result = INVALID_HANDLE_VALUE;
	DWORD flags = DUPLICATE_SAME_ACCESS;
	if (close) flags |= DUPLICATE_CLOSE_SOURCE;
	if (!DuplicateHandle(GetCurrentProcess(), handle, proc, &result, 0, false, flags))
	{
		fprintf(stderr, "handle_to_coordinator: Error %ld\n",
		GetLastError());
		shmassert(false);
	}
	CloseHandle(proc);
	return result;
}

ShmHandle
handle_to_private(ShmProcessID coordinator_process, ShmHandle handle)
{
	HANDLE proc = OpenProcess(PROCESS_DUP_HANDLE, 0, coordinator_process);
	HANDLE result = INVALID_HANDLE_VALUE;
	DWORD flags = DUPLICATE_SAME_ACCESS;
	if (!DuplicateHandle(proc, handle, GetCurrentProcess(), &result, 0, false, flags))
	{
		fprintf(stderr, "handle_to_private: Error %ld\n",
		GetLastError());
		shmassert(false);
	}
	CloseHandle(proc);
	return result;
}

bool
shm_handle_get_handle(ShmAbstractHandle *event, ShmEventHandle *handle)
{
	if (GetCurrentProcessId() == superblock->coordinator_process)
	{
		if (event->main_handle_valid)
		{
			*handle = event->main_handle;
			return true;
		}
	}
	else
	{
		if (private_events && event->main_handle_valid)
		{
			*handle = private_events->events[event->handle_id].handle;
			return *handle != INVALID_HANDLE_VALUE;
		}
	}
	return false;
}

bool
shm_handle_check_private(ShmAbstractHandle *event, ShmProcessID coordinator_process)
{
	shmassert(event->main_handle_valid && event->main_handle != INVALID_HANDLE_VALUE);
	if (private_events->events[event->handle_id].handle == INVALID_HANDLE_VALUE)
		private_events->events[event->handle_id].handle = handle_to_private(coordinator_process, event->main_handle);

	shmassert(private_events->events[event->handle_id].handle != INVALID_HANDLE_VALUE);
	return RESULT_OK;
}

void
shm_handle_init(ShmAbstractHandle *event, ShmEventHandle new_handle)
{
	event->handle_id = p_atomic_int_add(&superblock->last_available_event_id, 1);
	shmassert(event->handle_id >= 0);
	shmassert(superblock->last_available_event_id < MAX_EVENTS_COUNT);
	// coordinator process owns every shared kernel object
	if (ShmGetCurrentProcessId() != superblock->coordinator_process)
	{
		event->main_handle = handle_to_coordinator(superblock->coordinator_process, new_handle, false);
		shmassert(private_events->events[event->handle_id].handle == INVALID_HANDLE_VALUE);
		private_events->events[event->handle_id].handle = new_handle;
	}
	else
	{
		event->main_handle = new_handle;
	}
	event->main_handle_valid = true;
}

int
shm_event_init(ShmEvent *event)
{
	shmassert(private_events || GetCurrentProcessId() == superblock->coordinator_process);
	event->ready = 0;
	event->waiting = 0;
	ShmEventHandle h = CreateEvent(NULL, true, false, NULL);
	shm_handle_init((ShmAbstractHandle*)event, h);
	return RESULT_OK;
}

/**
 * @return RESULT_OK on success; RESULT_WAIT on timeout; RESULT_FAILURE otherwise.
 */
int
shm_event_wait(ShmEvent *event, ShmInt milliseconds, bool debug_break)
{
	bool timeout = false;
	take_spinlock(CAS2, &event->ready, 0, 1, {
		if ((milliseconds == 1 || milliseconds == 2) && backoff >= SPINLOCK_MAX_BACKOFF)
		{
			timeout = true;
			break;
		}
	});
	int rslt = RESULT_OK;
	if (timeout)
	{
		p_atomic_int_set(&event->waiting, 1);
		// last time check for those who set event->ready after we declared waiting, thus event might never be signalled.
		if (!CAS2(&event->ready, 0, 1))
		{
			shm_handle_check_private((ShmAbstractHandle*)event, superblock->coordinator_process);
			DWORD wait_status = WaitForSingleObject(private_events->events[event->handle_id].handle, debug_break ? 2000 : 2000);
			switch (wait_status)
			{
			case WAIT_OBJECT_0:
				rslt = RESULT_OK;
				break;
			case WAIT_ABANDONED:
				fprintf(stderr, "shm_event_wait: abandoned\n");
				rslt = RESULT_FAILURE;
				break;
			case WAIT_TIMEOUT:
				if (debug_break)
				{
					fprintf(stderr, "shm_event_wait 2 timeout\n");
					if (DEBUG_STRICT)
						DebugPauseAll();
				}
				else
					/* fprintf(stderr, "shm_event_wait timeout\n")*/ ;

				rslt = RESULT_WAIT;
				break;
			case WAIT_FAILED:
				fprintf(stderr, "shm_event_wait: error %ld\n", GetLastError());
				rslt = RESULT_FAILURE;
				break;
			}
		}
	}

	p_atomic_int_set(&event->waiting, 0);
	return rslt;
}

int
shm_event_sleep_wait(ShmEvent *event, puint milliseconds)
{
	while (!shm_event_ready(event))
		Sleep(milliseconds);
	shm_event_reset(event);
	return RESULT_OK;
}

/* Sender-listener permutations (6+5+4+3+2+1 = 21):
	thread1 thread2
	===============
			event->waiting != 0
			event->ready = 1
	waiting = true
	CAS(ready) - done
	waiting = false

	waiting = true
	CAS(ready) - fail
	waiting = false
		  won't signal, won't wait
	------------------ -
			event->waiting != 0
	waiting = true
			event->ready = 1
	CAS(ready)
	waiting = false

	waiting = true
	CAS(ready)
	waiting = false
		  won't signal, won't wait
	------------------ -
			event->waiting != 0
	waiting = true
	CAS(ready)
			event->ready = 1
	waiting = false

	waiting = true
	CAS(ready)
	waiting = false
		  won't signal, will WAIT1
	------------------ -
			event->waiting != 0
	waiting = true
	CAS(ready)
	waiting = false
			event->ready = 1

	waiting = true
	CAS(ready)
	waiting = false
		  won't signal, will WAIT2
	------------------ -
			event->waiting != 0
	waiting = true
	CAS(ready)
	waiting = false

	waiting = true
			event->ready = 1
	CAS(ready)
	waiting = false
		  won't signal, won't wait
	------------------ -
			event->waiting != 0
	waiting = true
	CAS(ready)
	waiting = false

	waiting = true
	CAS(ready)
			event->ready = 1
	waiting = false
		 won't signal, will WAIT2
	==========
	waiting = true
		event->waiting != 0
		event->ready = 1
	CAS(ready)
	waiting = false

	waiting = true
	CAS(ready)
	waiting = false
		  will SIGNAL, won't wait
	------
	waiting = true
			event->waiting != 0
	CAS(ready)
			event->ready = 1
	waiting = false

	waiting = true
	CAS(ready)
	waiting = false
		  will SIGNAL, will WAIT1
	------
	waiting = true
			event->waiting != 0
	CAS(ready)
	waiting = false
			event->ready = 1

	waiting = true
	CAS(ready)
	waiting = false
		will SIGNAL, will WAIT
	------
	waiting = true
			event->waiting != 0
	CAS(ready)
	waiting = false

	waiting = true
			event->ready = 1
	CAS(ready)
	waiting = false
		will SIGNAL, will WAIT2
	------
	waiting = true
			event->waiting != 0
	CAS(ready)
	waiting = false

	waiting = true
	CAS(ready)
			event->ready = 1
	waiting = false
		will SIGNAL, will WAIT2
	==========
	waiting = true
	CAS(ready)
			event->waiting != 0
			event->ready = 1
	waiting = false

	waiting = true
	CAS(ready)
	waiting = false
		will SIGNAL, will WAIT1
	------ -
	waiting = true
	CAS(ready)
			event->waiting != 0
	waiting = false
			event->ready = 1

	waiting = true
	CAS(ready)
	waiting = false
		will SIGNAL, will WAIT2
	------ -
	waiting = true
	CAS(ready)
			event->waiting != 0
	waiting = false

	waiting = true
			event->ready = 1
	CAS(ready)
	waiting = false
		will SIGNAL, will WAIT2
	------ -
	waiting = true
	CAS(ready)
			event->waiting != 0
	waiting = false

	waiting = true
	CAS(ready)
			event->ready = 1
	waiting = false
		will SIGNAL, will WAIT2
	==========
	waiting = true
	CAS(ready)
	waiting = false
			event->waiting != 0
			event->ready = 1

	waiting = true
	CAS(ready)
	waiting = false
		won't signal, will WAIT2
	------ -
	waiting = true
	CAS(ready)
	waiting = false
			event->waiting != 0

	waiting = true
			event->ready = 1
	CAS(ready)
	waiting = false
		won't signal, will WAIT2
	------ -
	waiting = true
	CAS(ready)
	waiting = false
			event->waiting != 0

	waiting = true
	CAS(ready)
			event->ready = 1
	waiting = false
		won't signal, will WAIT2
	==========
	waiting = true
	CAS(ready)
	waiting = false

	waiting = true
			event->waiting != 0
			event->ready = 1
	CAS(ready)
	waiting = false
		will SIGNAL, will WAIT2
	----
	waiting = true
	CAS(ready)
	waiting = false

	waiting = true
			event->waiting != 0
	CAS(ready)
			event->ready = 1
	waiting = false
		will SIGNAL, will WAIT2
	==========
	waiting = true
	CAS(ready)
	waiting = false

	waiting = true
	CAS(ready)
			event->waiting != 0
			event->ready = 1
	waiting = false
		will SIGNAL, will WAIT2
*/

// There's still like 0.000000001% chance of getting a race condition here
int
shm_event_signal(ShmEvent *event)
{
	// must be checked in reverse order compared to shm_event_wait, so the listener won't suddenly start waiting while the event is not yet ready.
	bool waiting = p_atomic_int_get(&event->waiting) != 0;
	p_atomic_int_set(&event->ready, 1);
	ShmEventHandle handle;
	ShmInt newval = 0;
	if (waiting) newval |= 1;
	if (p_atomic_int_get(&event->waiting)) newval |= 2;
	if (shm_handle_get_handle((ShmAbstractHandle*)event, &handle))
		newval |= 4;
	// second check in case consumer run a full circle of "waiting = false; ... waiting = true; CAS(ready);..."
	if (waiting || p_atomic_int_get(&event->waiting))
	{
		shm_handle_check_private((ShmAbstractHandle*)event, superblock->coordinator_process);
		bool rslt = shm_handle_get_handle((ShmAbstractHandle*)event, &handle);
		shmassert(rslt);
		newval |= 8;
		SetEvent(handle);
	}
	event->last_set = newval;

	return RESULT_OK;
}

int
shm_event_reset(ShmEvent *event)
{
	// same as shm_event_set
	bool waiting = p_atomic_int_get(&event->waiting) != 0;
	if (p_atomic_int_get(&event->ready) != 0)
		p_atomic_int_set(&event->ready, 0);
	ShmEventHandle handle;
	// do we actually need the "waiting" check for reset?
	if (waiting && shm_handle_get_handle((ShmAbstractHandle*)event, &handle))
		ResetEvent(handle);
	return RESULT_OK;
}

bool
shm_event_ready(ShmEvent *event)
{
	return p_atomic_int_get(&event->ready) != 0;
}


/* ShmSimpleLock */

int
shm_simple_lock_init(ShmSimpleLock *lock)
{
	shmassert(private_events || GetCurrentProcessId() == superblock->coordinator_process);
	lock->lock_count = 0;
	// Both mutex and semaphore seem to be a bad choice in here because we don't have a well-defined acquire-release block for it (just for the kernel object, not for the whole lock)
	// ShmEventHandle h = CreateMutex(NULL, false, NULL);
	ShmEventHandle h = CreateEvent(NULL, false, false, NULL);
	shm_handle_init((ShmAbstractHandle*)lock, h);
	lock->owner = 0;
	lock->threads_inside = 0;
	lock->contention_count = 0;
	lock->contention_duration = 0;
	return RESULT_OK;
}

bool
shm_lock_tryacquire(ShmSimpleLock *lock)
{
	bool rslt = p_atomic_int_compare_and_exchange(&lock->lock_count, 0, 1);
	if (rslt)
	{
		shmassert(lock->lock_count > 0);
		p_atomic_int_set(&lock->owner, GetCurrentThreadId());
		bool correct = p_atomic_int_add(&lock->threads_inside, 1) == 0;
		shmassert(correct);
	}
	else
		p_atomic_int_add(&lock->contention_count, 1);

	return rslt;
}

// https://github.com/mirror/reactos/blob/master/reactos/lib/rtl/critical.c
// Lock is taken only when RESULT_OK returned.
// Should never return RESULT_INVALID.
// Recursion is an error because you should never hold two simple locks at the same time.
int
shm_lock_acquire_with_cb(ShmSimpleLock *lock, ShmSimpleLockCallback callback, void *callback_data)
{
	int rslt = RESULT_INVALID;
	bool timeout = false;
	if (!p_atomic_int_compare_and_exchange(&lock->lock_count, 0, 1))
	{
		p_atomic_int_add(&lock->contention_count, 1);
		uint64_t started = __rdtsc();
		if (DEBUG_LOCK == 0)
		{
			take_spinlock(CAS2, &lock->lock_count, 1, 0, {
				if (backoff >= SPINLOCK_MAX_BACKOFF / 8)
				{
					timeout = true;
					break;
				}
				if (callback)
				{
					int rslt = callback(callback_data);
					if (rslt != RESULT_INVALID)
					{
						uint64_t diff = __rdtsc() - started;
						InterlockedAdd(&lock->contention_duration, diff & 0xFFFFFFFF);
						InterlockedAdd(&lock->contention_duration_high, diff >> 32);
						return rslt;
					}
				}
			});
		}
		if (DEBUG_LOCK != 0)
			timeout = true; // "always contented" scenario
		// lock_count is used as both "waiters_count" and "locked"
		if ((timeout && p_atomic_int_add(&lock->lock_count, 1) != 0) || DEBUG_LOCK != 0)
		{
			do {
				// fprintf(stderr, "shm_lock_acquire waiting mutex\n");

				shm_handle_check_private((ShmAbstractHandle*)lock, superblock->coordinator_process);
				shmassert(lock->owner != GetCurrentThreadId()); // no recursions allowed
				DWORD wait_status = WaitForSingleObject(private_events->events[lock->handle_id].handle, 200);
				switch (wait_status)
				{
				case WAIT_OBJECT_0:
					rslt = RESULT_OK; // lock_count was already incremented, so we effectively have "locked = true"
					break;
				case WAIT_ABANDONED:
					fprintf(stderr, "shm_lock_acquire: abandoned?\n");
					rslt = RESULT_FAILURE;
					break;
				case WAIT_TIMEOUT:
					// fprintf(stderr, "shm_lock_acquire timeout\n");
					continue;
					break;
				case WAIT_FAILED:
					fprintf(stderr, "shm_lock_acquire: error %ld\n", GetLastError());
					rslt = RESULT_FAILURE;
					break;
				}
				if (rslt == RESULT_INVALID && callback)
					rslt = callback(callback_data);
			} while (rslt == RESULT_INVALID);

			if (rslt != RESULT_OK)
				p_atomic_int_dec_and_test(&lock->lock_count); // remove this thread from waiters_count
		}
		else
		{
			rslt = RESULT_OK;
		}
		uint64_t diff = __rdtsc() - started;
		InterlockedAdd(&lock->contention_duration, diff & 0xFFFFFFFF);
		InterlockedAdd(&lock->contention_duration_high, diff >> 32);
	}
	else
		rslt = RESULT_OK;

	shmassert(lock->lock_count > 0);
	if (rslt == RESULT_OK)
	{
		p_atomic_int_set(&lock->owner, GetCurrentThreadId());
		bool correct = p_atomic_int_add(&lock->threads_inside, 1) == 0;
		shmassert(correct);
	}
	return rslt;
}

int
shm_lock_acquire(ShmSimpleLock *lock)
{
	return shm_lock_acquire_with_cb(lock, NULL, 0);
}

bool shm_lock_owned(ShmSimpleLock *lock)
{
	int rslt1 = p_atomic_int_get(&lock->owner) == GetCurrentThreadId();
	int rslt2 = p_atomic_int_get(&lock->lock_count) > 0;

	return rslt1 && rslt2;
}

int
shm_lock_release(ShmSimpleLock *lock, int linenum)
{
	shmassert(lock->lock_count > 0);
	shmassert(p_atomic_int_get(&lock->owner) == GetCurrentThreadId());
	p_atomic_int_set(&lock->owner, 0); // without this shm_lock_owned will return true
	bool correct = p_atomic_int_dec_and_test(&lock->threads_inside);
	shmassert(correct);
	lock->release_line = linenum;
	bool has_waiters = ! p_atomic_int_dec_and_test(&lock->lock_count);
	// Unfortunately we have to release the mutex after we have released the main lock.
	// Otherwise waiter might resort to waiting a second time while we are still in the ReleaseMutex syscall.
	// Thus mutex_owned shall be mutex-protected, but not "lock->locked" protected.
	if (has_waiters)
	{
		ShmEventHandle event = INVALID_HANDLE_VALUE;
		shm_handle_check_private((ShmAbstractHandle*)lock, superblock->coordinator_process);
		bool succ = shm_handle_get_handle((ShmAbstractHandle*)lock, &event);
		shmassert(succ);
		// If someone incremented the lock_count/waiters_count then he has to always wait,
		// otherwise the event will be left hanging in the set state, causing access by two threads on next contention.
		SetEvent(event);
	}
	return RESULT_OK;
}

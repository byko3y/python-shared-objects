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

#include <sys/syscall.h>
#include <linux/futex.h>
#include <stdint.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>

void
init_private_events(void)
{
}

/**
 * @return TRUE on successfull exchange.
 */
bool
futex_compare_and_exchange(vl uint32_t *value, uint32_t old_value, uint32_t new_value)
{
	return p_atomic_int_compare_and_exchange((pint *)(intptr_t)value, (pint)old_value, (pint)new_value);
}

uint32_t
futex_exchange(vl uint32_t *value, uint32_t new_value)
{
	return (uint32_t)p_atomic_int_exchange((pint *)(intptr_t)value, (pint)new_value);
}

uint32_t
futex_get(vl uint32_t *value)
{
	return (uint32_t)p_atomic_int_get((pint *)(intptr_t)value);
}

void
futex_set(vl uint32_t *value, uint32_t new_value)
{
	p_atomic_int_set((pint *)(intptr_t)value, (pint)new_value);
}

/**
 * @return TRUE if the new value is equal to zero, FALSE otherwise.
 */
bool
futex_dec_and_test(vl uint32_t *value)
{
	return p_atomic_int_dec_and_test((pint *)(intptr_t)value);
}

struct timespec
timeout_in_ms(int value)
{
	shmassert(value >= 0 && value < 1000000);
	struct timespec timeout = {. tv_sec = value / 1000, .tv_nsec=(value % 1000)*1000*1000 };
	return timeout;
}

/* ShmSimpleLock */
int
shm_simple_lock_init(ShmSimpleLock *lock)
{
	lock->lock_state = 0;
	lock->owner = 0;
	// memset(CAST_VL(&lock->padding), 0x55, SHM_SIMPLE_LOCK_PADDING);
	lock->threads_inside = 0;
	lock->contention_count = 0;
	lock->wait_count = 0;
	lock->wake_count = 0;
	lock->contention_duration = 0;
	lock->contention_duration_high = 0;
	return RESULT_OK;
}

bool
shm_lock_tryacquire(ShmSimpleLock *lock)
{
	bool rslt = futex_compare_and_exchange(&lock->lock_state, 0, 1);
	if (rslt)
	{
		shmassert(lock->lock_state != 0);
		p_atomic_int_set(&lock->owner, ShmGetCurrentThreadId());
		bool correct = p_atomic_int_add(&lock->threads_inside, 1) == 0;
		shmassert(correct);
	}
	else
		p_atomic_int_add(&lock->contention_count, 1);

	return rslt;
}

// https://eli.thegreenplace.net/2018/basics-of-futexes/
// https://dept-info.labri.fr/~denis/Enseignement/2008-IR/Articles/01-futex.pdf
int
shm_lock_acquire_with_cb(ShmSimpleLock *lock, ShmSimpleLockCallback callback, void *callback_data)
{
	ShmInt state = futex_get(&lock->lock_state);
	shmassert(state == 0 || state == 1 || state == 2);
	bool fast_path = futex_compare_and_exchange(&lock->lock_state, 0, 1);
	if (!fast_path)
	{
		p_atomic_int_add(&lock->contention_count, 1);
		uint32_t old_state = futex_exchange(&lock->lock_state, 2);
		while (old_state != 0)
		{
			// struct timespec timeout = timeout_in_ms(200);
			struct timespec timeout = timeout_in_ms(2000);
			uint64_t started = rdtsc();
			long rslt = syscall(SYS_futex, &lock->lock_state, FUTEX_WAIT, 2, &timeout, 0, 0);
			p_atomic_int_inc(&lock->wait_count);

			uint64_t diff = rdtsc() - started;
			uint32_t lower = (diff & 0xFFFFFFFF);
			uint32_t higher = (diff >> 32);
			uint32_t prev = (uint32_t)p_atomic_int_add((volatile pint*)&lock->contention_duration, (pint)lower);
			if (lower > UINT_MAX - prev)
				higher++;
			p_atomic_int_add((volatile pint*)&lock->contention_duration_high, (pint)higher);

			if (rslt != 0)
			{
				shmassert(ETIMEDOUT == errno || EAGAIN == errno || EINTR == errno);
				if (ETIMEDOUT == errno)
					return RESULT_WAIT;
				else
					return RESULT_REPEAT;
			}
			if (callback)
			{
				int call_r = callback(callback_data);
				if (RESULT_INVALID != call_r)
					return call_r;
			}
			old_state = futex_exchange(&lock->lock_state, 2);
		}
	}
	// Success
	shmassert(lock->lock_state != 0);
	p_atomic_int_set(&lock->owner, ShmGetCurrentThreadId());
	p_atomic_int_set(&lock->last_owner, ShmGetCurrentThreadId());
	bool correct = p_atomic_int_add(&lock->threads_inside, 1) == 0;
	shmassert(correct);

	return RESULT_OK;
}

// May not acquire lock
int
shm_lock_acquire_maybe(ShmSimpleLock *lock)
{
	return shm_lock_acquire_with_cb(lock, NULL, 0);
}

int
shm_lock_acquire(ShmSimpleLock *lock)
{
	int rslt = RESULT_INVALID;
	do
	{
		rslt = shm_lock_acquire_with_cb(lock, NULL, 0);
		shmassert(RESULT_OK == rslt || RESULT_REPEAT == rslt);
		if (RESULT_FAILURE == rslt)
			return RESULT_FAILURE;
	} while (RESULT_OK != rslt);
	return RESULT_OK;
}

bool
shm_lock_owned(ShmSimpleLock *lock)
{
	int rslt1 = p_atomic_int_get(&lock->owner) == ShmGetCurrentThreadId();
	int rslt2 = futex_get(&lock->lock_state) > 0;
	return rslt1 && rslt2;
}

int
shm_lock_release(ShmSimpleLock *lock, int linenum)
{
	ShmInt state = futex_get(&lock->lock_state);
	shmassert(state == 1 || state == 2);

	shmassert(p_atomic_int_get(&lock->owner) == ShmGetCurrentThreadId());
	p_atomic_int_set(&lock->owner, 0); // without this shm_lock_owned will return true
	bool correct = p_atomic_int_dec_and_test(&lock->threads_inside);
	shmassert(correct);
	lock->release_line = linenum;

	if (futex_dec_and_test(&lock->lock_state) == false)
	{
		futex_set(&lock->lock_state, 0);
		syscall(SYS_futex, &lock->lock_state, FUTEX_WAKE, 1, 0, 0, 0);
		p_atomic_int_inc(&lock->wake_count);
	}
	return RESULT_OK;
}


/* ShmEvent */
int
shm_event_init(ShmEvent *event)
{
	event->ready = 0;
	return RESULT_OK;
}

/**
 * @return RESULT_OK on success; RESULT_WAIT on timeout; RESULT_FAILURE otherwise.
 */
int
shm_event_wait(ShmEvent *event, ShmInt milliseconds, bool debug_break)
{
	while (1)
	{
		if (futex_get(&event->ready) != 0)
		{
			shm_event_reset(event);
			return RESULT_OK;
		}
		struct timespec timeout = timeout_in_ms(4000);
		long rslt = syscall(SYS_futex, &event->ready, FUTEX_WAIT, 0, &timeout, 0, 0);
		if (rslt != 0) {
			if (ETIMEDOUT == errno)
			{
				if (debug_break)
					shmassert_msg(false, "Futex timed out");
				return RESULT_WAIT;
			}
			if (EAGAIN != errno && EINTR != errno)
				shmassert(false);
		}
	}
}

int
shm_event_sleep_wait(ShmEvent *event, puint milliseconds)
{
	while (futex_get(&event->ready) == 0)
		Sleep(milliseconds);
	shm_event_reset(event);
	return RESULT_OK;
}

int
shm_event_reset(ShmEvent *event)
{
	futex_set(&event->ready, 0);
	return RESULT_OK;
}

int
shm_event_signal(ShmEvent *event)
{
	if (shm_event_ready(event))
		return RESULT_OK;
	if (futex_compare_and_exchange(&event->ready, 0, 1))
	{
		syscall(SYS_futex, &event->ready, FUTEX_WAKE, 1, 0, 0, 0);
	}
	// else shm_event_reset() haven't been called yet and the event will be processed.
	// Do not call shm_event_reset() after processing/before waiting.
	// I.e. the regular scenario is:
	// Thread 1 (event producer): "something" happenned, shm_event_signal().
	// Thread 2 (event consumer): shm_event_wait(){ shm_event_reset }, process "something", shm_event_wait(){ FUTEX_WAIT }.
	return RESULT_OK;
}

bool
shm_event_ready(ShmEvent *event)
{
	return futex_get(&event->ready) != 0;
}

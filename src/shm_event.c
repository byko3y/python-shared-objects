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

// ShmPromise

ShmPromise *
new_shm_promise(ThreadContext *thread, ShmPointer *out_pntr)
{
	*out_pntr = EMPTY_SHM;
	ShmPromise *new_event = get_mem(thread, out_pntr, sizeof(ShmPromise), SHM_PROMISE_DEBUG_ID);
	init_container((ShmContainer *)new_event, sizeof(ShmPromise), SHM_TYPE_PROMISE);
	new_event->state = PROMISE_STATE_PENDING;
	new_event->new_state = PROMISE_STATE_INVALID;
	new_event->value = EMPTY_SHM;
	new_event->new_value = EMPTY_SHM;
	new_event->waiters = 0;
	new_event->finalized = false;
	return new_event;
}

int
shm_promise_wait_finish(ThreadContext *thread, ShmPromise *promise)
{
	atomic_bitmap_reset(&promise->waiters, thread->index);
	return RESULT_OK;
}

// for some reason windows version returns RESULT_REPEAT a lot
int
shm_promise_wait(ThreadContext *thread, ShmPromise *promise)
{
	if (thread->transaction_mode >= TRANSACTION_TRANSIENT)
	{
		shmassert_msg(false, "Cannot wait inside transaction");
	}
	if (p_atomic_int_get(&promise->finalized) == true)
		return RESULT_ABORT;

	if (p_atomic_int_get(&promise->state) > PROMISE_STATE_PENDING)
	{
		shm_promise_wait_finish(thread, promise);
		return RESULT_OK;
	}

	shm_event_reset(&thread->ready);
	// shall we reset thread->ready only after we've checked it for every alternative signal source?

	atomic_bitmap_set(&promise->waiters, thread->index);
	if (p_atomic_int_get(&promise->state) > PROMISE_STATE_PENDING)
	{
		shm_promise_wait_finish(thread, promise);
		return RESULT_OK;
	}
	if (p_atomic_int_get(&promise->finalized) == true)
	{
		shm_promise_wait_finish(thread, promise);
		return RESULT_ABORT;
	}
	// otherwise the event wasn't signalled nor finalized before we registerd ourselves as a waiter.
	shm_event_wait(&thread->ready, 1, DEBUG_SHM_EVENTS);
	// shm_event_wait(&thread->ready, 1, false);
	shm_promise_wait_finish(thread, promise);
	if (p_atomic_int_get(&promise->state) > PROMISE_STATE_PENDING)
		return RESULT_OK;
	else
		return RESULT_REPEAT;
}

int
shm_promise_get_state(ThreadContext *thread, ShmPromise *promise, ShmInt *state, bool owned)
{
	if (p_atomic_int_get(&promise->finalized) == true)
		return RESULT_ABORT;

	if (owned)
	{
		ShmInt new_state = p_atomic_int_get(&promise->new_state);
		if (new_state != PROMISE_STATE_INVALID)
		{
			*state = new_state;
			return RESULT_OK;
		}
	}
	*state = p_atomic_int_get(&promise->state);
	return RESULT_OK;
}

void
shm_promise_send_signal(ThreadContext *thread, ShmPromise *promise)
{
	for (int cycle = 0; cycle <= 3; cycle++)
	{
		shmassert(cycle < 3);
		ShmReaderBitmap waiters = atomic_bitmap_get(&promise->waiters);
		if (waiters == 0)
			break;
		for (int i = 0; i < 64; ++i)
		{
			if (atomic_bitmap_reset(&promise->waiters, i))
			{
				ShmPointer target_shm = superblock->threads.threads[i];
				signal_thread(thread, target_shm);
			}
		}
	}
}

// consumes the "value"
int
shm_promise_signal(ThreadContext *thread, ShmPromise *promise, ShmPointer promise_shm, ShmInt new_state, ShmPointer value)
{
	shmassert(new_state != PROMISE_STATE_PENDING);
	ShmInt old_state = p_atomic_int_get(&promise->new_state);
	if (old_state == PROMISE_STATE_INVALID)
		old_state = p_atomic_int_get(&promise->state);
	if (old_state == PROMISE_STATE_REJECTED || old_state == PROMISE_STATE_FULFILLED)
	{
		shm_pointer_release(thread, value);
		return RESULT_REPEAT; // might as well be treated as failure
	}

	/* if (thread->transaction_mode == TRANSACTION_TRANSIENT)
	{
		// waiting for the lock, but not taking it.
		while (1) // should really rewrite this with RESULT_REPEAT later
		{
			ShmPointer writer_lock = p_atomic_shm_pointer_get(&promise->lock.writer_lock);
			if (SBOOL(writer_lock) == FALSE || writer_lock == thread->self)
				break;
		};
	} */

	if_failure(
		transaction_lock_write(thread, &promise->lock, promise_shm, CONTAINER_PROMISE, NULL),
		{
			shm_pointer_release(thread, value);
			transient_abort(thread);
			return status;
		}
	);
	/* if (old_number != p_atomic_int_get(&promise->number))
	{
		transient_abort(thread);
		return RESULT_REPEAT;
	} */
	ShmInt current_state = PROMISE_STATE_INVALID;
	int rslt = shm_promise_get_state(thread, promise, &current_state, true);
	if (rslt != RESULT_OK)
	{
		shm_pointer_release(thread, value);
		transient_abort(thread);
		return rslt;
	}
	if (current_state == PROMISE_STATE_REJECTED || current_state == PROMISE_STATE_FULFILLED)
	{
		shm_pointer_release(thread, value);
		transient_abort(thread);
		return RESULT_REPEAT; // might as well be treated as failure
	}

	shm_pointer_move(thread, &promise->new_value, &value);
	promise->new_state = new_state;
	// if (thread->transaction_mode == TRANSACTION_TRANSIENT)
	//     shm_promise_commit(thread, promise);
	transient_commit(thread);

	return RESULT_OK;
}

int
shm_promise_commit(ThreadContext *thread, ShmPromise *promise)
{
	shm_cell_check_write_lock(thread, &promise->lock);

	if (promise->new_state != PROMISE_STATE_INVALID)
	{
		p_atomic_int_set(&promise->state, promise->new_state);
		promise->new_state = PROMISE_STATE_INVALID;
		shm_pointer_move_atomic(thread, &promise->value, &promise->new_value);
		// First set the flag, then signal the threads, so newcomers would see the flag after
		// they added themselves into waiters list and no threads would be added after the signalling loop.
		shm_promise_send_signal(thread, promise);
	}
	return RESULT_OK;
}

int
shm_promise_rollback(ThreadContext *thread, ShmPromise *promise)
{
	shm_cell_check_write_lock(thread, &promise->lock);

	p_atomic_int_set(&promise->new_state, PROMISE_STATE_INVALID);
	shm_pointer_empty_atomic(thread, &promise->value);
	return RESULT_OK;
}

int
shm_promise_unlock(ThreadContext *thread, ShmPromise *promise, ShmInt type)
{
	_shm_cell_unlock(thread, &promise->lock, type);
	return RESULT_OK;
}

int
shm_barrier_finalize(ThreadContext *thread, PromiseRef promise)
{
	p_atomic_int_set(&promise.local->finalized, true);
	return RESULT_OK;
}

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

#include "assert.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
// #include <windows.h>

#include "shm_types.h"
#include <time.h>
#ifndef P_CC_MSVC
	#include "safe_lib.h"
#endif

#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
	#include <windows.h>
#else
	#include <errno.h>
	static inline int fopen_s(FILE **file, const char *fname, const char *mode)
	{
		FILE *newfile = fopen(fname, mode);
		*file = newfile;
		if (newfile)
			return 0;
		else
		return errno;
	}
#endif

static char charset[26] = "abcdefghijklmnopqrstuvwxyz";

//bool check(int result)
//{
//	if (result != RESULT_OK && result != RESULT_ABORT)
//		assert(result == RESULT_OK);
//	return result == RESULT_OK;
//}

#define TEST_MM_COUNT 1024*100
static ShmPointer test_mm_data[TEST_MM_COUNT];
extern void dump_mm_debug(void);

void test_mm(ThreadContext *thread)
{
	for (int i = 1; i < 3; ++i)
	{
		for (int j = 0; j < TEST_MM_COUNT; ++j)
		{
			int cntr = j*i;
			if (cntr % 100 == 1)
			{
				int sizes[3] = {4000, 12000, 30000};
				if (cntr % 13 == 1123)
				{
					// for cache locality last released block will be the first allocated
					int size = sizes[j*i % 3];
					ShmPointer tmp;
					ShmAbstractBlock *med_tmp = get_mem(thread, &tmp, size, TEST_MM_DEBUG_ID);
					med_tmp->type = 0;
					_unallocate_mem(tmp, 2);

					ShmPointer tmp2;
					ShmAbstractBlock *med_tmp2 = get_mem(thread, &tmp2, size, TEST_MM_DEBUG_ID);
					med_tmp2->type = 0;
					shmassert(tmp == tmp2);
					shmassert(med_tmp == med_tmp2);
					_unallocate_mem(tmp2, 2);
				}

				int size = sizes[cntr % 3];
				if (cntr % 1000 == 1)
					size = size * 10;
				ShmAbstractBlock *med = get_mem(thread, &test_mm_data[j], size, TEST_MM_MEDIUM_DEBUG_ID);
				med->type = 0;
				shmassert(med);
			}
			else
			{
				int sizes[3] = {20, 40, 200};
				int size = sizes[cntr % 3];
				ShmAbstractBlock *data = get_mem(thread, &test_mm_data[j], size, TEST_MM_DEBUG_ID);
				data->type = 0;
				shmassert(data);
			}
		}
		for (int j = 0; j < TEST_MM_COUNT; ++j)
		{
			_unallocate_mem(test_mm_data[j], 2);
		}
		fprintf(stderr, "MM test iteration %d\n", i);
		dump_mm_debug();
	}
}


#define TEST_LOCK_THREAD_COUNT 3

typedef struct {
	ShmInt cycles_count;
	volatile ShmInt counter;
	volatile ShmInt owners_count;
	ShmSimpleLock *lock;
	uint32_t threads_sleep_time;
	uint32_t threads_sleep_time_high;
	pint sleep_inside_count;
} test_locks_data;


static void *
test_locks_thread(void *arg)
{
	test_locks_data *data = arg;
	ShmThreadID myid = ShmGetCurrentThreadId();
	SHM_UNUSED(myid);
	for (int i = 0; i < data->cycles_count; i++)
	{
		shmassert(shm_lock_owned(data->lock) == false);
		int rslt = RESULT_INVALID;
		do
		{
			rslt = shm_lock_acquire(data->lock);
			shmassert(RESULT_OK == rslt || RESULT_REPEAT == rslt);
		} while (RESULT_OK != rslt);

		uint64_t started = rdtsc();
		int val = p_atomic_int_get(&data->counter);

		// if (i % 97 == 0) { Sleep(0); p_atomic_int_inc(&data->sleep_inside_count); }
		int prev_value = p_atomic_int_add(&data->owners_count, 1);
		shmassert(prev_value == 0);

		// if (i % 79 == 0) { Sleep(0); p_atomic_int_inc(&data->sleep_inside_count); }
		p_atomic_int_set(&data->counter, val + 1);

		if (i % 93 == 0) { Sleep(0); p_atomic_int_inc(&data->sleep_inside_count); }
		shmassert(shm_lock_owned(data->lock));

		// if (i % 83 == 0) { Sleep(0); p_atomic_int_inc(&data->sleep_inside_count); }
		bool succ = p_atomic_int_dec_and_test(&data->owners_count);
		shmassert(succ);

		uint64_t diff = rdtsc() - started;
		uint32_t lower = (diff & 0xFFFFFFFF);
		uint32_t higher = (diff >> 32);
		uint32_t prev = (uint32_t)p_atomic_int_add((volatile pint*)&data->threads_sleep_time, (pint)lower);
		if (lower > UINT_MAX - prev)
			higher++;
		p_atomic_int_add((volatile pint*)&data->threads_sleep_time_high, (pint)higher);

		shm_lock_release(data->lock, __LINE__);
		if (i % 101 == 0) Sleep(0);
	}
	return NULL;
}

#ifndef P_OS_WIN
	#include <unistd.h>
	#include <pthread.h>
#endif

void
test_locks(ThreadContext *thread)
{
	SHM_UNUSED(thread);
#ifndef P_OS_WIN
	pthread_attr_t attr;
	int rslt;
	rslt = pthread_attr_init(&attr);
	shmassert_msg(rslt == 0, "pthread_attr_init");

	rslt = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	shmassert_msg(rslt == 0, "pthread_attr_setstacksize");

	pthread_t threads[TEST_LOCK_THREAD_COUNT];
	test_locks_data test_data = { 1*1000*1000, 0, 0, calloc(sizeof(ShmSimpleLock), 1), 0, 0, 0 };
	shm_simple_lock_init(test_data.lock);
	for (int i = 0; i < TEST_LOCK_THREAD_COUNT; i++) {
		rslt = pthread_create(&threads[i], &attr, test_locks_thread, &test_data);
		if (rslt != 0)
			threads[i] = 0;
	}

	rslt = pthread_attr_destroy(&attr);
	shmassert_msg(rslt == 0, "pthread_attr_destroy");

	test_locks_thread(&test_data);

	for (int i = 0; i < TEST_LOCK_THREAD_COUNT; i++)
		pthread_join(threads[i], NULL);

	shmassert(test_data.counter == (TEST_LOCK_THREAD_COUNT + 1) * test_data.cycles_count);
	printf("Lock stats after %d cycles in %d threads:\n  %d contentions (%d%%), %d waits, %d wakes\n",
	       test_data.counter, TEST_LOCK_THREAD_COUNT + 1,
	       test_data.lock->contention_count, test_data.lock->contention_count / (test_data.counter / 100),
	       test_data.lock->wait_count, test_data.lock->wake_count);
#endif
}

#define TRIES_TO_SLEEP 7

// This macro is useless for perofrimng multiple write operations
// within transaction because it will do writes multiple times oin RESULT_REPEAT.
// This mistake is hard to trigger because RESULT_REPEAT is a very rare result.
#define CHECK_RETRY(rslt, thread) { \
	int tmp_rslt = rslt; \
	if (tmp_rslt != RESULT_OK) { \
		shmassert(tmp_rslt != RESULT_FAILURE); \
		if (tmp_rslt != RESULT_REPEAT) \
		{ \
			abort_transaction_retaining(thread); \
			Sleep(0); /* would rather prefer exponential backoff */ \
		} \
		continue; \
	} \
}

// Need some quick solution for associative array verification:
// https://github.com/attractivechaos/klib
//    https://github.com/attractivechaos/klib/blob/master/khash.h

// https://stackoverflow.com/questions/4864453/associative-arrays-in-c
//    http://troydhanson.github.io/uthash/index.html
#include "uthash/uthash.h"

struct my_struct {
	char id[10];       /* we'll use this field as the key */
	char name[10];
	UT_hash_handle hh; /* makes this structure hashable */
};

static struct my_struct *dict_test_data = NULL;

void
verify_dict_data(ThreadContext *thread, DictLocalReference *dict, int *cnt, int *good)
{
	shmassert(thread->transaction_mode == TRANSACTION_NONE);
	struct my_struct *s = NULL;
	struct my_struct *tmp = NULL;
	*cnt = 0;
	*good = 0;
	bool started = false;
	HASH_ITER(hh, dict_test_data, s, tmp) {
		(*cnt)++;
		ShmDictKey key;
		shm_dict_init_key(&key, (char*)&s->id);
		ShmPointer value;
		ShmDictElement *element;
		int rslt = RESULT_INVALID;
		do {
			if (!started)
			{
				start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
				started = true;
			}
			rslt = shm_dict_get(thread, *(DictRef*)dict, &key, &value, &element);
			if (rslt != RESULT_OK)
				Sleep(0);
			if (result_is_abort(rslt))
				abort_transaction_retaining(thread);
		} while (result_is_abort(rslt) || result_is_repeat(rslt));
		shmassert(rslt == RESULT_OK);
		// shmassert(value != EMPTY_SHM, NULL);
		if (value != EMPTY_SHM)
			(*good)++;
		else
			//printf("missing %s\n", s->id);
			;
	}
	if (started)
		commit_transaction(thread, NULL);
	// printf("verify_dict_data: %d good out of %d\n", good, cnt);
	shmassert(thread->transaction_mode == TRANSACTION_NONE);
	shmassert(thread->private_data->write_locks_taken == 0);
}

void dict_add_entry(char *key)
{
	struct my_struct *s = NULL;
	HASH_FIND_STR(dict_test_data, key, s);
	if (!s)
	{
		s = calloc(1, sizeof(*s));
		strcpy_s(s->id, 10, key);
		HASH_ADD_STR(dict_test_data, id, s);
	}
}

void dict_del_entry(char *key)
{
	struct my_struct *s = NULL;
	HASH_FIND_STR(dict_test_data, key, s);
	if (s)
		HASH_DEL(dict_test_data, s);
}

void
create_random_dict_key(ShmDictKey *key, uint64_t *counter_rand, uint64_t *counter_init_key2)
{
	char *buf = malloc(10);
	memset(buf, 0, 10);
	uint64_t tmp = rdtsc();
	for (int position = 0; position < 9; ++position)
		buf[position] = charset[rand() % 26];
	buf[9] = 0;
	if (counter_rand)
		*counter_rand += rdtsc() - tmp;
	tmp = rdtsc();
	//shm_dict_init_key(&key2, "second");
	shm_dict_init_key_consume(key, buf);
	buf = NULL;
	if (counter_rand)
		*counter_init_key2 += rdtsc() - tmp;
}

void
print_main_thread_counters(ThreadContext *thread)
{
	printf("1. Times:        waiting %5d, %5d,        waiting2 %5d, %5d,\n",
		thread->private_data->times_waiting, thread->private_data->tickets_waiting,
		thread->private_data->times_waiting2, thread->private_data->tickets_waiting2);
	printf("                repeated %5d, %5d,  times_aborted1 %5d, %5d,\n",
		thread->private_data->times_repeated, thread->private_data->tickets_repeated,
	thread->private_data->times_aborted1, thread->private_data->tickets_aborted1);
	printf("          times_aborted2 %5d, %5d,  times_aborted3 %5d, %5d,\n",
		thread->private_data->times_aborted2, thread->private_data->tickets_aborted2,
		thread->private_data->times_aborted3, thread->private_data->tickets_aborted3);
	printf("          times_aborted4 %5d, %5d,  times_aborted5 %5d, %5d,\n",
		thread->private_data->times_aborted4, thread->private_data->tickets_aborted4,
		thread->private_data->times_aborted5, thread->private_data->tickets_aborted5);
	printf("          times_aborted6 %5d, %5d,  times_aborted7 %5d, %5d,\n",
		thread->private_data->times_aborted6, thread->private_data->tickets_aborted6,
	thread->private_data->times_aborted7, thread->private_data->tickets_aborted7);
	printf("          times_aborted8 %5d, %5d,  times_aborted9 %5d, %5d.\n",
		thread->private_data->times_aborted8, thread->private_data->tickets_aborted8,
		thread->private_data->times_aborted9, thread->private_data->tickets_aborted9);
}

// we need to craete some kind of test with "always fail after N locks each transaction" for retry testing. Also some "fail after random lock count each transaction"
void
test_dict(ThreadContext *thread, DictLocalReference *dict, char prefix, int kiterations, bool first, int kreties, bool fin, QueueRef* queue, ListRef *list)
{
	shmassert(thread->transaction_mode == TRANSACTION_NONE);
	shmassert(thread->private_data->write_locks_taken == 0);
	clock_t begin = clock();
	uint64_t tmp = 0;
	uint64_t counter_start_transaction = 0;
	uint64_t counter_init_key = 0;
	uint64_t counter_rand = 0;
	uint64_t counter_init_key2 = 0;
	uint64_t counter_set_first = 0;
	uint64_t counter_set_second = 0;
	uint64_t counter_get1 = 0;
	uint64_t counter_clear_first = 0;
	uint64_t counter_get2 = 0;
	uint64_t counter_clear_second = 0;
	uint64_t counter_clear_second1 = 0;
	uint64_t counter_clear_second2 = 0;
	uint64_t counter_get3 = 0;
	DictProfiling first_set;
	DictProfiling first_clear;
	DictProfiling second_set;
	DictProfiling second_clear;
	OutputDebugStringA(NULL);
	memset(&first_set, 0, sizeof(DictProfiling));
	memset(&first_clear, 0, sizeof(DictProfiling));
	memset(&second_set, 0, sizeof(DictProfiling));
	memset(&second_clear, 0, sizeof(DictProfiling));
	srand((unsigned)time(NULL));   // Initialization, should only be called once.
	const int iteration_count = kiterations * 1000;
	int iteration = 0;
	bool started = false;
	int total_retries = 0;
	int debug_retires = 0;
	int single_iteration_retries = 0;
	int single_iteration_retries_max = 0;
	int last_ticket = 0;
	int max_tickets_during_retry = 0;
	int commit_count = 0;
	bool retires_started = false;
	bool retires_completed = false;
	int start_dict_cnt = shm_dict_get_count(thread, *(DictRef*)dict);
	if (start_dict_cnt == -1) start_dict_cnt = 0;

	shm_thread_reset_debug_counters(thread);

	ShmDictKey key1;
	ShmDictKey key2;
	memset(&key1, 0, sizeof(key1));
	memset(&key2, 0, sizeof(key2));
	int start_recursion_count = 0;
	while (iteration < iteration_count)
	{
		bool key1_value_valid = true;
		bool key2_value_valid = true;
		shm_dict_free_key(&key1);
		shm_dict_free_key(&key2);

		if (single_iteration_retries > single_iteration_retries_max)
			single_iteration_retries_max = single_iteration_retries;
		if (single_iteration_retries_max > 400 && first)
		{
			// SuspendThread(second_process);
			// DebugBreak();
			// ResumeThread(second_process);
		}
		// if (!started && iteration % (iteration_count / 10) == (iteration_count / 10 - 1))
		// 	printf("Done %dk\n", kiterations);

		if (!retires_started && !retires_completed && iteration / 2 == iteration_count) // ????
		{
			retires_started = true;
			debug_retires = 0;
		}
		if (retires_started && debug_retires >= kreties * 1000)
		{
			retires_started = false;
			retires_completed = true;
		}

		tmp = rdtsc();
		if (!started)
		{
			started = true;
			single_iteration_retries = 0;
			// thread->private_data->times_aborted = 0;
			// thread->private_data->times_waiting = 0;
			// thread->private_data->times_repeated = 0;
			// thread->private_data->times_early_aborted = 0;
			last_ticket = p_atomic_int_get(&superblock->ticket);
			thread->private_data->last_known_ticket = p_atomic_int_get(&superblock->ticket);

			start_transaction(thread, TRANSACTION_PERSISTENT, LOCKING_WRITE, true, &start_recursion_count);
		}
		else
			continue_transaction(thread);

		if (queue)
		{
			// Queue's second lock.
			ShmPointer value_shm;
			char buf[15];
			int len = snprintf(buf, 15, "i%d", iteration);
			new_shm_unicode_value(thread, buf, len, &value_shm);
			CellRef cell;
			cell.local = NULL;
			cell.shared = EMPTY_SHM;
			int queue_rslt = shm_queue_append_consume(thread, *(QueueRef*)queue, value_shm, &cell);
			if (queue_rslt != RESULT_OK)
			{
				total_retries++;
				single_iteration_retries++;
				if (retires_started)
				{
					abort_transaction_retaining_debug_preempt(thread);
					debug_retires++;
				}
				else
					abort_transaction_retaining(thread);

				if (single_iteration_retries > TRIES_TO_SLEEP)
					Sleep(0);
				continue;
			}
		}

		counter_start_transaction += rdtsc() - tmp;
		// should we look at groff's hash?
		ShmDictKey key1;
		tmp = rdtsc();
		if (first)
			shm_dict_init_key(&key1, "first");
		else
			shm_dict_init_key(&key1, "second");
		counter_init_key += rdtsc() - tmp;
		//shm_dict_init_key(&key2, "second");
		create_random_dict_key(&key2, &counter_rand, &counter_init_key2);

		tmp = rdtsc();
		ShmDictElement *result_element;
		ShmPointer firstval = NONE_SHM;
		// fprintf(stderr, "set first %08x\n", key1.hash);
		// dict_cnt = shm_dict_get_count(thread, *(DictRef*)dict);
		// queue_cnt = transaction_length(thread);
		if (shm_dict_set_consume(thread, *(DictRef*)dict, &key1, firstval, &result_element, &first_set) != RESULT_OK)
		{
			total_retries++;
			single_iteration_retries++;
			if (retires_started)
			{
				abort_transaction_retaining_debug_preempt(thread);
				debug_retires++;
			}
			else
			{
				abort_transaction_retaining(thread);
				// ShmQueue is know to corrupt memory on rollbacks. Who cares?
				/*  _shmassert (condition=false,
						condition_msg=0x43f49c "((puint)ref_block->type & SHM_TYPE_RELEASE_MARK) == 0", message=0x0, file=0x43c228 "shm_types.c", line=6154) at shm_types.c:54
					#5  0x0042bdd6 in shm_pointer_to_refcounted (thread=0xb5d4b014,
						pointer=275847488) at shm_types.c:6154
					#6  0x0042bec6 in shm_pointer_release (thread=0xb5d4b014, pointer=275847488)
						at shm_types.c:6178
					#7  0x0042c259 in shm_next_pointer_empty (thread=0xb5d4b014,
						pointer=0xb1b528ec) at shm_types.c:6366
					#8  0x0042396f in shm_queue_rollback (thread=0xb5d4b014, queue=0xb1b528b0)
						at shm_types.c:3164
					#9  0x004207ba in transaction_end (thread=0xb5d4b014, rollback=true)
						at shm_types.c:1901
					#10 0x00421310 in abort_transaction_retaining (thread=0xb5d4b014)
						at shm_types.c:2163
					#11 0x00437830 in test_dict (thread=0xb5d4b014, dict=0xb5d4b0c8,
						prefix=49 '1', kiterations=5, first=true, kreties=0, fin=true,
						queue=0xb5d4b0ac, list=0xb5d4b0e4) at test.c:455
				 */
			}

			if (single_iteration_retries > TRIES_TO_SLEEP)
				Sleep(0);
			continue;
		}
		key1_value_valid = true;
		if (thread->transaction_mode == TRANSACTION_IDLE)
			dict_add_entry(key1.key);

		counter_set_first += rdtsc() - tmp;
		tmp = rdtsc();
		ShmPointer secondval = EMPTY_SHM;
		new_shm_bytes(thread, "second val", strlen("second val") + 1, &secondval);
		// dict_cnt = shm_dict_get_count(thread, *(DictRef*)dict);
		// queue_cnt = transaction_length(thread);
		if (shm_dict_set_consume(thread, *(DictRef*)dict, &key2, secondval, &result_element, &second_set) != RESULT_OK)
		{
			total_retries++;
			single_iteration_retries++;
			if (retires_started)
			{
				abort_transaction_retaining_debug_preempt(thread);
				debug_retires++;
			}
			else
				abort_transaction_retaining(thread);

			if (single_iteration_retries > TRIES_TO_SLEEP)
				Sleep(0);
			continue;
		}
		key2_value_valid = true;
		if (thread->transaction_mode == TRANSACTION_IDLE)
			dict_add_entry(key2.key);

		counter_set_second += rdtsc() - tmp;
		ShmPointer value1, value2;

		tmp = rdtsc();
		// fprintf(stderr, "get first %08x\n", key1.hash);
		CHECK_RETRY(shm_dict_get(thread, *(DictRef*)dict, &key1, &value1, &result_element), thread);
		// fprintf(stderr, "get second %08x\n", key2.hash);
		CHECK_RETRY(shm_dict_get(thread, *(DictRef*)dict, &key2, &value2, &result_element), thread);
		counter_get1 += rdtsc() - tmp;
		assert(value1 == firstval);
		assert(value2 == secondval);
		tmp = rdtsc();
		// dict_cnt = shm_dict_get_count(thread, *(DictRef*)dict);
		// queue_cnt = transaction_length(thread);
		if (iteration % 2 == 0)
		{
			if (shm_dict_set_empty(thread, *(DictRef*)dict, &key1, &first_clear) != RESULT_OK)
			{
				total_retries++;
				single_iteration_retries++;
				if (retires_started)
				{
					abort_transaction_retaining_debug_preempt(thread);
					debug_retires++;
				}
				else
					abort_transaction_retaining(thread);

				if (single_iteration_retries > TRIES_TO_SLEEP)
					Sleep(0);
				continue;
			}

			key1_value_valid = false;
			if (thread->transaction_mode == TRANSACTION_IDLE)
				dict_del_entry(key1.key);
		}
		counter_clear_first += rdtsc() - tmp;
		tmp = rdtsc();
		CHECK_RETRY(shm_dict_get(thread, *(DictRef*)dict, &key1, &value1, &result_element), thread);
		CHECK_RETRY(shm_dict_get(thread, *(DictRef*)dict, &key2, &value2, &result_element), thread);
		counter_get2 += rdtsc() - tmp;
		if (iteration % 2 == 0)
			assert(value1 == EMPTY_SHM || value1 == DEBUG_SHM);
		else
			assert(value1 == firstval);
		assert(value2 == secondval);
		tmp = rdtsc();
		// dict_cnt = shm_dict_get_count(thread, *(DictRef*)dict);
		// queue_cnt = transaction_length(thread);
		if (iteration % 2 == 1)
		{
			if (shm_dict_set_empty(thread, *(DictRef*)dict, &key2, &second_clear) != RESULT_OK)
			{
				total_retries++;
				single_iteration_retries++;
				if (retires_started)
				{
					abort_transaction_retaining_debug_preempt(thread);
					debug_retires++;
				}
				else
					abort_transaction_retaining(thread);

				if (single_iteration_retries > TRIES_TO_SLEEP)
					Sleep(0);
				continue;
			}

			key2_value_valid = false;
			if (thread->transaction_mode == TRANSACTION_IDLE)
				dict_del_entry(key2.key);
		}
		counter_clear_second += rdtsc() - tmp;
		if (iteration > iteration_count / 2)
			counter_clear_second1 += rdtsc() - tmp;
		else
			counter_clear_second2 += rdtsc() - tmp;
		tmp = rdtsc();
		CHECK_RETRY(shm_dict_get(thread, *(DictRef*)dict, &key1, &value1, &result_element), thread);
		CHECK_RETRY(shm_dict_get(thread, *(DictRef*)dict, &key2, &value2, &result_element), thread);
		counter_get3 += rdtsc() - tmp;
		if (iteration % 2 == 0)
			assert(value1 == EMPTY_SHM || value1 == DEBUG_SHM);
		else
			assert(value1 == firstval);
		if (iteration % 2 == 1)
			assert(value2 == EMPTY_SHM || value1 == DEBUG_SHM);
		else
			assert(value2 == secondval);

		if (list)
		{
			CHECK_RETRY(shm_list_append_consume(thread, *list, NONE_SHM, NULL), thread);
		}

		commit_count++;
		// dict_cnt = shm_dict_get_count(thread, *(DictRef*)dict);
		// shmassert((start_dict_cnt + iteration / 2 + (iteration % 2) + 1 == dict_cnt), NULL);
		if (thread->transaction_mode == TRANSACTION_PERSISTENT)
		{
			if (key1_value_valid)
				dict_add_entry(key1.key);
			else
				dict_del_entry(key1.key);

			if (key2_value_valid)
				dict_add_entry(key2.key);
			else
				dict_del_entry(key2.key);
		}
		int commit_recursion_count;
		commit_transaction(thread, &commit_recursion_count);
		// every iteration adds 2 and removes 1 element.
		// int dict_cnt_committed = shm_dict_get_count(thread, *(DictRef*)dict);
		// shmassert(dict_cnt == dict_cnt_committed, NULL);
		started = false;
		assert(start_recursion_count == commit_recursion_count);
		++iteration;

		int ticket_diff = p_atomic_int_get(&superblock->ticket) - last_ticket;
		if (ticket_diff > max_tickets_during_retry)
			max_tickets_during_retry = ticket_diff;
		if (max_tickets_during_retry > 10)
		{
			// SuspendThread(second_process);
			// DebugBreak();
			// ResumeThread(second_process);
		}
	}

	shm_dict_free_key(&key1);
	shm_dict_free_key(&key2);

	if (single_iteration_retries > single_iteration_retries_max)
		single_iteration_retries_max = single_iteration_retries;

	shmassert(thread->transaction_mode == TRANSACTION_NONE);

	clock_t end = clock();
	double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
	int cnt = shm_dict_get_count(thread, *(DictRef*)dict);
	if (first)
	{
		if (fin)
			Sleep(3000);
		if (fin)
		{
			int dict_local_good = 0;
			int dict_local_count = 0;
			verify_dict_data(thread, dict, &dict_local_count, &dict_local_good);
			printf("%c. Legacy dict verification: %d good out of %d\n", prefix, dict_local_good, dict_local_count);
		}

		printf("counter_start_transaction: %9.3f\n", counter_start_transaction / 3500000.0);
		printf("counter_init_key: %9.3f\n", counter_init_key / 3500000.0);
		printf("counter_rand: %9.3f\n", counter_rand / 3500000.0);
		printf("counter_init_key2: %9.3f\n", counter_init_key2 / 3500000.0);
		printf("counter_set_first: %9.3f\n", counter_set_first / 3500000.0);
		printf("counter_set_second: %9.3f\n", counter_set_second / 3500000.0);
		printf("counter_get1: %9.3f\n", counter_get1 / 3500000.0);
		printf("counter_clear_first (half): %9.3f\n", counter_clear_first / 3500000.0);
		printf("counter_get2: %9.3f\n", counter_get2 / 3500000.0);
		printf("counter_clear_second (half): %9.3f\n", counter_clear_second / 3500000.0);
		printf("counter_clear_second1: %9.3f\n", counter_clear_second1 / 3500000.0);
		printf("counter_clear_second2: %9.3f\n", counter_clear_second2 / 3500000.0);
		printf("counter_get3: %9.3f\n", counter_get3 / 3500000.0);
		printf("first_set: %9.3f %9.3f %9.3f %9.3f\n            %9.3f %9.3f %9.3f\n",
			first_set.counter1 / 3500000.0, first_set.counter2 / 3500000.0, first_set.counter3 / 3500000.0,
			first_set.counter4 / 3500000.0, first_set.counter5 / 3500000.0, first_set.counter6 / 3500000.0,
			first_set.counter7 / 3500000.0);
		printf("second_set: %9.3f %9.3f %9.3f %9.3f\n            %9.3f %9.3f %9.3f\n",
			second_set.counter1 / 3500000.0, second_set.counter2 / 3500000.0, second_set.counter3 / 3500000.0,
			second_set.counter4 / 3500000.0, second_set.counter5 / 3500000.0, second_set.counter6 / 3500000.0,
			second_set.counter7 / 3500000.0);
		printf("first_clear: %9.3f %9.3f %9.3f %9.3f\n            %9.3f %9.3f %9.3f\n",
			first_clear.counter1 / 3500000.0, first_clear.counter2 / 3500000.0, first_clear.counter3 / 3500000.0,
			first_clear.counter4 / 3500000.0, first_clear.counter5 / 3500000.0, first_clear.counter6 / 3500000.0,
			first_clear.counter7 / 3500000.0);
		printf("second_clear: %9.3f %9.3f %9.3f %9.3f\n            %9.3f %9.3f %9.3f\n",
			second_clear.counter1 / 3500000.0, second_clear.counter2 / 3500000.0, second_clear.counter3 / 3500000.0,
			second_clear.counter4 / 3500000.0, second_clear.counter5 / 3500000.0, second_clear.counter6 / 3500000.0,
			second_clear.counter7 / 3500000.0);
		printf("1. Max ticket: %i\n", superblock->ticket);
		printf("1. Max tickets passed when retrying: %d\n", max_tickets_during_retry);
		printf("1. Commit count: %i\n", commit_count);
		printf("1. Dict size: %i\n", cnt);
		printf("1. Retries: %d, max %d\n", total_retries, single_iteration_retries_max);
        printf("1. Total time %9.3f\n", time_spent);
        print_main_thread_counters(thread);
	}
	else
	{
		if (fin)
		{
			int dict_local_good = 0;
			int dict_local_count = 0;
			verify_dict_data(thread, dict, &dict_local_count, &dict_local_good);
			printf("%c. Legacy dict verification: %d good out of %d\n", prefix, dict_local_good, dict_local_count);
		}
		printf("2. Max tickets passed when retrying: %d\n", max_tickets_during_retry);
		printf("2. Commit count: %i\n", commit_count);
		printf("2. Retries: %d, max %d\n", total_retries, single_iteration_retries_max);
		printf("2. Total time %9.3f\n", time_spent);
		printf("2. Times:       waiting %5d, %5d,         waiting2 %5d, %5d,\n",
			thread->private_data->times_waiting, thread->private_data->tickets_waiting,
			thread->private_data->times_waiting2, thread->private_data->tickets_waiting2);
		printf("               repeated %5d, %5d,   times_aborted1 %5d, %5d,\n",
			thread->private_data->times_repeated, thread->private_data->tickets_repeated,
			thread->private_data->times_aborted1, thread->private_data->tickets_aborted1);
		printf("          times_aborted2 %5d, %5d,  times_aborted3 %5d, %5d,\n",
			thread->private_data->times_aborted2, thread->private_data->tickets_aborted2,
			thread->private_data->times_aborted3, thread->private_data->tickets_aborted3);
		printf("          times_aborted4 %5d, %5d,  times_aborted5 %5d, %5d,\n",
			thread->private_data->times_aborted4, thread->private_data->tickets_aborted4,
			thread->private_data->times_aborted5, thread->private_data->tickets_aborted5);
		printf("          times_aborted6 %5d, %5d,  times_aborted7 %5d, %5d,\n",
			thread->private_data->times_aborted6, thread->private_data->tickets_aborted6,
			thread->private_data->times_aborted7, thread->private_data->tickets_aborted7);
		printf("          times_aborted8 %5d, %5d,  times_aborted9 %5d, %5d.\n",
			thread->private_data->times_aborted8, thread->private_data->tickets_aborted8,
			thread->private_data->times_aborted9, thread->private_data->tickets_aborted9);
	}

	shmassert(thread->transaction_mode == TRANSACTION_NONE);
	shmassert(thread->private_data->write_locks_taken == 0);
}

void
print_list(ThreadContext *thread, ListRef *list)
{
	char filename[20];
	snprintf(filename, 20, "list%d.txt", ShmGetCurrentProcessId());
	FILE *pFile = NULL;
	fopen_s(&pFile, filename, "w");
	printf("Dumped list into %s\n", filename);
	ShmListCounts counts = shm_list_get_fast_count(thread, list->local, false);
	printf("    count = %d, deleted = %d\n", counts.count, counts.deleted);
	// little endian
	fputc(0xFF, pFile);
	fputc(0xFE, pFile);
	shm_list_print_to_file(pFile, list->local);
	fclose(pFile);
}

void
test_list_long_reads(ThreadContext *thread, ListRef *list, bool concurrent, int count_diff)
{
	start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
	ShmListCounts count = SHM_LIST_INVALID_COUNTS;
	for (int cycle = 0; cycle < 1000000; cycle++)
	{
		CHECK_RETRY(shm_list_get_count(thread, *list, &count, cycle == 0), thread);
		break;
	}
	if (!concurrent)
		shmassert(count.count == count_diff);
	commit_transaction(thread, NULL);
}

int
test_list(ThreadContext *thread, ListRef *list, int kiterations, bool concurrent)
{
	shmassert(thread->transaction_mode == TRANSACTION_NONE);
	shmassert(thread->private_data->write_locks_taken == 0);

	const int iteration_count = kiterations * 1000;
	ShmListCounts starting_count = SHM_LIST_INVALID_COUNTS;
	// const iteration_count = 1000;
	int iteration = 0;
	bool started = false;
	ShmInt last_ticket;
	SHM_UNUSED(last_ticket);
	int deleted_count = 0;
	// int single_iteration_retries = 0;
	while (iteration < iteration_count)
	{
		if (!started)
		{
			// single_iteration_retries = 0;
			// thread->private_data->times_aborted = 0;
			// thread->private_data->times_waiting = 0;
			// thread->private_data->times_repeated = 0;
			// thread->private_data->times_early_aborted = 0;
			last_ticket = p_atomic_int_get(&superblock->ticket);
			thread->private_data->last_known_ticket = p_atomic_int_get(&superblock->ticket);

			start_transaction(thread, TRANSACTION_PERSISTENT, LOCKING_WRITE, true, NULL);
			started = true;
		}
		else
			continue_transaction(thread);

		if (iteration == 0)
		{
			CHECK_RETRY(shm_list_get_count(thread, *list, &starting_count, false), thread);
			if (starting_count.count == -1)
				starting_count.count = 0;
		}

		ShmListCounts cnt = SHM_LIST_INVALID_COUNTS;
		CHECK_RETRY(shm_list_get_count(thread, *list, &cnt, (iteration % 128) == 1), thread);
		if (!concurrent)
			shmassert(cnt.count + deleted_count == iteration + starting_count.count);
		else
			shmassert(cnt.count + deleted_count >= iteration);

		ShmPointer value_shm = EMPTY_SHM;
		char buf[15];
		int len = snprintf(buf, 15, "i%d", iteration);
		new_shm_unicode_value(thread, buf, len, &value_shm);
		int list_rslt = shm_list_append_consume(thread, *list, value_shm, NULL);

		CHECK_RETRY(list_rslt, thread);
		shmassert(list_rslt == RESULT_OK);
		// shmassert(atomic_bitmap_check_exclusive(&list->local->base.lock.reader_lock, thread->index));
		shm_cell_check_write_lock(thread, &list->local->base.lock);

		ShmListCounts new_cnt = SHM_LIST_INVALID_COUNTS;
		CHECK_RETRY(shm_list_get_count(thread, *list, &new_cnt, (iteration % 128) == 2), thread);
		if (!concurrent)
		{
			shmassert(new_cnt.count + deleted_count == iteration + 1 + starting_count.count);
		}
		else
		{
			shmassert(new_cnt.count == cnt.count + 1);
			shmassert(new_cnt.count + deleted_count > iteration);
		}

		if (iteration % 11 == 10)
		{
			ShmPointer popped_value = EMPTY_SHM;
			bool valid  = false;
			CHECK_RETRY(shm_list_popleft(thread, *list, &popped_value, &valid), thread);
			shmassert(SBOOL(popped_value));
			shm_pointer_release(thread, popped_value);
			deleted_count++;
		}

		commit_transaction(thread, NULL);
		started = false;
		iteration++;
	}
	if (concurrent && is_coordinator())
	{
		printf("1. test_list profile:\n");
		print_main_thread_counters(thread);
	}
	test_list_long_reads(thread, list, concurrent, iteration_count - starting_count.count - deleted_count);

	shmassert(thread->transaction_mode == TRANSACTION_NONE);
	return deleted_count;
}

void
verify_list(ThreadContext *thread, ListRef *list, int kiterations, int deleted_count)
{
	start_transaction(thread, TRANSACTION_PERSISTENT, LOCKING_WRITE, true, NULL);
	ShmListCounts count = SHM_LIST_INVALID_COUNTS;
	while (true)
	{
		int rslt = shm_list_get_count(thread, *list, &count, true);
		CHECK_RETRY(rslt, thread);
		if (rslt == RESULT_OK)
			break;
	}
	shmassert(count.count != -1);
	shmassert(1000 * kiterations == count.count + deleted_count);
	shmassert(count.deleted == deleted_count);
	int iteration = 0;
	while (iteration < count.count)
	{
		ShmPointer got_value = EMPTY_SHM;
		CHECK_RETRY(shm_list_get_item(thread, *list, iteration, &got_value), thread);
		shmassert(SBOOL(got_value));
		RefUnicode str = shm_ref_unicode_get(got_value);
		char buf[15];
		int len = snprintf(buf, 15, "i%d", iteration + deleted_count);
		shmassert(len == str.len);
		for (int i = 0; i < str.len; i++)
			shmassert(str.data[i] == (Py_UCS4)buf[i]);

		iteration++;
	}
	commit_transaction(thread, NULL);
}

int verify_undict(ThreadContext *thread, UnDictRef undict, int count, char prefix);

int test_undict(ThreadContext *thread, UnDictRef undict, char prefix)
{
	shmassert(thread->transaction_mode == TRANSACTION_NONE);
	shmassert(thread->private_data->write_locks_taken == 0);

	bool transaction_started = false;
	ShmDictKey key;
	memclear(&key, sizeof(ShmDictKey));

	for (int iteration = 0; iteration < 1000;)
	{
		shm_dict_free_key(&key);
		if (!transaction_started)
		{
			start_transaction(thread, TRANSACTION_PERSISTENT, LOCKING_WRITE, true, NULL);
			transaction_started = true;
		}
		// create_random_dict_key(&key, NULL, NULL);
		// ShmUnDictKey key2 = { .hash = key.hash, .key = key.key, .keysize = key.keysize, .key_shm = EMPTY_SHM };

		ShmPointer value_shm;
		char buf[15];
		int len = snprintf(buf, 15, "%c.i%d", prefix, iteration);
		ShmValueHeader *header = new_shm_unicode_value(thread, buf, len, &value_shm);
		shmassert(shm_value_get_length(header) == len * isizeof(Py_UCS4));
		ShmUnDictKey key2 = EMPTY_SHM_UNDICT_KEY;
		key2.hash = hash_string_ascii(buf, len);
		key2.key4 = shm_value_get_data(header);
		key2.keysize = len;

		// value_data = NULL;
		int rslt = shm_undict_consume_item(thread, undict, &key2, value_shm, NULL);
		if (rslt != RESULT_OK)
		{
			abort_transaction_retaining(thread);
			continue; // for (iteration)
		}
		commit_transaction(thread, NULL);

		if (iteration % 90 == 1)
		{
			start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
			// int correct_count = (iteration + 1) - (iteration / 3 + 1);
			// int succeded = verify_undict(thread, undict, iteration + 1, prefix);
			// shmassert(succeded >= iteration - ((iteration - 1) / 3 + 1));
			// shmassert(succeded == correct_count);
			ShmPointer getval = EMPTY_SHM;
			int get_rslt = RESULT_INVALID;
			// It's a good idea to verify previous key too.
			while (get_rslt != RESULT_OK && get_rslt != RESULT_FAILURE)
				get_rslt = shm_undict_acq(thread, undict, &key2, &getval);
			shmassert(get_rslt != RESULT_FAILURE);
			if (getval == EMPTY_SHM)
			{
				// int succeded = verify_undict(thread, undict, 1000, prefix);
				shmassert(getval != EMPTY_SHM);
			}
			shmassert(getval != EMPTY_SHM);
			shm_pointer_release(thread, getval);
			int succeded2 = verify_undict(thread, undict, iteration + 1, prefix);
			shmassert(succeded2 >= iteration + 1 - (iteration / 3 + 1));
			commit_transaction(thread, NULL);
		}
		if (iteration % 3 == 0)
		{
			start_transaction(thread, TRANSACTION_PERSISTENT, LOCKING_WRITE, true, NULL);
			int rslt = RESULT_OK;
			while (true)
			{
				rslt = shm_undict_consume_item(thread, undict, &key2, EMPTY_SHM, NULL);
				shmassert(rslt != RESULT_FAILURE);
				if (rslt != RESULT_OK)
					abort_transaction_retaining(thread);
				else
					break; // while (true)
			}
			commit_transaction(thread, NULL);
		}

		transaction_started = false;
		++iteration;
	}
	shm_dict_free_key(&key);

	shm_undict_debug_print(thread, undict.local, false);
	if (transaction_started)
		commit_transaction(thread, NULL);

	shmassert(thread->transaction_mode == TRANSACTION_NONE);
	shmassert(thread->private_data->write_locks_taken == 0);

	return RESULT_OK;
}

int verify_undict(ThreadContext *thread, UnDictRef undict, int count, char prefix)
{
	start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
	int succeded = 0;
	for (int iteration = 0; iteration < count; ++iteration)
	{
		if (iteration % 3 == 0)
			continue;
		char buf[15];
		int keysize = snprintf(buf, 15, "%c.i%d", prefix, iteration);
		ShmUnDictKey key2 = EMPTY_SHM_UNDICT_KEY;
		key2.hash = hash_string_ascii(buf, keysize);
		key2.key1 = (Py_UCS1 *)buf;
		key2.keysize = keysize;
		key2.key_shm = EMPTY_SHM;

		ShmPointer value = EMPTY_SHM;
		int rslt;
		do {
			rslt = shm_undict_acq(thread, undict, &key2, &value);
			volatile char *bucket = NULL;
			if (rslt == RESULT_OK && value == EMPTY_SHM)
			{
				char filename[20];
				snprintf(filename, 20, "undict%d.txt", ShmGetCurrentProcessId());
				FILE *pFile = NULL;
				fopen_s(&pFile, filename, "w");
				print_items_to_file(pFile, undict.local);
				fclose(pFile);

				DebugBreak();

				char buf3[15];
				int someint = 108;
				int keysize3 = snprintf(buf3, 15, "%c.i%d", prefix, someint);
				ShmUnDictKey key3 = EMPTY_SHM_UNDICT_KEY;
				key3.hash = hash_string_ascii(buf3, keysize3);
				key3.key1 = (Py_UCS1*)buf3;
				key3.keysize = keysize3;
				key3.key_shm = EMPTY_SHM;

				ShmPointer value3 = EMPTY_SHM;
				rslt = shm_undict_acq(thread, undict, &key3, &value3);

				bucket = shm_undict_debug_scan_for_item(undict.local, &key2);
			}
			SHM_UNUSED(bucket);
			if (value != EMPTY_SHM)
				succeded += 1;
			shm_pointer_empty(thread, &value);
		} while (rslt != RESULT_OK && rslt != RESULT_FAILURE);
	}
	commit_transaction(thread, NULL);
	return succeded;
}

#ifdef P_OS_UNIX
	#include <spawn.h>
	// posix_spawn
	extern char **environ;
#endif

typedef struct process_handles_
{
	#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
		ShmProcessHandle pid; // hHandle
		ShmProcessHandle hProcess;
	#elif defined(P_OS_UNIX)
		pid_t pid;
	#endif
} process_handles;

bool
run_child_process(char *argv0, char id, int argcount, volatile unsigned int *args, process_handles *handles)
{
	#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
		ShmProcessHandle me = OpenThread(THREAD_SUSPEND_RESUME, 1, GetCurrentThreadId());
		char somebuf[MAX_PATH];
		memset(&somebuf[0], 0, sizeof somebuf);
		// snprintf(somebuf, sizeof somebuf, "\"%s\" %s %d %c %d %d %d %d %d %d %d",
		//     argv, &superblock_desc.id[0], me, id, dummy, barrier.shared, queue->shared, dict->shared,
		// 	list_2->shared, list_3->shared, undict.shared);
		int maxsize = sizeof somebuf;
		int len = snprintf(somebuf, maxsize, "\"%s\" %s %d %c", argv0, &superblock_desc.id[0], (int)me, id);
		shmassert(len < maxsize);
		char *pntr = somebuf + len;
		maxsize -= len;
		for (int idx = 0; idx < argcount; idx++)
		{
			int len2 = snprintf(pntr, maxsize, " %d", args[idx]);
			shmassert(len2 < maxsize);
			pntr += len2;
			maxsize -= len2;
		}

		STARTUPINFOA cif;
	ZeroMemory(&cif, sizeof(STARTUPINFOA));
		PROCESS_INFORMATION pi;
		ZeroMemory(&pi, sizeof(PROCESS_INFORMATION));
		bool rslt = CreateProcessA(argv0, somebuf,
			NULL, NULL, TRUE, 0, NULL, NULL, &cif, &pi);
		if (rslt && handles)
		{
			handles->pid = pi.hThread;
			handles->hProcess = pi.hProcess;
		}
		return rslt;
	#elif defined(P_OS_UNIX)
		ShmThreadID me = ShmGetCurrentThreadId();
		pid_t pid;
		handles->pid = 0;
		char *somearray[20] = {argv0, CAST_VL(&superblock_desc.id[0]), calloc(20, 1), calloc(1, 1), NULL};
		somearray[3][0] = id;
		snprintf(somearray[2], 20, "%d", me);
		for (int idx = 0; idx < argcount; idx++)
		{
			char *buf = calloc(20, 1);
			snprintf(buf, 20, "%d", args[idx]);
			somearray[4+idx] = buf;
		}
		somearray[4+argcount] = NULL;
		int error = posix_spawn(&pid, argv0, NULL, NULL, somearray, environ);
		bool rslt = error == 0;
		if (rslt && handles)
			handles->pid = pid;
		return rslt;
	#else
		return false;
#endif
}

int main(int argc, char *argv[])
{
	printf("Process started with %d\n", argc);
	if (argc > 1)
	{
		assert_prefix = "child init. ";
		if (argc != 14)
		{
			fprintf(stderr, "2. Dev use only, expected %d parameters, got %d", 14, argc);
			shmassert(false);
			return 1;
		}
		parent_process = (ShmProcessHandle)atoi(argv[2]);

		int rslt = init_superblock(argv[1]);
		char myid = *argv[3];
		if (myid == '2')
			assert_prefix = "p2. ";
		else
			assert_prefix = "p3. ";
		if (rslt != RESULT_OK)
			return rslt;
		//Sleep(25000);

		printf("%c. loaded super %s\n", myid, superblock_desc.id);
		//ShmPointer shm_pointer;
		//LONG *val2 = (LONG*)get_mem(&shm_pointer, sizeof(LONG));
		//assert(shm_pointer_is_valid(shm_pointer));
		//assert(val2);
		// assert(*superblock_count == 1);
		// LONG *val1 = (LONG*)superblock_get_block(0);
		// assert(val1);
		// use sleep before additional allocations so we can test some low level functions
		Sleep(200);
		ThreadContext *thread;
		init_thread_context(&thread);
		// for (int i = 0; i < 5; ++i)
		// {
		// 	Sleep(100);
		// 	printf("2. child val1 %d, superblock count %d\n", *val1, superblock->block_count);
		// 	*val1 = *val1 + 1;
		// 	superblock_alloc_more(thread->self, SHM_BLOCK_TYPE_ROOT, NULL);
		// }


		printf("%c. Secondary process started\n", myid);
		ShmPointer dummy_shm = (__ShmPointer)atol(argv[4]);
		ShmPointer barrier_shm = (__ShmPointer)atol(argv[5]);
		ShmPointer ret_barrier_shm = (__ShmPointer)atol(argv[6]);
		ShmPointer barrier2_shm = (__ShmPointer)atol(argv[7]);
		ShmPointer ret_barrier2_shm = (__ShmPointer)atol(argv[8]);
		ShmPointer queue_shm = (__ShmPointer)atol(argv[9]);
		ShmPointer dict_shm = (__ShmPointer)atol(argv[10]);
		ShmPointer list_shm = (__ShmPointer)atol(argv[11]);
		ShmPointer list3_shm = (__ShmPointer)atol(argv[12]);
		ShmPointer undict_shm = (__ShmPointer)atol(argv[13]);

		pint *val1 = shm_pointer_to_pointer_unsafe(dummy_shm);
		shmassert(val1);
		PromiseRef promise = { .shared = barrier_shm, .local = LOCAL(barrier_shm) };
		shmassert(promise.local->type == SHM_TYPE_PROMISE);
		PromiseRef ret_promise = { .shared = ret_barrier_shm, .local = LOCAL(ret_barrier_shm) };
		shmassert(ret_promise.local->type == SHM_TYPE_PROMISE);
		PromiseRef promise2 = { .shared = barrier2_shm, .local = LOCAL(barrier2_shm) };
		shmassert(promise2.local->type == SHM_TYPE_PROMISE);
		PromiseRef ret_promise2 = { .shared = ret_barrier2_shm, .local = LOCAL(ret_barrier2_shm) };
		shmassert(ret_promise2.local->type == SHM_TYPE_PROMISE);
		UnDictRef undict = { .shared = undict_shm, .local = LOCAL(undict_shm) };
		shmassert(undict.local->type == SHM_TYPE_UNDICT);

		printf("%c. Barrier %d, queue %d, dict_shm %d\n", myid, barrier_shm, queue_shm, dict_shm);

		int wait_result = shm_promise_wait(thread, promise.local);
		shmassert(wait_result == RESULT_OK);
		printf("%c. Passed barrier #1.\n", myid);
		// Sleep(300*1000);
		start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
		shm_promise_signal(thread, ret_promise.local, ret_promise.shared, PROMISE_STATE_FULFILLED, NONE_SHM);
		commit_transaction(thread, NULL);
		printf("%c. Signalled return barrier #1.\n", myid);

		ListRef *list = (ListRef *)thread_local_ref(thread, EMPTY_SHM);
		list->shared = list_shm;
		list->local = LOCAL(list->shared);
		((DictLocalReference*)list)->owned = false;

		test_list(thread, list, 3, true);
		printf("%c. List concurrent test finished.\n", myid);

		DictLocalReference *dict = (DictLocalReference *)thread_local_ref(thread, EMPTY_SHM);
		dict->shared = dict_shm;
		dict->local = (ShmDict *)LOCAL(dict->shared);
		dict->owned = false;

		ListRef *list3 = (ListRef *)thread_local_ref(thread, EMPTY_SHM);
		list3->shared = list3_shm;
		list3->local = LOCAL(list3->shared);
		((DictLocalReference*)list3)->owned = false;

		int wait_result2;
		do {
			wait_result2 = shm_promise_wait(thread, promise2.local);
		} while (wait_result2 == RESULT_REPEAT);

		shmassert(wait_result2 == RESULT_OK);
		printf("%c. Passed barrier #2.\n", myid);

		start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
		shm_promise_signal(thread, ret_promise2.local, ret_promise2.shared, PROMISE_STATE_FULFILLED, NONE_SHM);
		commit_transaction(thread, NULL);
		printf("%c. Signalled return barrier #2.\n", myid);

		test_undict(thread, undict, myid);
		printf("%c. Undict concurrent test finished.\n", myid);
		int succeded = verify_undict(thread, undict, 1000, myid);
		printf("%c. Verify_undict concurrent: %d out of %d\n", myid, succeded, 1000 * 2 / 3);

		// thread->debug_starts = true;
		printf("%c. test_dict...\n", myid);
		// superblock->debug_max_lock_count = 5;
		// test_dict(thread, dict, 50, false, 50, false, (QueueRef*)queue);
		test_dict(thread, dict, myid, 2, false, 2, true, NULL, list3);
		printf("%c. Finished test_dict\n", myid);

		/*Sleep(500);
		start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
		int dict_local_good = 0;
		int dict_local_count = 0;
		verify_dict_data(thread, dict, &dict_local_count, &dict_local_good);
		commit_transaction(thread, NULL);
		printf("2. dict second verification: %d good out of %d\n", dict_local_good, dict_local_count);*/

		start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
		shmassert(thread->private_data->write_locks_taken == 0);
		//ShmPointer queue_shm = 8;

		QueueLocalReference *queue = (QueueLocalReference *) thread_local_ref(thread, EMPTY_SHM);
		queue->shared = queue_shm;
		queue->local = (ShmQueue *) LOCAL(queue->shared);

		if (false)
		{
			CellLocalReference *first_cell = (CellLocalReference *)thread_local_ref(thread, EMPTY_SHM);
			first_cell->local = shm_queue_acq_first(thread, *(QueueRef*)queue, &first_cell->shared);

			CellLocalReference *next = (CellLocalReference *)thread_local_ref(thread, EMPTY_SHM);
			next->local = shm_queue_cell_acq_next(thread, first_cell->local, &next->shared);
			//assert(next->local == NULL);
			ShmValueHeader *first_value = cell_unlocked_acq_value(thread, first_cell->local, NULL);
			if (first_value)
				printf("%c. Value: %s\n", myid, (char *)shm_value_get_data(first_value));
			else
				printf("%c. No value.\n", myid);
			commit_transaction(thread, NULL);
		}

		//assert(*superblock_count == 1);
		//assert(!superblock_alloc_more());
		//assert(*superblock_count == 2);
		Sleep(100);
		while (*val1 != 1234567)
		{
			Sleep(1000); // exiting the process will dealloc the shared memory, causing segfault in the parent.
			// printf("2. exit value %d\n", *val1);
		}
		printf("%c. Exit", myid);
		release_superblock(false);
		return 0;
	}
	else
	{
		OutputDebugStringA("ping 1\n");
		assert_prefix = "p1. ";
		// SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_BELOW_NORMAL);
		if (init_superblock(NULL) != RESULT_OK)
		{
			printf("1. Failed init_superblock\n");
			return 1;
		}
		printf("1. new super %s\n", superblock_desc.id);
		superblock->debug_max_lock_count = 1;

		ShmPointer dummy;
		pint *val1 = (pint*)get_mem(NULL, &dummy, sizeof(pint), VAL_DEBUG_ID);
		printf("1. get_mem block %s\n", superblock->block_groups[0].id);
		//LONG *val2 = val1 + 1;

		// val1 must be zero block for testing purpose
		ThreadContext *thread;
		init_thread_context(&thread);

		test_mm(thread);
		test_locks(thread);

		const char *ascii_abc = "abc";
		const Py_UCS4 UCS4_abc[3] = {'a', 'b', 'c'};
		shmassert(hash_string_ascii(ascii_abc, 3) == hash_string(UCS4_abc, 3));

		random_flinch = true;
		reclaimer_debug_info = true;
		UnDictRef undict;
		undict.local = new_shm_undict(thread, &undict.shared);
		uint64_t tmp = rdtsc();
		test_undict(thread, undict, '1');
		printf("1. Test_undict single thread: %9.3f ms\n", (rdtsc() - tmp) / 3500000.0);
		int succeded = verify_undict(thread, undict, 1000, '1');
		printf("1. Verify_undict: %d out of %d\n", succeded, 1000 * 2 / 3);
		shmassert(succeded == 1000 * 2 / 3);
		test_undict(thread, undict, '1');
		printf("1. Test_undict overwrite in single thread finished\n");
		succeded = verify_undict(thread, undict, 1000, '1');
		printf("1. Verify_undict overwrite: %d of %d\n", succeded, 1000 * 2 / 3);
		shmassert(succeded == 1000 * 2 / 3);

		start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
		shm_undict_clear(thread, undict);
		commit_transaction(thread, NULL);

		// superblock->stop_reclaimer = true;

		LocalReference *loc_ref = thread_local_ref(thread, EMPTY_SHM);
		ListRef *list = (ListRef *)loc_ref;
		strcpy_s(&loc_ref->name[0], 20, "list");
		list->local = new_shm_list(thread, &list->shared);
		int deleted = test_list(thread, list, 5, false);
		verify_list(thread, list, 5, deleted);
		// print_list(thread, list);

		LocalReference *loc_ref_2 = thread_local_ref(thread, EMPTY_SHM);
		ListRef *list_2 = (ListRef *)loc_ref_2;
		strcpy_s(&loc_ref_2->name[0], 20, "list concurrent");
		list_2->local = new_shm_list(thread, &list_2->shared);

		start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
		QueueLocalReference *queue = (QueueLocalReference *)thread_local_ref(thread, EMPTY_SHM);
		strcpy_s(&queue->name[0], 20, "queue");
		queue->local = new_shm_queue(thread, &queue->shared);
		// queue->owned = TRUE;
		queue->owned = FALSE; // let's keep it in memory until process finishes
		// ------------------------ //
		DictLocalReference *dict = (DictLocalReference *)thread_local_ref(thread, EMPTY_SHM);
		strcpy_s(&queue->name[0], 20, "dict");
		dict->local = new_shm_dict(thread, &dict->shared);
		dict->owned = FALSE; // let's keep it in memory until process finishes
							 // ------------------------ //

		LocalReference *loc_ref_3 = thread_local_ref(thread, EMPTY_SHM);
		ListRef *list_3 = (ListRef *)loc_ref_3;
		strcpy_s(&loc_ref_3->name[0], 20, "list concurrent");
		list_3->local = new_shm_list(thread, &list_3->shared);

		Sleep(1000);

		commit_transaction(thread, NULL);
		/* here, do your time-consuming job */
		printf("1. test_dict\n");
		test_dict(thread, dict, '1', 5, true, 5, false, NULL, NULL);

		PromiseRef promise;
		promise.local = new_shm_promise(thread, &promise.shared);

		PromiseRef ret_promise;
		ret_promise.local = new_shm_promise(thread, &ret_promise.shared);

		PromiseRef promise2;
		promise2.local = new_shm_promise(thread, &promise2.shared);

		PromiseRef ret_promise2;
		ret_promise2.local = new_shm_promise(thread, &ret_promise2.shared);

		// return 0;

		// random_flinch = false;
		superblock->debug_max_lock_count = 4;

		printf("1. Starting second process...\n");
		printf("=================================\n");
		bool created_proc = false;
#define additional_args_count 10
		volatile unsigned int args[additional_args_count] =
			{ dummy,
			  promise.shared, ret_promise.shared, promise2.shared, ret_promise2.shared,
			  queue->shared, dict->shared, list_2->shared, list_3->shared, undict.shared };
		process_handles handles2;
		created_proc = run_child_process(argv[0], '2', additional_args_count, args, &handles2);

		if (created_proc)
		{
			printf("1. Starting third process...\n");
			printf("=================================\n");
			process_handles handles3;
			bool created_proc3 = run_child_process(argv[0], '3', additional_args_count, args, &handles3);
			if (!created_proc3)
			{
				shmassert(false);
				return 10;
			}

			// Sleep(150); // wait for both processes to load
			Sleep(1);

			child_processes[0] = handles2.pid;
			child_processes[1] = handles3.pid;
			// Sleep(720); // same sleep interval as in the second process
			start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
			shm_promise_signal(thread, promise.local, promise.shared, PROMISE_STATE_FULFILLED, NONE_SHM);
			commit_transaction(thread, NULL);
			// Sleep(1);
			printf("1. Barrier signalled #1.\n");

			int ret_wait_result = shm_promise_wait(thread, ret_promise.local);
			shmassert(ret_wait_result == RESULT_OK);
			printf("1. Passed return barrier #1.\n");


			Sleep(1);
			test_list(thread, list_2, 3, true);
			printf("1. List concurrent test finished.\n");

			Sleep(10);
			start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
			shm_promise_signal(thread, promise2.local, promise2.shared, PROMISE_STATE_FULFILLED, NONE_SHM);
			commit_transaction(thread, NULL);
			printf("1. Barrier signalled #2.\n");
			// Sleep(1);

			int ret2_wait_result;
			do {
				ret2_wait_result = shm_promise_wait(thread, ret_promise2.local);
			} while (ret2_wait_result == RESULT_REPEAT);
			shmassert(ret2_wait_result == RESULT_OK);
			printf("1. Passed return barrier #2.\n");

			test_undict(thread, undict, '1');
			Sleep(1);
			printf("1. Undict concurrent test finished.\n");
			int succeded = verify_undict(thread, undict, 1000, '1');
			printf("1. Verify_undict concurrent: %d out of %d\n", succeded, 1000 * 2 / 3);

			printf("1. Second test_dict...\n");
			test_dict(thread, dict, '1', 5, true, 0, true, (QueueRef*)queue, list_3);
			printf("1. Finished second test_dict.\n");

			// queue->local = new_shm_queue(&queue->shared);
			start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
			shmassert(thread->private_data->write_locks_taken == 0);
			if (false)
			{
				ValueLocalReference *value = (ValueLocalReference *)thread_local_ref(thread, EMPTY_SHM);
				strcpy_s(&value->name[0], 20, "value");
				CellLocalReference *cell = (CellLocalReference *)thread_local_ref(thread, EMPTY_SHM);
				strcpy_s(&cell->name[0], 20, "cell");
				{
					// value->local = shm_queue_new_value(thread, *(QueueRef*)queue, 15 * sizeof(Py_UCS4), SHM_TYPE_UNICODE, &value->shared);
					new_shm_unicode_value(thread, "Hello world", strlen("Hello world"), &value->shared);
					value->owned = TRUE;
					// ShmQueueCell *cell = shm_queue_new_cell(queue, &cell_shm); - done by shm_queue_append_consume
					value->owned = FALSE; // consumed
					if (shm_queue_append_consume(thread, *(QueueRef*)queue, value->shared, (CellRef*)cell) != RESULT_OK)
					{
						fprintf(stderr, "shm_queue_append_consume returned failure");
						assert(false);
					}
					// cell is not owned, just a fast ref
					cell->owned = FALSE;

					CellLocalReference *first_cell = (CellLocalReference *)thread_local_ref(thread, EMPTY_SHM);
					strcpy_s(&first_cell->name[0], 20, "first_cell");
					first_cell->local = shm_queue_acq_first(thread, *(QueueRef*)queue, &first_cell->shared);
					first_cell->owned = TRUE;
					assert(first_cell->shared == cell->shared);
					{
						CellLocalReference *next = (CellLocalReference *)thread_local_ref(thread, EMPTY_SHM);
						strcpy_s(&next->name[0], 20, "next");
						next->local = shm_queue_cell_acq_next(thread, first_cell->local, &next->shared);
						assert(next->local == NULL);
						ShmValueHeader *first_value = cell_unlocked_acq_value(thread, first_cell->local, NULL);
						printf("1. Value: %s\n", (char *)shm_value_get_data(first_value));

						//thread_local_clear_ref(next_shm);
					}

				}
				//thread_local_clear_ref(value_shm);
				//thread_local_clear_ref(cell_shm);
			}
			// shm_queue_release(queue_shm);


			// thread_local_clear_refs(thread); - destroyes the dict
			commit_transaction(thread, NULL);
			Sleep(100);

			print_list(thread, list_2);

			for (int i = 0; i < 5; ++i)
			{
				Sleep(1000);
				*val1 = i;
				printf("1. parent val1 %d, count %d\n", *val1, superblock->block_count);
			}
			*val1 = 1234567; // signal the child to exit.

			// start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
			// verify_list_data(thread, list_2);
			// commit_transaction(thread, NULL);

			//TerminateProcess(pi.hProcess, NO_ERROR);
			printf("1. Exit");
			// DebugBreak();
		}
		release_superblock(true);
		return 0;
	}

	return -1;
}

// tabs-spaces verified

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

// #include "windows.h"
#include <stdint.h>
#include <stdbool.h>
// #include <assert.h>
// #include <plibsys.h>
// I'm getting problems with plibsys.h because something is including both winsock.h and winsock2.h.
#define PLIBSYS_H_INSIDE
// #include "pcondvariable.h"
#include "pmacros.h"
#include "pmacroscompiler.h"
#include "pmacroscpu.h"
#include "pmacrosos.h"
#include "ptypes.h"
#include "patomic.h"
// #include "pmain.h"
// #include "pshm.h"
#include "puthread.h"
#undef PLIBSYS_H_INSIDE
// typedef unsigned int pint;

#include "shm_defs.h"
#include "shm_utils.h"
#include "shm_memory.h"

bool
result_is_abort(int result);
bool
result_is_repeat(int result);

// inline bool
// CAS(PShmPointer data, __ShmPointer new_value, __ShmPointer old_value);
// 
// inline bool
// CAS2(PShmPointer data, __ShmPointer new_value, __ShmPointer old_value);

/*inline bool
PCAS(PShmPointer data, __ShmPointer new_value, __ShmPointer old_value);
inline bool
PCAS2(PShmPointer data, __ShmPointer new_value, __ShmPointer old_value);
inline bool
CAS(vl ShmInt *data, ShmInt new_value, ShmInt old_value);
inline bool
CAS2(vl ShmInt *data, ShmInt new_value, ShmInt old_value);*/


static inline bool
PCAS(PShmPointer data, __ShmPointer new_value, __ShmPointer old_value)
{
	// return InterlockedCompareExchange(data, new_value, old_value) == old_value;
	return p_atomic_pointer_compare_and_exchange(data, (ppointer)old_value, (ppointer)new_value);
}

static inline bool
PCAS2(PShmPointer data, __ShmPointer new_value, __ShmPointer old_value)
{
	// return *data == old_value && InterlockedCompareExchange(data, new_value, old_value) == old_value;
	return *data == old_value && p_atomic_pointer_compare_and_exchange(data, (ppointer)old_value, (ppointer)new_value);
}

static inline bool
CAS(vl ShmInt *data, ShmInt new_value, ShmInt old_value)
{
	return p_atomic_int_compare_and_exchange(data, old_value, new_value);
}

static inline bool
CAS2(vl ShmInt *data, ShmInt new_value, ShmInt old_value)
{
	return *data == old_value && p_atomic_int_compare_and_exchange(data, old_value, new_value);
}

// rdtsc and pause()
#ifdef P_CC_MSVC
	#include <intrin.h>
#else
#	include <x86intrin.h>
#endif

// https://www.boost.org/doc/libs/1_36_0/boost/detail/yield_k.hpp
// https://github.com/gstrauss/plasma/blob/master/plasma_spin.h
#if defined(_MSC_VER) && _MSC_VER >= 1310 && ( defined(_M_IX86) || defined(_M_X64) )
// extern "C" void _mm_pause();
// #pragma intrinsic( _mm_pause )
	#define SHM_SMT_PAUSE _mm_pause();
#elif defined(__GNUC__) && ( defined(__i386__) || defined(__x86_64__) )
// #define SHM_SMT_PAUSE __asm__ __volatile__("pause;");
	#define SHM_SMT_PAUSE  _mm_pause()
#elif defined(__arm__)
	#ifdef __CC_ARM
		#define SHM_SMT_PAUSE  __yield()
	#else
		#define SHM_SMT_PAUSE  __asm__ __volatile__ ("yield")
	#endif
#endif

// intrin.h/x86intrin.h
static inline uint64_t rdtsc() {
	return __rdtsc();
}

#ifdef P_CC_MSVC
	// #include <Windows.h>
#else
	#include <unistd.h>
	#include <sched.h>
//	#define Sleep(msec)  usleep(msec * 1000)
static inline void
Sleep(unsigned int msec) {
	if (msec == 0)
		sched_yield();
	else
		usleep(msec * 1000);
}
#endif

#define SPINLOCK_MAX_BACKOFF 4 * 1024
#define SPINLOCK_MAX_SLEEP_BACKOFF 4 * 1024 * 1024
#define take_spinlock(method, lock, new_value, compare_value, test)    do { \
	int backoff = 64; \
	/* int max_backoff = 8 * 1024 * 1024; - too much */ \
	/*int sum = *lock;*/ \
	long count = 0; \
	while (! method(lock, new_value, compare_value)) \
	{ \
		test; /* shall perform wait condition test and set "rslt" value */ \
		if (backoff <= SPINLOCK_MAX_BACKOFF)  \
		{ \
			for (int i = 0; i < backoff; ++i) \
				/*sum += dummy_load_function(sum);*/ \
				SHM_SMT_PAUSE; \
			backoff *= 2;  \
		} \
		else if (backoff <= SPINLOCK_MAX_SLEEP_BACKOFF) \
		{ \
			Sleep(0);  \
			backoff *= 2; \
		} \
		else \
		{ \
			Sleep(1); \
		} \
		count++; \
	}  \
} while (0);

#define release_spinlock(lock, new_value, compare_value)    do { \
	shmassert(*lock = compare_value); \
	p_atomic_int_set(lock, new_value); \
} while (0);

#define if_failure(code, code2)   do { \
	int status = code; \
	if (status != RESULT_OK) \
		code2; \
} while (0)

#include "string.h"

static inline void
memclear(vl void *dst, ShmInt count)
{
	memset(CAST_VL(dst), 0, (puint)count);
}
// strcpy/strncpy?

#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
typedef vl struct {
	ShmInt value;
	ShmInt handle_id;
	ShmInt main_handle_valid;
	ShmEventHandle main_handle;
} ShmAbstractHandle;

typedef vl struct {
	// ShmAbstractHandle
	ShmInt ready;
	ShmInt handle_id; // index of private handle
	ShmInt main_handle_valid;
	ShmEventHandle main_handle; // handle belonging to the owner process
	// ShmEvent
	ShmInt waiting; // to avoid syscalls when the event listener is not listening
	// debug
	ShmInt last_set;
} ShmEvent;

typedef int (*ShmSimpleLockCallback) (void *data);

typedef vl struct {
	// ShmAbstractHandle
	ShmInt lock_count;
	ShmInt handle_id;
	ShmInt main_handle_valid;
	ShmEventHandle main_handle;
	// ShmSimpleLock
	//   None
	// For debug purpose
	ShmInt owner;
	ShmInt threads_inside;
	ShmInt contention_count;
	uint32_t contention_duration;
	uint32_t contention_duration_high;
	int release_line;
} ShmSimpleLock; // similar to windows critical section

#else // defined(P_OS_WIN) || defined(P_OS_WIN64)

typedef vl struct {
	uint32_t value; // futex requires 32 bit value on all platforms
} ShmAbstractHandle;

typedef vl struct {
	uint32_t ready;
} ShmEvent;

typedef int (*ShmSimpleLockCallback) (void *data);

typedef vl struct {
	// ShmAbstractHandle
	uint32_t lock_state;
	ShmThreadID owner;
	// test show false sharing is not a problem here because the code usually accesses all the fields simultaneously
	// for debug purpose
	ShmThreadID last_owner;
	ShmInt threads_inside;
	ShmInt contention_count;
	ShmInt wait_count;
	ShmInt wake_count;
	uint32_t contention_duration;
	uint32_t contention_duration_high;
	int release_line;
} ShmSimpleLock;

#endif // defined(P_OS_WIN) || defined(P_OS_WIN64)

typedef vl struct _ShmThreads {
	ShmInt size;
	ShmInt lock;
	ShmInt count;
	ShmPointer threads[MAX_THREAD_COUNT];
} ShmThreads;

typedef vl struct {
	ShmPointer head;
	ShmPointer tail;
} ShmHeapSectorList;

typedef vl struct {
	ShmInt taken;
	ShmInt count;
	ShmInt halt;
	ShmPointer threads[MAX_THREAD_COUNT];
} ShmCoordinatorData;

typedef vl struct {
	ShmInt size;
	// semi-volatile
	ShmPointer thread;
	ShmThreadID owner;
	// volatiles
	// ShmInt lock;
	ShmSimpleLock lock;
	ShmInt count;
	ShmPointer large_segment;
	ShmHeapSectorList fixed_sectors;
	ShmHeapSectorList flex_sectors;
} ShmHeap;

typedef vl struct {
	ShmInt size;
	ShmInt lock;
	ShmInt count;
	ShmPointer self;
	ShmHeap heaps[MAX_THREAD_COUNT]; // ShmHeap[]
} ShmSuperheap;

#define MAX_DEBUG_ID 50
typedef vl char type_debug_id_list[MAX_DEBUG_ID][32];

typedef vl struct {
	ShmInt type;
	ShmProcessID coordinator_process;
	ShmCoordinatorData coordinator_data;
	ShmSimpleLock lock;
	ShmInt ticket;
	ShmInt mm_last_used_root_sector;
	ShmInt last_available_event_id;
	ShmInt block_count;
	ShmChunk block_groups[SHM_BLOCK_COUNT / SHM_BLOCK_GROUP_SIZE];
	ShmThreads threads;
	ShmSuperheap superheap;
	ShmInt has_garbage;
	ShmInt stop_reclaimer;
	ShmEvent has_garbage_event;
	ShmPointer root_container; // dictionary for storing global objects visible for every worker
	// debug
	ShmPointer ticket_history[64];
	ShmInt debug_lock_count;
	ShmInt pending_lock_count;
	ShmInt debug_max_lock_count;
	type_debug_id_list type_debug_ids;
} ShmSuperblock;

#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
typedef vl struct {
	ShmEventHandle handle;
} ShmPrivateEvent;

// just a random multiplier, we have at least MAX_THREAD_COUNT*2 possible events (heaps and threads)
#define MAX_EVENTS_COUNT (MAX_THREAD_COUNT * 5)

typedef vl struct {
	ShmInt count;
	ShmPrivateEvent events[MAX_EVENTS_COUNT];
} ShmPrivateEvents;
extern ShmPrivateEvents *private_events; // array of pointers, private. Keeps copies of all the shared events
#endif

extern ShmSuperblock *superblock; // shared data
extern ShmChunk superblock_desc; // private data, explicitly synchronized
extern vl char * superblock_mmap[SHM_BLOCK_COUNT / SHM_BLOCK_GROUP_SIZE]; // array of pointers, private
// Superblock shall be a 0xFFFF block, while 0x0000 should be an invalid block.

# define NONE_SHM 0
# define EMPTY_SHM ((__ShmPointer)-1)
# define DEBUG_SHM ((__ShmPointer)-2)

// ShmAbstract
#define SHM_TYPE_FLAG_REFCOUNTED  (1 << 8)
// ShmContainedBlock
#define SHM_TYPE_FLAG_CONTAINED  (2 << 8)
// ShmCellBase
#define SHM_TYPE_FLAG_MUTABLE (4 << 8)
#define SHM_TYPE_MASK ((1 << 8) - 1)
#define SHM_TYPE_RELEASE_MARK 0xFF000000
#define SHM_TYPE_CELL  (SHM_TYPE_FLAG_MUTABLE | SHM_TYPE_FLAG_REFCOUNTED)

#define SHM_TYPE_BOOL     (1 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_LONG     (2 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_FLOAT    (3 | SHM_TYPE_FLAG_REFCOUNTED)
// kinda same structure as SHM_TYPE_REF_UNICODE, just legacy
#define SHM_TYPE_UNICODE  (4 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_REF_UNICODE  (4 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_BYTES  (5 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_TRANSACTION_ELEMENT   (0x10)
// currently same as SHM_TYPE_UNDICT
#define SHM_TYPE_OBJECT  (0x20 | SHM_TYPE_CELL)
#define SHM_TYPE_TUPLE  (0x30 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_LIST  (0x40 | SHM_TYPE_CELL)
#define SHM_TYPE_LIST_BLOCK  (0x41 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_LIST_CELL  (0x42 | SHM_TYPE_FLAG_CONTAINED)
#define SHM_TYPE_LIST_INDEX  (0x44 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_LIST_CHANGES  0x48
#define SHM_TYPE_QUEUE  (0x50 | SHM_TYPE_CELL)
#define SHM_TYPE_QUEUE_CELL  (0x51 | SHM_TYPE_CELL)
#define SHM_TYPE_QUEUE_CHANGES  0x54
#define SHM_TYPE_DICT ( 0x60 | SHM_TYPE_CELL)
#define SHM_TYPE_DICT_ELEMENT_ARRAY  0x61
#define SHM_TYPE_DICT_DELTA  0x64
#define SHM_TYPE_UNDICT  (0x70 | SHM_TYPE_CELL)
#define SHM_TYPE_UNDICT_TABLE  (0x74 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_UNDICT_INDEX  (0x74 | 1 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_UNDICT_DELTA_TABLE  (0x76 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_UNDICT_DELTA_INDEX  (0x76 | 1 | SHM_TYPE_FLAG_REFCOUNTED)
#define SHM_TYPE_PROMISE  (0x80 | SHM_TYPE_CELL)
#define SHM_TYPE_DEBUG    0xAA

static inline ShmInt
shm_type_get_type(ShmInt type)
{
	return type & SHM_TYPE_MASK;
}

#define SHM_TYPE(x)  (x & SHM_TYPE_MASK)

#define SHM_ABSTRACT_BLOCK \
	ShmInt type; \
	ShmInt size;

/* type - is a universal first tag for the block identification.
 * size - means complete size of the block, with header and data.
 * refcount - refcount
 * revival_count - is used for ocassions of block being revived after refcount briefly hitting 0.
 *                 You are allowed to modify refcount any way inside the transaction
 *                 but you should decide on the final refcount by the transaction's end.
 *                 Unlike classic RCU/quiescent-state reclamation, we allow arbitrary refcount modification for readers,
 *                 even outside transactions sometimes e.g. when the reference was acquired inside transaction.
 *                 The counter is required to detect the revival inside transaction
 *                 and delay the reclamation until some latter reclamation cycle,
 *                 otherwise that block might become reclaimed instantly on subsequent brief zeroing of refcount
 *                 due to some other running transaction during the first scheduled reclamation cycle.
 *                 Each revival (atomic increment from zero) corresponds to exactly one additional
 *                 release request (atomic decrement to zero).
 *                 The block will be truly reclaimed after exactly revival_count + 1 release requests processed.
 *                 We could have implemented removal of the release request from queue during the revival of the object
 *                 but that would be complex and inefficient.
 *                 Though frequent queueing for reclamations due to refcount hitting 0 is inefficient too so it's better be avoided.
 *                 It could've been a good idea to trigger revival/release at refcount = 1 for items contained in a tree hierarchy
 *                 thus eliminating the need for additional reclamation cycles after branches are dereferenced
 *                 in the tree's root destructor.
 * release_count - how many release request already processed. The object is dead when release_count = revival_count and
 *                 all running transactions have finished.
 *
 * I'm already thinking about some kind of unified "grow only" counters,
 * so we can register every acquire as "revival" and eliminate the separate refcount field.
 * Or we might merge revival_count and release_count into single field that is decremented on release request.
 * */
#define SHM_REFCOUNTED_BLOCK \
	ShmInt type; \
	ShmInt size; \
	ShmInt refcount; \
	ShmInt revival_count; \
	ShmInt release_count;

#define SHM_CONTAINED_BLOCK \
	ShmInt type; \
	ShmInt size; \
	ShmPointer container;

typedef vl struct _ShmAbstractBlock
{
	SHM_ABSTRACT_BLOCK
} ShmAbstractBlock;

typedef vl struct _ShmRefcountedBlock
{
	SHM_REFCOUNTED_BLOCK
} ShmRefcountedBlock;

typedef vl struct _ShmContainedBlock
{
	SHM_CONTAINED_BLOCK
} ShmContainedBlock;

#define SHM_CONTAINER \
	SHM_REFCOUNTED_BLOCK \
	ShmLock     lock;

typedef vl struct ThreadContext_ ThreadContext;

typedef uint64_t ShmReaderBitmap;

static inline ShmReaderBitmap
atomic_bitmap_get(const volatile ShmReaderBitmap *p)
{
	return *p; // not so atomic
}

static inline ShmReaderBitmap
atomic_bitmap_exclude_thread(const ShmReaderBitmap bitmap, int index)
{
	return bitmap & (~(UINT64_C(1) << index));
}

static inline bool
atomic_bitmap_check_exclusive(const volatile ShmReaderBitmap *p, int index)
{
	ShmReaderBitmap bitmap = atomic_bitmap_get(p);
	return (bitmap & (~(UINT64_C(1) << index))) == 0;
}

static inline bool
atomic_bitmap_check_me(const volatile ShmReaderBitmap *p, int index)
{
	ShmReaderBitmap bitmap = atomic_bitmap_get(p);
	return (bitmap & (UINT64_C(1) << index)) != 0;
}

static inline ShmReaderBitmap
atomic_bitmap_contenders(const volatile ShmReaderBitmap *p, int index)
{
	ShmReaderBitmap bitmap = atomic_bitmap_get(p);
	return (bitmap & (~(UINT64_C(1) << index)));
}

static inline ShmReaderBitmap
atomic_bitmap_thread_mask(int index)
{
	return UINT64_C(1) << index;
}

static inline bool
atomic_bitmap_set(volatile ShmReaderBitmap *p, int index)
{
	shmassert(index < 64 && index >= 0);
	if (atomic_bitmap_check_me(p, index))
		return true;
	else
	{
/*#if defined (InterlockedBitTestAndSet64)
		return InterlockedBitTestAndSet64(p, index);
#else
		uint32_t *bitmap32 = (uint32_t *)p;
		if (index >= 32)
		{
			bitmap32 += 1;
			index -= 32;
		}

		return InterlockedBitTestAndSet(bitmap32, index);
#endif */
		// the function simply takes next 32 bits when index >= 32
		return p_atomic_bit_test_and_set((volatile psize *)p, (puint)index);
	}
}

static inline bool
atomic_bitmap_reset(volatile ShmReaderBitmap *p, int index)
{
	shmassert(index < 64 && index >= 0);
	if (!atomic_bitmap_check_me(p, index))
		return false;
	else
	{
/* #if defined (InterlockedBitTestAndSet64)
		return InterlockedBitTestAndReset64(p, index);
#else
		uint32_t *bitmap32 = (uint32_t *)p;
		if (index >= 32)
		{
			bitmap32 += 1;
			index -= 32;
		}

		 return InterlockedBitTestAndReset(bitmap32, index);
#endif */

		return p_atomic_bit_test_and_reset((volatile psize *)p, (puint)index);
	}
}

static inline __ShmPointer
p_atomic_shm_pointer_get(ShmPointer *p)
{
	// silence the stupid compiler
	return (__ShmPointer)(ppointer)p_atomic_pointer_get(p);
}

static inline void
p_atomic_shm_pointer_set(ShmPointer *p, __ShmPointer value)
{
	p_atomic_pointer_set(p, (ppointer)value);
}

static inline bool
p_atomic_shm_pointer_cas(ShmPointer *p, __ShmPointer old_value, __ShmPointer new_value)
{
	return p_atomic_pointer_compare_and_exchange(p, (ppointer)old_value, (ppointer)new_value);
}

static inline __ShmPointer
p_atomic_shm_pointer_exchange(ShmPointer *p, __ShmPointer new_value)
{
	return (__ShmPointer)(ppointer)p_atomic_pointer_exchange(p, (ppointer)new_value);
}

#define LOCK_UNLOCKED 0
#define LOCK_PENDING  EMPTY_SHM

typedef vl struct {
	// ShmPointer id;
	ShmReaderBitmap reader_lock;  // only owner can change its bit
	// Make a way for old transactions
	// ShmPointer rw_barrier_thread; // thread is about to get the writer_lock
	// ShmTicks rw_barrier_value; // must always accompany the writer_lock_pending or writer_lock, otherwise readers will force those threads to abort
	ShmPointer writer_lock; // pointer to a single owning thread
	// ShmPointer writer_lock_ensured; // pointer to a single owning thread
	ShmPointer next_writer; // Used to negotiate for the highest priority next owner. Also functions as a barrier for readers.
	                        // Set only once by a each transaction cycle and cleared once the lock is released (commit) or transaction cycle abandoned.
	ShmReaderBitmap queue_threads; // only owner can change its bit
	// ShmPointer queue;
	ShmPointer transaction_data; // pointer to a storage of changes, usually the container itself. Is used to determine the cell locked and needs no second lock.
	// debug
	ShmPointer prev_lock;
	ShmInt release_line;
	// those register only real uses of read/write locks, not just flags during contention resolution.
	ShmInt writers_count;
	ShmInt readers_count;
	ShmInt read_contention_count;
	ShmInt write_contention_count;
	ShmInt break_on_contention;
} ShmLock;

typedef vl struct _ShmContainer
{
	SHM_CONTAINER
} ShmContainer;

#define PROMISE_STATE_INVALID (-1)
#define PROMISE_STATE_PENDING 0
#define PROMISE_STATE_FULFILLED 1
#define PROMISE_STATE_REJECTED 2

typedef vl struct {
	SHM_CONTAINER
	// ShmBarrierEvent
	ShmInt state;
	ShmPointer value;
	ShmInt new_state;
	ShmPointer new_value;
	ShmReaderBitmap waiters;
	ShmInt finalized;
} ShmPromise;

typedef vl2 struct {
	ShmPointer shared;
	ShmPromise *local;
} PromiseRef;

ShmPromise *
new_shm_promise(ThreadContext *thread, ShmPointer *out_pntr);
int
shm_promise_wait(ThreadContext *thread, ShmPromise *promise);
int
shm_promise_get_state(ThreadContext *thread, ShmPromise *promise, ShmInt *state, bool owned);
int
shm_promise_signal(ThreadContext *thread, ShmPromise *promise, ShmPointer promise_shm, ShmInt new_state, ShmPointer value);
int
shm_promise_rollback(ThreadContext *thread, ShmPromise *promise);
int
shm_promise_commit(ThreadContext *thread, ShmPromise *promise);
int
shm_promise_unlock(ThreadContext *thread, ShmPromise *promise, ShmInt type);
int
shm_promise_finalize(ThreadContext *thread, PromiseRef promise);

typedef vl struct
{
	ShmPointer  data;
	bool        has_new_data;
	ShmPointer  new_data;
} ShmCellBase;

typedef vl struct _ShmCell
{
	SHM_CONTAINER
	// ShmCell/ShmCellBase
	ShmPointer  data;
	bool        has_new_data;
	ShmPointer  new_data;
} ShmCell;

typedef vl struct _ShmQueueCell
{
	SHM_CONTAINER
	// ShmCell
	ShmPointer  data;
	ShmInt      has_new_data;
	ShmPointer  new_data;
	// ShmQueueCell
	ShmPointer next;
	ShmPointer new_next; // NONE_SHM for unused, EMPTY_SHM for "set next to EMPTY_SHM"
} ShmQueueCell;

typedef ShmRefcountedBlock ShmValueHeader;

#define DELTA_ARRAY_SIZE 20

// Entity made for quick commit/rollback of changed items instead of traversing the whole list.
// Stores links to the modified items. Modified items in turn point to this storage, while unmodified usually don't.
typedef vl struct _ShmQueueChanges {
	SHM_ABSTRACT_BLOCK

	ShmInt count;
	ShmPointer cells[DELTA_ARRAY_SIZE];
} ShmQueueChanges;

typedef vl struct _ShmQueue
{
	ShmContainer base;
	ShmPointer   head;
	ShmPointer   new_head; // NONE_SHM means unused, EMPTY_SHM means "head changed to EMPTY_SHM"
	// Every list cell is referenced only once by container. So tail does not increase ref count and is supposed to point to an item owned by this container only
	ShmPointer   tail; // pointer to the last block (to speed up appending)
	ShmPointer   new_tail; // pointer to the last block (to speed up appending)
	ShmInt       count;
	ShmPointer   type_desc; // for testing purposes we will use PChar (type_desc = 0) as the only data type
	ShmPointer   changes_shm; // (ShmQueueChanges *)
} ShmQueue;

// ///////////////
// ShmTuple
// ///////////////

typedef vl2 struct {
	ShmPointer shared;
	ShmValueHeader *local;
} TupleRef;

// ---------------------------
// ShmList
// ---------------------------

// Need a special type because ShmListCell cannot be handled as a separate object e.g. with shm_pointer_to_pointer
typedef vl struct _ShmListCell
{
	SHM_CONTAINED_BLOCK
	// ShmCellBase
	ShmPointer  data;
	ShmInt      has_new_data;
	ShmPointer  new_data;
	// ShmListCell
	ShmInt changed; // for committing, to avoid registering changes for same cell twice
} ShmListCell;

typedef vl struct _ShmListBlockHeader
{
	SHM_REFCOUNTED_BLOCK

	ShmInt count;
	ShmInt new_count;
	ShmInt deleted;
	ShmInt new_deleted;
	ShmInt count_added_after_relocation;
	ShmInt capacity;
} ShmListBlockHeader;

typedef vl struct _ShmListBlock
{
	SHM_REFCOUNTED_BLOCK
	// ShmListBlockHeader
	ShmInt count;
	ShmInt new_count;
	// We can only delete from the start of the block. Thus deleted is an offset for stored items
	ShmInt deleted;
	ShmInt new_deleted;
	ShmInt count_added_after_relocation;
	ShmInt capacity; // count + deleted <= capacity
	ShmListCell cells[P_MAXINT / sizeof(ShmListCell) / 2];
} ShmListBlock;

// For simplicity block can only grow in size. Thus any transaction attempting to append elements first reallocates ShmListBlock
// and then either commits or rolls back changes, but the new block stays in this field in both cases, so ShmListChangeItem can unambigously point to the modified item.
typedef vl struct _ShmListIndexItem
{
	// duplicates similar fields from the ShmListBlock for quick access
	ShmInt count;
	ShmInt new_count;
	ShmInt deleted;
	ShmInt new_deleted;
	// ShmListBlock
	ShmPointer block;
} ShmListIndexItem;

typedef vl struct _ShmListIndexHeader
{
	SHM_REFCOUNTED_BLOCK
	// ShmListIndexHeader
	ShmInt index_size;
} ShmListIndexHeader;

typedef vl struct _ShmListIndex
{
	SHM_REFCOUNTED_BLOCK
	// ShmListIndex
	// sorry, I was tired of coding mutable structures - just made an immutable one. Thanksfully it's not resized often.
	ShmInt index_size;
	// alignment is not a problem here because everything is ShmInt
	ShmListIndexItem cells[P_MAXINT / sizeof(ShmListCell) / 2];
} ShmListIndex;

typedef vl struct {
	ShmInt block_index;
	ShmInt item_index;
} ShmListChangeItem;

typedef vl struct _ShmListChanges {
	SHM_ABSTRACT_BLOCK

	ShmInt count;
	ShmListChangeItem cells[DELTA_ARRAY_SIZE];
} ShmListChanges;

// Optimized for appending at tail and popping items from head, thus working well for regular vectors and queues
// Still not a queue though.
// Also it would be easy to implement popping from tail for stack-like behaviour.
typedef vl struct _ShmList
{
	ShmContainer base;
	// We cannot allocate a block larger than "SHM_FIXED_BLOCK_SIZE - sizeof(ShmHeapSectorHeader)",
	// so for large lists we put ShmListIndex here instead of ShmListBlock.
	// Currently storage can only grow, so we store the largest storage here during transaction no matter it is successfull or not.
	ShmPointer  top_block;
	ShmInt      count;
	ShmInt      new_count;
	ShmInt      deleted;
	ShmInt      new_deleted;
	ShmPointer  type_desc; // for testing purposes we will use PChar (type_desc = 0) as the only data type
	ShmPointer  changes_shm; // (ShmListChanges *)
	ShmInt inited;
} ShmList;

typedef vl2 struct {
	ShmPointer shared;
	ShmList *local;
} ListRef;

typedef struct {
	int count;
	int deleted;
} ShmListCounts;

#define SHM_LIST_INVALID_COUNTS { .count = -1, .deleted = 0 }

ShmList *
new_shm_list(ThreadContext *thread, PShmPointer result);
int
shm_list_get_count(ThreadContext *thread, ListRef list, ShmListCounts *result, bool debug);
int
shm_list_get_item(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer *result);
int
shm_list_acq_item(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer *result);
int
shm_list_set_item_raw(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer value, bool consume);
int
shm_list_set_item(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer value);
int
shm_list_consume_item(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer value);
ShmListCounts
shm_list_get_fast_count(ThreadContext *thread, ShmList *list, bool owned);
ShmList *
new_shm_list_with_capacity(ThreadContext *thread, PShmPointer result, ShmInt capacity);
int
shm_list_append(ThreadContext *thread, ListRef list, ShmPointer value, ShmInt *index);
int
shm_list_append_consume(ThreadContext *thread, ListRef list, ShmPointer value, ShmInt *index);
int
shm_list_popleft(ThreadContext *thread, ListRef list, ShmPointer *result);
void
shm_list_print_to_file(FILE *file, ShmList *list);

// Strings

/*typedef vl struct {
	// ShmAbstractBlock
	ShmInt type;
	ShmInt size;
	ShmInt refcount;
} ShmRefString;*/

// unicodeobject.h
typedef uint32_t Py_UCS4;
typedef uint16_t Py_UCS2;
typedef uint8_t Py_UCS1;

typedef vl struct {
	SHM_REFCOUNTED_BLOCK
	Py_UCS4 chars;
} ShmRefUnicode;

typedef struct {
	ShmInt len;
	vl Py_UCS4 *data;
} RefUnicode;

void
ASCII_from_UCS4(Py_UCS1 *to, const Py_UCS4 *from, ShmInt length);
void
UCS4_from_ASCII(Py_UCS4 *to, const Py_UCS1 *from, ShmInt length);
int
UCS4_format_number(Py_UCS4 *to, int number);

ShmPointer
shm_ref_unicode_new_ascii(ThreadContext *thread, const Py_UCS1 *s, int len);
ShmPointer
shm_ref_unicode_new(ThreadContext *thread, const Py_UCS4 *s, int len);
RefUnicode
shm_ref_unicode_get(ShmPointer p);

uint32_t
hash_string(const Py_UCS4 *s, int len);
uint32_t
hash_string_ascii(const char *s, int len);

ShmValueHeader *
new_shm_unicode_value(ThreadContext *thread, const char *s, int length, PShmPointer shm_pointer);

// ---------------------------
// Hash trie, but then I realized we don't really need multiversioning and lock-free operations,
// so abandonned it.
// ---------------------------
typedef vl struct
{
	uint32_t hash;
	ShmInt keysize;
	ShmPointer key;
	char *key_debug;
	ShmPointer data;
	ShmInt has_new_data;
	ShmPointer new_data;
	ShmPointer nested; // points to a nested ShmDictElementArray
} ShmDictElement;

typedef vl struct
{
	SHM_ABSTRACT_BLOCK
	ShmDictElement first;
} ShmDictElementArray;

#define DICT_LEVEL_BITS 2
#define DICT_LEVEL_SIZE 4

#define DICT_DELTA_NEW_ROOT 0 // created root array
#define DICT_DELTA_NEW_NODE 1 // created nested array
#define DICT_DELTA_DEL_ROOT 2 // removed root array
#define DICT_DELTA_DEL_NODE 3 // removed nested array
#define DICT_DELTA_CHANGED 4 // changed single element

// New items are added invisible with data = EMPTY.
// Appropriately, items are removed by marking them to become invisible (new_data = EMPTY)
typedef vl struct
{
	ShmInt type;
	// both value and element are not refcounted
	ShmPointer value;
	ShmPointer parent_element;
} ShmDictDeltaElement;

#define SHM_DICT_DELTA_ARRAY_SIZE 30

typedef vl struct {
	SHM_ABSTRACT_BLOCK
	ShmInt capacity;
	ShmInt count;
	ShmDictDeltaElement deltas[DELTA_ARRAY_SIZE];
} ShmDictDeltaArray;

// must duplicate the ShmCell fields
// Inplace modification dictionary, so two versions are visible at one moment (one for owning transaction and one for the readers)
typedef vl struct _ShmDict
{
	SHM_CONTAINER
	// ShmDict
	ShmPointer  data;  // pointer to level 1 array. No need for "new_data" because the new array will be always initialized with at least empty values
	ShmInt count;
	ShmPointer delta; // ShmDictDeltaArray
} ShmDict;

// Unordered hash map
// ShmUnDict

typedef struct {
	uint32_t hash;
	Py_UCS1 *key1;
	Py_UCS2 *key2;
	Py_UCS4 *key4;
	ShmPointer key_shm;
	int keysize;
} ShmUnDictKey;

#define EMPTY_SHM_UNDICT_KEY { .hash = 0, .key1 = NULL,.key2 = NULL,.key4 = NULL, .keysize = 0, .key_shm = EMPTY_SHM }

#define SHM_UNDICT_LOGCOUNT_LIMIT 28

typedef vl struct _ShmUnDict
{
	SHM_CONTAINER
	// ShmDict
	ShmPointer class_name;
	ShmPointer  buckets; // ShmUnDictTableHeader
	ShmInt count;
	ShmInt deleted_count;
	ShmPointer delta_buckets; // ShmUnDictTableHeader
	ShmInt delta_count;
	ShmInt delta_deleted_count;
} ShmUnDict;

// ---------------------------------
#define LOCAL_REFERENCE_BLOCK_SIZE 50

typedef vl2 struct {
	ShmPointer shared;
	void *local;
} ResultReference;

typedef vl2 struct {
	ShmPointer shared;
	void *local;
	bool owned;
	char name[19];
} LocalReference;

typedef vl2 struct {
	ShmPointer shared;
	ShmValueHeader *local;
	bool owned;
	char name[19];
} ValueLocalReference;

typedef vl2 struct {
	ShmPointer shared;
	ShmCell *local;
} CellRef;

typedef vl2 struct {
	ShmPointer shared;
	ShmQueueCell *local;
} QueueCellRef;

typedef vl2 struct {
	ShmPointer shared;
	ShmQueue *local;
} QueueRef;

typedef vl2 struct {
	ShmPointer shared;
	ShmListCell *local;
} ListCellRef;

typedef vl2 struct {
	ShmPointer shared;
	ShmListBlock *local;
} ShmListBlockRef;

typedef vl2 struct {
	ShmPointer shared;
	ShmDict *local;
} DictRef;

typedef vl2 struct {
	ShmPointer shared;
	ShmUnDict *local;
} UnDictRef;

typedef vl2 struct {
	ShmPointer shared;
	ShmCell *local;
	bool owned;
	char name[19];
} CellLocalReference;

typedef vl2 struct {
	ShmPointer shared;
	ShmQueue *local;
	bool owned;
	char name[19];
} QueueLocalReference;

typedef vl2 struct {
	ShmPointer shared;
	ShmDict *local;
	bool owned;
	char name[19];
} DictLocalReference;

// singly linked block list
typedef vl2 struct {
	ShmInt count;
	LocalReference references[LOCAL_REFERENCE_BLOCK_SIZE];
	LocalReference *next;
} LocalReferenceBlock;

#define CONTAINER_NONE 0
#define CONTAINER_CELL 1
#define CONTAINER_LIST 2
#define CONTAINER_QUEUE 3
#define CONTAINER_DICT_DELTA 4
#define CONTAINER_UNORDERED_DICT 5
#define CONTAINER_PROMISE 6

#define TRANSACTION_ELEMENT_READ 1
#define TRANSACTION_ELEMENT_WRITE 2

// Transaction (transient or persistent) saves taken locks and data changes in this structure.
// It's supposed to make easier commit/rollback operations, including fast preemtption.
// Currently nested transaction are not supported for simplicity of implementation,
//   so single locked item is guaranteed to have only one associated ShmTransactionElement.
// For performance reason, every modified container keeps the link to this structure, so we can get the transaction data quickly for rollback.
// Modified container still contains the modified value itself, because the ratio is (1 container):(1 ShmTransactionElement):(N values).
typedef vl struct ShmTransactionElement_ ShmTransactionElement;
typedef vl struct ShmTransactionElement_ {
	SHM_ABSTRACT_BLOCK
	ShmPointer owner; // thread that created this item
	// ShmPointer lock; // need to check it every time we commit/rollback, coz we might've already lost the lock due to rollback from outside.
	ShmInt element_type;
	ShmInt container_type;
	ShmPointer container; // ShmDictDeltaArray or ShmQueueChanges
	ShmTransactionElement *next;
	ShmPointer next_shm; // it will not be usually used from outside, but still usefull e.g. for freeing memory.
} ShmTransactionElement;

#define TRANSACTION_STACK_SIZE 100
typedef vl struct {
	ShmInt transaction_mode;
	ShmInt transaction_lock_mode;
} TransactionMode;

typedef vl struct {
	ShmInt count;
	TransactionMode modes[TRANSACTION_STACK_SIZE];
} ShmTransactionStack;

#define THREAD_FREE_LIST_BLOCK_SIZE 50

typedef vl struct {
	ShmInt capacity;
	ShmInt count;
	ShmPointer next; // (ShmFreeList*)
	ShmPointer items[THREAD_FREE_LIST_BLOCK_SIZE];
} ShmFreeList;

#define DELTA_STACK_SIZE 50

typedef vl struct {
	ShmFreeList *free_list;
	ShmPointer free_list_shm;
	ShmTransactionStack *transaction_stack;

	ShmInt write_locks_taken;
	ShmInt read_locks_taken;
	// to backtrack the take_write_lock
	ShmInt last_take_write_lock_result;
	ShmInt last_operation;
	int last_operation_rslt;
	ShmLock *last_writer_lock_pntr;
	ShmPointer last_writer_lock;
	// RESULT_WAIT_SIGNAL debug
	ShmPointer last_wait_oldest;
	int last_wait_oldest_index;
	ShmReaderBitmap last_wait_queue;
	ShmPointer last_wait_writer_lock;
	ShmPointer last_wait_next_writer;

	ShmInt last_known_ticket;

	ShmInt pending_lock_count;

	ShmInt times_aborted;
	ShmInt tickets_aborted;
	ShmInt times_waiting;
	ShmInt tickets_waiting;
	ShmInt times_waiting2;
	ShmInt tickets_waiting2;
	ShmInt times_repeated;
	ShmInt tickets_repeated;
	ShmInt times_aborted1;
	ShmInt tickets_aborted1;
	ShmInt times_aborted2;
	ShmInt tickets_aborted2;
	ShmInt times_aborted3;
	ShmInt tickets_aborted3;
	ShmInt times_aborted4;
	ShmInt tickets_aborted4;
	ShmInt times_aborted5;
	ShmInt tickets_aborted5;
	ShmInt times_aborted6;
	ShmInt tickets_aborted6;
	ShmInt times_aborted7;
	ShmInt tickets_aborted7;
	ShmInt times_aborted8;
	ShmInt tickets_aborted8;
	ShmInt times_aborted9;
	ShmInt tickets_aborted9;

	ShmInt last_read_operation;
	int last_read_rslt;

	ShmInt times_read_preempted;
	ShmInt tickets_read_preempted;
	ShmInt times_read_preempted2;
	ShmInt tickets_read_preempted2;
	ShmInt times_read_preempted3;
	ShmInt tickets_read_preempted3;
	ShmInt times_read_repeated;
	ShmInt tickets_read_repeated;
	ShmInt times_read_waited;
	ShmInt tickets_read_waited;
	ShmInt times_read_aborted;
	ShmInt tickets_read_aborted;
} ThreadContextPrivate;

#define SHM_THREAD_MAGIC 0xB6C7

//
// transaction_mode
//
#define TRANSACTION_NONE 0
// Commit after every modification (but preserve references to local variables for the whole duration of transaction?)
#define TRANSACTION_IDLE 1
// Temporary transaction mode for single operation
#define TRANSACTION_TRANSIENT 2
// normal atomic transaction
#define TRANSACTION_PERSISTENT 3
//
// transaction_read_mode
//
#define LOCKING_NONE 0
#define LOCKING_WRITE 1
#define LOCKING_ALL 2

// thread_state
#define THREAD_NORMAL 0
// waiting (for other lock)
#define THREAD_WAITING 1
// locked from outside
#define THREAD_PREEMPTING 2
// successfully waiting_for_lock
#define THREAD_PREEMPTED 3

typedef vl struct ThreadContext_ {
	ShmInt magic;
	ShmPointer self;
	ShmInt index; // ShmReaderBitmap bit
	ThreadContextPrivate *private_data;
	ShmPointer private_data_shm;
	// separate thread_state and thread_preempted so thread_state can be set with simple atomical operation while thread_preempted is changed using CAS
	ShmInt thread_state;
	ShmPointer thread_preempted; // thread that should be signalled once the preemption is finished
	ShmInt always_retry;
	ShmInt transaction_mode; // TRANSACTION_NONE, TRANSACTION_IDLE, TRANSACTION_TRANSIENT, TRANSACTION_PERSISTENT
	ShmInt transaction_lock_mode; // LOCKING_NONE, LOCKING_WRITE, LOCKING_ALL
	// ShmTicks last_start; // the moment when the current transactional block/function was first attempted.
	ShmInt last_start; // ticket instead of ticks
	bool fresh_starts; // update ticket and thread_preempted on every nested traqnsaction start/stop.
	ShmInt test_finished; // set by sweeper to determine the last contemporary transaction has finished
	ShmInt async_mode; // 0 - sync, 1 - async, don't wait for locks but return immediately.

	// ShmFreeList *free_list;
	// ShmPointer free_list_shm;
	ShmPointer free_list; // single linked list-stack, (ShmFreeList *)
	ShmPointer heap;
	LocalReferenceBlock *local_vars; // decouple semiprivate data
	ShmPointer local_vars_shm;

	ShmPointer current_transaction_shm;
	ShmTransactionElement *current_transaction;
	// ShmPointer transaction_stack_shm;
	// ShmTransactionStack *transaction_stack;
	ShmPointer pending_lock; // ShmContainer *
	ShmEvent ready; // currently used for ShmBarrierEvent, "preempted successfully" notification
	// lock queue
	ShmPointer waiting_for_lock;
	ShmPointer next;
} ThreadContext;

int
signal_thread(ThreadContext *self, ShmPointer target);
void
shm_thread_reset_debug_counters(ThreadContext *self);

bool
thread_queue_to_lock(ThreadContext *thread, ShmLock *lock, ShmPointer container_shm);
void
thread_unqueue_from_lock(ThreadContext *thread);

# define SHM_DICT_LIST_BLOCK 1024

// https://stackoverflow.com/questions/1537964/visual-c-equivalent-of-gccs-attribute-packed
#define PACK( __Declaration__ ) __pragma( pack(push, 1) ) __Declaration__ __pragma( pack(pop) )

void
superblock_check_mmap_inited(int group_index);
vl void *
superblock_get_block(int index);
vl void *
superblock_get_block_noside(int index);

int
superblock_alloc_more(ShmPointer thread, int type, vl ShmInt *new_index, int old_index);

// transaction

int
transaction_lock_write(ThreadContext *thread, ShmLock *lock, ShmPointer lock_shm, int container_type, bool *lock_taken);
int
transaction_lock_read(ThreadContext *thread, ShmLock *lock, ShmPointer container_shm, int container_type, bool *lock_taken);

int
transaction_unlock_local(ThreadContext *thread, ShmLock *lock, ShmPointer lock_shm, int status, bool lock_taken);

ShmTransactionElement *
transaction_unlock_all(ThreadContext *thread);

void
shm_types_transaction_end(ThreadContext *thread, ShmTransactionElement *element, bool rollback);
void
shm_types_transaction_unlock(ThreadContext *thread, ShmTransactionElement *element);
void
transient_check_clear(ThreadContext *thread);

int
start_transaction(ThreadContext *thread, int mode, int locking_mode, int is_initial, int *recursion_count);
void
engage_transient(ThreadContext *thread);
void
continue_transaction(ThreadContext *thread);
int
abort_transaction(ThreadContext *thread, int *recursion_count);
int
transaction_length(ThreadContext *thread);
int
abort_transaction_retaining(ThreadContext *thread);
int
abort_transaction_retaining_debug_preempt(ThreadContext *thread);
int
commit_transaction(ThreadContext *thread, int *recursion_count);
int
transient_commit(ThreadContext *thread);
int
transient_abort(ThreadContext *thread);
// End of transaction routines

int
init_thread_context(ThreadContext **context);

ShmWord
shm_pointer_get_block(ShmPointer pntr);

ShmWord
shm_pointer_get_offset(ShmPointer pntr);

bool
shm_pointer_is_valid(ShmPointer pntr);

bool
SBOOL(ShmPointer pntr);

void *
shm_pointer_to_pointer_root(ShmPointer pntr);
void *
shm_pointer_to_pointer_unsafe(ShmPointer pntr);
void *
shm_pointer_to_pointer_no_side(ShmPointer pntr);
void *
shm_pointer_to_pointer(ShmPointer pntr);

ShmPointer
shm_pointer_shift(ShmPointer value, int offset);

#define LOCAL(x)  shm_pointer_to_pointer(x)

// ShmInt is usually 32 bit, while ShmPointer is 32 or 64
ShmPointer
pack_shm_pointer(int offset, int block);
ShmPointer
pointer_to_shm_pointer(void *pntr, int block);

///////////////////////////////////

ShmLock *
shm_cell_to_lock(ShmPointer cell_shm);

bool
init_cell_ref(ShmPointer cell_shm, CellRef *cell);
bool
init_queuecell_ref(ShmPointer cell_shm, QueueCellRef *cell);
bool
init_queue_ref(ShmPointer cell_shm, QueueRef *cell);
bool
init_list_ref(ShmPointer cell_shm, ListRef *cell);
bool
init_dict_ref(ShmPointer cell_shm, DictRef *cell);
bool
init_undict_ref(ShmPointer cell_shm, UnDictRef *cell);

int align_higher(ShmInt value, ShmInt bits);
int align_lower(ShmInt value, ShmInt bits);

// Memory manager
void
init_mm_maps(void);

void *
get_mem(ThreadContext *thread, PShmPointer shm_pointer, int size, int debug_id);

void
free_mem(ThreadContext *thread, ShmPointer shm_pointer, int size);

void
_unallocate_mem(ShmPointer shm_pointer, ShmInt lock_value);

int
mm_block_get_debug_id(vl void *block, ShmPointer block_shm);
vl char *
data_get_flex_block(vl void *data);

vl char *
debug_id_to_str(int id);

#ifndef P_CC_MSVC
	#include "safe_lib.h"
#endif

#define TYPE_DEBUG_ID_STRING_COUNT 31

enum {
	SHM_THREAD_CONTEXT_ID,
	SHM_TRANSACTION_ELEMENT_DEBUG_ID,
	SHM_TYPE_DESC_DEBUG_ID,

	SHM_VALUE_DEBUG_ID,
	SHM_REF_STRING_DEBUG_ID,

	SHM_LIST_DEBUG_ID,
	SHM_LIST_BLOCK_FIRST_DEBUG_ID,
	SHM_LIST_BLOCK_DEBUG_ID,
	SHM_LIST_INDEX_DEBUG_ID,
	SHM_LIST_CHANGES_DEBUG_ID,

	SHM_QUEUE_DEBUG_ID,
	SHM_QUEUE_CELL_DEBUG_ID,
	SHM_QUEUE_CHANGES_DEBUG_ID,

	SHM_DICT_DEBUG_ID,
	SHM_DICT_ELEMENT_ARRAY_DEBUG_ID,
	SHM_DICT_ELEMENT_KEY_DEBUG_ID,
	SHM_DICT_DELTA_ARRAY_DEBUG_ID,

	SHM_UNDICT_DEBUG_ID,
	SHM_UNDICT_TABLE_DEBUG_ID,
	SHM_UNDICT_DELTA_TABLE_DEBUG_ID,
	SHM_UNDICT_TABLE_INDEX_DEBUG_ID,
	SHM_UNDICT_DELTA_TABLE_INDEX_DEBUG_ID,
	SHM_UNDICT_TABLE_BLOCK_DEBUG_ID,
	SHM_UNDICT_DELTA_TABLE_BLOCK_DEBUG_ID,

	SHM_PROMISE_DEBUG_ID,

	PRIVATE_DATA_FREE_LIST2_DEBUG_ID,

	THREAD_LOCAL_VARS_DEBUG_ID,
	VAL_DEBUG_ID,
	EXIT_FLAG_DEBUG_ID,
	TEST_MM_DEBUG_ID,
	TEST_MM_MEDIUM_DEBUG_ID,
};

static inline void
init_debug_type_ids(type_debug_id_list *list)
{
	const char *data[TYPE_DEBUG_ID_STRING_COUNT] = {
		"ThreadContext", // 0
		"ShmTransactionElement",
		"type_desc",

		"ShmValue",
		"ShmRefString",

		"ShmList",
		"ShmListBlock first",
		"ShmListBlock",
		"ShmListIndex",
		"ShmListChanges",

		"ShmQueue", // 10
		"ShmQueueCell",
		"ShmQueueChanges",

		"ShmDict",
		"ShmDictElement array",
		"ShmDictElement->key",
		"ShmDictDeltaArray",

		"ShmUnDict",
		"ShmUnDict table",
		"ShmUnDict delta table",
		"ShmUnDict table index", // 20
		"ShmUnDict delta table index",
		"ShmUnDict table block",
		"ShmUnDict delta table block",

		"ShmPromise",

		"t->private_data->free_list 2",
		"thread->local_vars",
		"val",
		"exit_flag",
		"test_mm",
		"test_mm_medium", // 30
	};
	for (int i = 0; i < TYPE_DEBUG_ID_STRING_COUNT; i++)
	{
		strcpy_s(CAST_VL((*list)[i]), 32, data[i]);
	}
}
// end MM


int
init_superblock(const char *id);
void
release_superblock(bool is_coordinator);

ShmPointer get_pchar_type_desc(ThreadContext *thread);


int
shm_value_get_length(ShmValueHeader *value);
void *
shm_value_get_data(ShmValueHeader *value);

ShmValueHeader *
new_shm_value(ThreadContext *thread, ShmInt item_size, ShmInt type, PShmPointer shm_pointer);

vl void *
new_shm_refcounted_block(ThreadContext *thread, PShmPointer shm_pointer, int total_size, ShmInt type, int debug_id);

void
init_container(ShmContainer *cell, ShmInt size, ShmInt type);

// ShmQueue
ShmQueue *
new_shm_queue(ThreadContext *thread, PShmPointer shm_pointer);


ShmInt
shm_value_get_size(ShmValueHeader *value);

ShmValueHeader *
new_shm_bytes(ThreadContext *thread, const char *value, ShmInt size, PShmPointer result_shm);

ShmValueHeader *
shm_queue_new_value(ThreadContext *thread, QueueRef queue, ShmInt item_size, ShmInt type, PShmPointer shm_pointer);

ShmQueueCell *
shm_queue_new_cell(ThreadContext *thread, QueueRef queue, PShmPointer shm_pointer);

bool
shm_cell_have_write_lock(ThreadContext *thread, ShmLock *lock);
bool
shm_cell_have_read_lock(ThreadContext *thread, ShmLock *lock);
void
shm_cell_check_write_lock(ThreadContext *thread, ShmLock *lock);
void
shm_cell_check_read_lock(ThreadContext *thread, ShmLock *lock);
void
shm_cell_check_read_write_lock(ThreadContext *thread, ShmLock *lock);
void
shm_cell_get_data(ThreadContext *thread, ShmCell *cell, PShmPointer data);
void
shm_queuecell_get_next(ThreadContext *thread, ShmQueueCell *cell, PShmPointer next);

int
shm_cell_commit(ThreadContext *thread, ShmCell *item);
int
shm_cell_rollback(ThreadContext *thread, ShmCell *item);
void
_shm_cell_unlock(ThreadContext *thread, ShmLock *lock, ShmInt type);

int
shm_queue_commit(ThreadContext *thread, ShmQueue *list);
int
shm_queue_rollback(ThreadContext *thread, ShmQueue *list);

int
shm_queue_append_consume(ThreadContext *thread, QueueRef queue, ShmPointer value, CellRef *rslt);
int
shm_queue_append(ThreadContext *thread, QueueRef queue, ShmPointer value, CellRef *rslt);

ShmCell *
shm_queue_acq_first(ThreadContext *thread, QueueRef queue, PShmPointer shm_pointer);
ShmCell *
shm_queue_get_first(ThreadContext *thread, QueueRef queue, PShmPointer shm_pointer);

ShmCell *
shm_queue_cell_acq_next(ThreadContext *thread, ShmCell *item, PShmPointer next_shm);

ShmValueHeader *
cell_unlocked_acq_value(ThreadContext *thread, ShmCell *item, PShmPointer value_shm);

ShmValueHeader *
cell_unlocked_get_value(ThreadContext *thread, ShmCell *item, PShmPointer value_shm);

int
shm_cell_commit(ThreadContext *thread, ShmCell *item);


typedef struct {
	uint32_t hash;
	bool key_owned;
	char *key;
	Py_UCS4 *key_ucs;
	int keysize;
} ShmDictKey;

typedef vl struct {
    SHM_ABSTRACT_BLOCK
    char data;
} ShmDictKeyString;

void
shm_dict_init_key(ShmDictKey *key, const char *s);
void
shm_dict_init_key_consume(ShmDictKey *key, const char *s);
void
shm_dict_free_key(ShmDictKey *key);

// ShmDict
ShmDict *
new_shm_dict(ThreadContext *thread, PShmPointer shm_pointer);

ShmPointer
shm_dict_element_get_value(ShmDictElement *element, bool owned);

typedef struct {
	uint64_t counter1;
	uint64_t counter2;
	uint64_t counter3;
	uint64_t counter4;
	uint64_t counter5;
	uint64_t counter6;
	uint64_t counter7;
} DictProfiling;

int
shm_dict_set_consume(ThreadContext *thread, DictRef dict, ShmDictKey *key,
        ShmPointer value, ShmDictElement **result, DictProfiling *profiling);
int
shm_dict_set_empty(ThreadContext *thread, DictRef dict, ShmDictKey *key, DictProfiling *profiling);

int
shm_dict_get(ThreadContext *thread, DictRef dict, ShmDictKey *key,
	PShmPointer result_value, ShmDictElement **result_element);
int
shm_dict_get_count_debug(ThreadContext *thread, DictRef *dict);
int
shm_dict_get_count(ThreadContext *thread, DictRef dict);
int
dict_nested_count(ShmDictElement *root, bool owned);

int
shm_dict_rollback(ThreadContext *thread, ShmDict *dict);
int
shm_dict_commit(ThreadContext *thread, ShmDict *dict);

/* ShmUndict */

typedef struct {
	ShmPointer key;
	ShmPointer value;
} ShmKeyValue;

ShmUnDict *
new_shm_undict(ThreadContext *thread, PShmPointer shm_pointer);
bool
shm_undict_get_bucket_at_index(ThreadContext *thread, ShmUnDict *dict, int itemindex, ShmPointer *key, ShmPointer *value);
int
shm_undict_consume_item(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key,
		ShmPointer value, DictProfiling *profiling);
int
shm_undict_set_item(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key,
		ShmPointer value, DictProfiling *profiling);
int
shm_undict_set_empty(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key,
		DictProfiling *profiling);
int
shm_undict_set_item_raw(ThreadContext *thread, ShmUnDict *dict, ShmUnDictKey *key, ShmPointer value, bool consume);

volatile char *
shm_undict_debug_scan_for_item(ShmUnDict *dict, ShmUnDictKey *key);
void
print_items_to_file(FILE *pFile, ShmUnDict *dict);
int
_shm_undict_get_count(ThreadContext *thread, UnDictRef dict, ShmInt *rslt, bool commit);
int
shm_undict_get_count(ThreadContext *thread, UnDictRef dict, ShmInt *rslt);
int
shm_undict_get_do(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key, ShmPointer *value, bool acquire);
int
shm_undict_get(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key, ShmPointer *value);
int
shm_undict_acq(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key, ShmPointer *value);
int
shm_undict_clear(ThreadContext *thread, UnDictRef dict);
void
shm_undict_debug_print(ThreadContext *thread, ShmUnDict *dict, bool full);
void
shm_undict_destroy(ThreadContext *thread, ShmUnDict *dict, ShmPointer dict_shm);

void
shm_refcounted_block_destroy(ThreadContext *thread, ShmPointer shm_pointer, ShmRefcountedBlock *obj);
ShmRefcountedBlock *
shm_pointer_to_refcounted(ThreadContext *thread, ShmPointer pointer, bool strict_refcounted, bool strict_check_type);

vl void *
shm_pointer_acq(ThreadContext *thread, ShmPointer pointer);
// dec ref and free
bool
shm_pointer_release(ThreadContext *thread, ShmPointer pointer);
int
shm_pointer_refcount(ThreadContext *thread, ShmPointer pointer);
void
shm_refcounted_block_before_release(ThreadContext *thread, ShmPointer shm_pointer, ShmRefcountedBlock *block);
void
shm_refcounted_block_destroy(ThreadContext *thread, ShmPointer shm_pointer, ShmRefcountedBlock *obj);

LocalReference *
thread_local_ref(ThreadContext *thread, ShmPointer pointer);

void
thread_local_clear_ref(ThreadContext *thread, LocalReference *reference);

void
thread_local_clear_refs(ThreadContext *thread);

// clear the pointer, then decref and release
PShmPointer 
shm_pointer_empty(ThreadContext *thread, PShmPointer pointer);
PShmPointer
shm_pointer_empty_atomic(ThreadContext *thread, PShmPointer pointer);
PShmPointer 
shm_next_pointer_empty(ThreadContext *thread, PShmPointer pointer);

ShmPointer
shm_pointer_copy(ThreadContext *thread, PShmPointer dest, ShmPointer source);

PShmPointer 
shm_pointer_move(ThreadContext *thread, PShmPointer pointer, PShmPointer newval);
PShmPointer
shm_pointer_move_atomic(ThreadContext *thread, PShmPointer pointer, PShmPointer newval);
PShmPointer 
shm_next_pointer_move(ThreadContext *thread, PShmPointer pointer, PShmPointer newval);

#define RESULT_INVALID 0
#define RESULT_OK 1
#define RESULT_WAIT 3
// When the waiter is a registered via thread->preempted, lock->writer_lock (last reader notification),
// or lock->queue (promoted to next_writer after writer_lock release).
#define RESULT_WAIT_SIGNAL 4
#define RESULT_ABORT 5
#define RESULT_PREEMPTED 6
#define RESULT_REPEAT 7
#define RESULT_FAILURE 16

void
init_coordinator(void);
bool
is_coordinator(void);
void
start_coordinator(void);
void
stop_coordinator_and_wait(void);

// ShmEvent.c
#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
ShmHandle
handle_to_coordinator(ShmProcessID coordinator_process, ShmHandle handle, bool close);
ShmHandle
handle_to_private(ShmProcessID coordinator_process, ShmHandle handle);
int
init_private_events(void);
#endif
int
shm_event_init(ShmEvent *event);
int
shm_event_wait(ShmEvent *event, ShmInt milliseconds, bool debug_break);
int
shm_event_sleep_wait(ShmEvent *event, puint milliseconds);
int
shm_event_signal(ShmEvent *event);
int
shm_event_reset(ShmEvent *event);
bool
shm_event_ready(ShmEvent *event);

int
shm_simple_lock_init(ShmSimpleLock *lock);
bool
shm_lock_tryacquire(ShmSimpleLock *lock);
int
shm_lock_acquire_with_cb(ShmSimpleLock *lock, ShmSimpleLockCallback callback, void *callback_data);
int
shm_lock_acquire_maybe(ShmSimpleLock *lock);
int
shm_lock_acquire(ShmSimpleLock *lock);
int
shm_lock_release(ShmSimpleLock *lock, int linenum);
bool
shm_lock_owned(ShmSimpleLock *lock);

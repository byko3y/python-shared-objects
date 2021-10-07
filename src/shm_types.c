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

// #include "assert.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "shm_types.h"
#include "unordered_map.h"

ShmSuperblock *superblock;
ShmChunk superblock_desc;
vl char * superblock_mmap[SHM_BLOCK_COUNT / SHM_BLOCK_GROUP_SIZE];
#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
	ShmPrivateEvents *private_events;
#endif

// static ThreadContext thread_local;

bool
result_is_abort(int result)
{
	return result == RESULT_ABORT || result == RESULT_PREEMPTED;
}

bool
result_is_repeat(int result)
{
	return result == RESULT_REPEAT || result == RESULT_WAIT || result == RESULT_WAIT_SIGNAL;
}

int
shm_cell_unlock(ThreadContext *thread, ShmCell *item, ShmInt type);
int
shm_queue_unlock(ThreadContext *thread, ShmQueue *queue, ShmInt type);
int
shm_list_unlock(ThreadContext *thread, ShmList *list, ShmInt type);
int
shm_dict_unlock(ThreadContext *thread, ShmDict *dict, ShmInt type);
int
shm_undict_unlock(ThreadContext *thread, ShmUnDict *dict, ShmInt type);
int
shm_cell_rollback(ThreadContext *thread, ShmCell *item);
int
shm_cell_commit(ThreadContext *thread, ShmCell *item);
int
shm_queue_rollback(ThreadContext *thread, ShmQueue *queue);
int
shm_queue_commit(ThreadContext *thread, ShmQueue *queue);
int
shm_list_rollback(ThreadContext *thread, ShmList *list);
int
shm_list_commit(ThreadContext *thread, ShmList *list);
int
shm_dict_rollback(ThreadContext *thread, ShmDict *dict);
int
shm_dict_commit(ThreadContext *thread, ShmDict *dict);
int
shm_undict_commit(ThreadContext *thread, ShmUnDict *dict);
int
shm_undict_rollback(ThreadContext *thread, ShmUnDict *dict);

void
shm_types_transaction_end(ThreadContext *thread, ShmTransactionElement *element, bool rollback)
{
	switch (element->container_type)
	{
	case CONTAINER_NONE:
		break;
	case CONTAINER_CELL:
	{
		CellRef cell;
		init_cell_ref(element->container, &cell);
		if (rollback)
			shm_cell_rollback(thread, cell.local);
		else
			shm_cell_commit(thread, cell.local);
		break;
	}
	case CONTAINER_QUEUE:
	{
		QueueRef queue;
		init_queue_ref(element->container, &queue);
		if (rollback)
			shm_queue_rollback(thread, queue.local);
		else
			shm_queue_commit(thread, queue.local);
		break;
	}
	case CONTAINER_LIST:
	{
		ListRef list;
		init_list_ref(element->container, &list);
		if (rollback)
			shm_list_rollback(thread, list.local);
		else
			shm_list_commit(thread, list.local);
		break;
	}
	case CONTAINER_DICT_DELTA:
	{
		DictRef dict;
		init_dict_ref(element->container, &dict);
		if (rollback)
			shm_dict_rollback(thread, dict.local);
		else
			shm_dict_commit(thread, dict.local);
		break;
	}
	case CONTAINER_UNORDERED_DICT:
	{
		UnDictRef undict;
		init_undict_ref(element->container, &undict);
		if (rollback)
			shm_undict_rollback(thread, undict.local);
		else
			shm_undict_commit(thread, undict.local);
		break;
	}
	case CONTAINER_PROMISE:
	{
		ShmPromise *promise = LOCAL(element->container);
		if (rollback)
			shm_promise_rollback(thread, promise);
		else
			shm_promise_commit(thread, promise);
		break;
	}
	default:
	{
		char buf[40];
		snprintf(buf, 40, "transaction_end: element is invalid %d", element->container_type);
		shmassert_msg(false, buf);
	}
	}
}

void
shm_types_transaction_unlock(ThreadContext *thread, ShmTransactionElement *element)
{
	switch (element->container_type)
	{
	case CONTAINER_NONE:
		break;
	case CONTAINER_CELL:
	{
		CellRef cell;
		init_cell_ref(element->container, &cell);
		shm_cell_unlock(thread, cell.local, element->type);
		break;
	}
	case CONTAINER_QUEUE:
	{
		QueueRef queue;
		init_queue_ref(element->container, &queue);
		shm_queue_unlock(thread, queue.local, element->type);
		break;
	}
	case CONTAINER_LIST:
	{
		ListRef list;
		init_list_ref(element->container, &list);
		shm_list_unlock(thread, list.local, element->type);
		break;
	}
	case CONTAINER_DICT_DELTA:
	{
		DictRef dict;
		init_dict_ref(element->container, &dict);
		shm_dict_unlock(thread, dict.local, element->type);
		break;
	}
	case CONTAINER_UNORDERED_DICT:
	{
		UnDictRef undict;
		init_undict_ref(element->container, &undict);
		shm_undict_unlock(thread, undict.local, element->type);
		break;
	}
	case CONTAINER_PROMISE:
	{
		ShmPromise *promise = LOCAL(element->container);
		shm_promise_unlock(thread, promise, element->type);
		break;
	}
	default:
		shmassert_msg(false, "transaction_end: element is invalid");
	}
}

// ShmCell

ShmLock *
shm_cell_to_lock(ShmPointer cell_shm)
{
	ShmCell *cell = LOCAL(cell_shm);
	if (cell)
	{
		return &cell->lock;
	}
	else
		return NULL;
}

bool
init_cell_ref(ShmPointer cell_shm, CellRef *cell)
{
	cell->shared = cell_shm;
	cell->local = LOCAL(cell_shm);
	return !!cell->local;
}

bool
init_queuecell_ref(ShmPointer cell_shm, QueueCellRef *cell)
{
	return init_cell_ref(cell_shm, (CellRef *) cell);
}

bool
init_queue_ref(ShmPointer cell_shm, QueueRef *cell)
{
	return init_cell_ref(cell_shm, (CellRef *) cell);
}

bool
init_list_ref(ShmPointer cell_shm, ListRef *cell)
{
	return init_cell_ref(cell_shm, (CellRef *)cell);
}

bool
init_dict_ref(ShmPointer cell_shm, DictRef *cell)
{
	return init_cell_ref(cell_shm, (CellRef *)cell);
}

bool
init_undict_ref(ShmPointer cell_shm, UnDictRef *cell)
{
	return init_cell_ref(cell_shm, (CellRef *)cell);
}

////////////////////////////////////  Types  /////////////////////

static ShmPointer pchar_type_desc = EMPTY_SHM;

ShmPointer get_pchar_type_desc(ThreadContext *thread)
{
	if (shm_pointer_is_valid(pchar_type_desc))
		return pchar_type_desc;
	// ShmPointer id = 1;
	// if (thread)
	// 	id = thread->self;
	if (get_mem(thread, &pchar_type_desc, sizeof(ShmPointer), SHM_TYPE_DESC_DEBUG_ID))
		return pchar_type_desc;
	else
		return EMPTY_SHM;
}

int
shm_value_get_length(ShmValueHeader *value)
{
	shmassert(value->size > isizeof(ShmValueHeader));
	return value->size - isizeof(ShmValueHeader);
}

void *
shm_value_get_data(ShmValueHeader *value)
{
	return (void *)(((uintptr_t)value) + sizeof(ShmValueHeader));
}

void
init_container(ShmContainer *cell, ShmInt size, ShmInt type)
{
	shmassert((type & SHM_TYPE_FLAG_REFCOUNTED) == SHM_TYPE_FLAG_REFCOUNTED);
	shmassert((type & SHM_TYPE_FLAG_MUTABLE) == SHM_TYPE_FLAG_MUTABLE);
	memset(CAST_VL(cell), 0x1B, (puint)size);
	cell->refcount = 1;
	cell->revival_count = 0;
	cell->release_count = 0;
	cell->size = size;
	cell->type = type;
	cell->lock.reader_lock = 0;
	// cell->lock.rw_barrier_thread = EMPTY_SHM;
	// cell->lock.rw_barrier_value = 0;
	// cell->lock.writer_lock_ensured = 0;
	cell->lock.writer_lock = LOCK_UNLOCKED;
	// cell->lock.next_writer = 0;
	cell->lock.next_writer = LOCK_UNLOCKED;
	// cell->lock.queue = NONE_SHM;
	cell->lock.queue_threads = 0;
	cell->lock.transaction_data = EMPTY_SHM;
	cell->lock.prev_lock = EMPTY_SHM;
	cell->lock.release_line = __LINE__;
	cell->lock.writers_count = 0;
	cell->lock.readers_count = 0;
}

////////////////////////////////////  Queue  ///////////////////////

static void
init_cell(ShmCell *cell, ShmInt size, ShmInt type)
{
	init_container((ShmContainer *)cell, size, type);
	cell->data = EMPTY_SHM;
	cell->has_new_data = false;
	cell->new_data = EMPTY_SHM;
}

// data is ShmInt-aligned
vl void *
new_shm_refcounted_block(ThreadContext *thread, PShmPointer shm_pointer, int total_size, ShmInt type, int debug_id)
{
	shmassert((type & SHM_TYPE_FLAG_REFCOUNTED) == SHM_TYPE_FLAG_REFCOUNTED);
	ShmRefcountedBlock *new_block = get_mem(thread, shm_pointer, total_size, debug_id);
	memclear(new_block, total_size);
	new_block->type = type;
	new_block->size = total_size;
	new_block->refcount = 1;
	new_block->revival_count = 0;
	new_block->release_count = 0;
	return new_block;
}

// data is ShmInt-aligned
ShmValueHeader *
new_shm_value(ThreadContext *thread, ShmInt item_size, ShmInt type, PShmPointer shm_pointer)
{
	if (shm_pointer) *shm_pointer = EMPTY_SHM;
	int total_size = item_size + isizeof(ShmValueHeader);
	ShmValueHeader *value = new_shm_refcounted_block(thread, shm_pointer, total_size, type | SHM_TYPE_FLAG_REFCOUNTED, SHM_VALUE_DEBUG_ID);
	return value;
}

ShmInt
shm_value_get_size(ShmValueHeader *value)
{
	shmassert_msg(value->size >= isizeof(ShmValueHeader), "value->size >= sizeof(ShmValueHeader)");
	if (value->size >= isizeof(ShmValueHeader))
		return value->size - isizeof(ShmValueHeader);
	else
		return 0;
}

ShmValueHeader *
new_shm_bytes(ThreadContext *thread, const char *value, ShmInt size, PShmPointer result_shm)
{
	// ShmValueHeader *rslt = new_shm_value(thread, size + 1, SHM_TYPE_BYTES, result_shm);
	ShmValueHeader *rslt = new_shm_value(thread, size, SHM_TYPE_BYTES, result_shm);
	if (!shm_value_get_data(rslt))
	{
		shmassert(false);
		return NULL;
	}
	// strncpy_s(shm_value_get_data(rslt), (size_t)size + 1, value, (size_t)size);
	memcpy(shm_value_get_data(rslt), value, size);
	return rslt;
}

void
shm_cell_get_data(ThreadContext *thread, ShmCell *cell, PShmPointer data)
{
	if (shm_cell_have_write_lock(thread, &cell->lock))
	{
		// cell is locked by me
		if (cell->has_new_data)
			*data = cell->new_data;
		else
			*data = cell->data;
	}
	else
		*data = cell->data;
}

int
shm_cell_commit(ThreadContext *thread, ShmCell *item)
{
	shm_cell_check_write_lock(thread, &item->lock);
	if (item->has_new_data)
	{
		item->has_new_data = false;
		shm_pointer_move(thread, &item->data, &item->new_data);
	}
	return RESULT_OK;
}

int
shm_cell_rollback(ThreadContext *thread, ShmCell *item)
{
	shm_cell_check_write_lock(thread, &item->lock);
	if (item->has_new_data)
	{
		item->has_new_data = false;
		shm_pointer_empty(thread, &item->new_data);
	}
	return RESULT_OK;
}

int
shm_cell_unlock(ThreadContext *thread, ShmCell *item, ShmInt type)
{
	// shmassert(item->lock.id == thread->self, "item->lock.id == thread->self");
	if (TRANSACTION_ELEMENT_WRITE == type)
		p_atomic_shm_pointer_set(&item->lock.transaction_data, EMPTY_SHM);
	_shm_cell_unlock(thread, &item->lock, type);
	return RESULT_OK;
}

ShmValueHeader *
do_cell_get_value(ThreadContext *thread, ShmCell *item, PShmPointer value_shm, bool acquire)
{
	if (value_shm) *value_shm = EMPTY_SHM;
	if (!item)
		return NULL;
	ShmPointer data;
	shm_cell_get_data(thread, item, &data);
	if (value_shm)
		*value_shm = data;
	if (!shm_pointer_is_valid(data) && data != NONE_SHM)
		return NULL;

	ShmValueHeader *value = (ShmValueHeader *)LOCAL(data);
	if (acquire)
		p_atomic_int_inc(&value->refcount);

	return value;
}

// Returns acquired value.
// Lock is not needed because item is already acquired and result is immutable.
ShmValueHeader *
cell_unlocked_acq_value(ThreadContext *thread, ShmCell *item, PShmPointer value_shm)
{
	return do_cell_get_value(thread, item, value_shm, true);
}

ShmValueHeader *
cell_unlocked_get_value(ThreadContext *thread, ShmCell *item, PShmPointer value_shm)
{
	return do_cell_get_value(thread, item, value_shm, false);
}

// ShmRefString

void
ASCII_from_UCS4(Py_UCS1 *to, const Py_UCS4 *from, ShmInt length)
{
	for (int i = 0; i < length; ++i)
	{
		to[i] = (Py_UCS1)from[i];
		shmassert(from[i] != 0);
	}
}

void
UCS4_from_ASCII(Py_UCS4 *to, const Py_UCS1 *from, ShmInt length)
{
	for (int i = 0; i < length; ++i)
	{
		to[i] = from[i];
		shmassert(from[i] != 0);
	}
}

static const unsigned char number_map[10] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

int
UCS4_format_number(Py_UCS4 *to, int number)
{
	int len = 0;
	if (number <= 0)
	{
		to[0] = '0';
		return 1;
	}
	int tmp = number;
	while (tmp > 0)
	{
		tmp = tmp / 10;
		len++;
	}
	for (int i = len - 1; i >= 0; --i)
	{
		to[i] = number_map[number % 10];
		number = number / 10;
	}
	return len;
}

ShmPointer
shm_ref_unicode_new_ascii(ThreadContext *thread, const Py_UCS1 *s, int len)
{
	ShmPointer rslt;
	int total_size = isizeof(ShmRefUnicode) - isizeof(Py_UCS4) + len * isizeof(Py_UCS4);
	ShmRefUnicode *ref = new_shm_refcounted_block(thread, &rslt, total_size, SHM_TYPE_REF_UNICODE, SHM_REF_STRING_DEBUG_ID);
	if (ref)
	{
		Py_UCS4 *data = (Py_UCS4 *)(intptr_t)&ref->chars;
		UCS4_from_ASCII(data, s, len);
	}
	return rslt;
}

ShmPointer
shm_ref_unicode_new(ThreadContext *thread, const Py_UCS4 *s, int len)
{
	ShmPointer rslt;
	int total_size = isizeof(ShmRefUnicode) - isizeof(Py_UCS4) + len * isizeof(Py_UCS4);
	ShmRefUnicode *ref = new_shm_refcounted_block(thread, &rslt, total_size, SHM_TYPE_REF_UNICODE, SHM_REF_STRING_DEBUG_ID);
	if (ref && s)
	{
		Py_UCS4 *data = (Py_UCS4 *)&ref->chars;
		memcpy(data, s, len * sizeof(Py_UCS4));
	}
	return rslt;
}

RefUnicode
shm_ref_unicode_get(ShmPointer p)
{
	RefUnicode rslt;
	ShmRefUnicode *s = LOCAL(p);
	if (!s)
	{
		rslt.data = NULL;
		rslt.len = 0;
		return rslt;
	}
	shmassert(SHM_TYPE(s->type) == SHM_TYPE(SHM_TYPE_REF_UNICODE));
	int strlen = (s->size - (isizeof(ShmRefUnicode) - isizeof(Py_UCS4))) / isizeof(Py_UCS4);
	rslt.len = strlen;
	rslt.data = &s->chars;
	return rslt;
}

// FNV_prime = 2^24 + 2^8 + 0x93 = 16777619
#define FNV_prime  16777619
#define FNV_basis  2166136261

// FNV-1a hash
uint32_t
hash_string(const Py_UCS4 *s, int len)
{
	uint32_t hash = FNV_basis;
	for (int i = 0; i < len; ++i)
	{
		hash = hash ^ s[i];
		hash = hash * FNV_prime;
	}
	return hash;
}

uint32_t
hash_string_ascii(const char *s, int len)
{
	uint32_t hash = FNV_basis;
	for (int i = 0; i < len; ++i)
	{
		hash = hash ^ (unsigned char)s[i];
		hash = hash * FNV_prime;
	}
	return hash;
}

ShmValueHeader *
new_shm_unicode_value(ThreadContext *thread, const char *s, int length, PShmPointer shm_pointer)
{
	ShmValueHeader *header = new_shm_value(thread, length * isizeof(Py_UCS4), SHM_TYPE_UNICODE, shm_pointer);
	Py_UCS4 *data = (Py_UCS4 *)shm_value_get_data(header);
	UCS4_from_ASCII(data, (const Py_UCS1 *)s, length);
	return header;
}

// ShmQueue

ShmQueue *
new_shm_queue(ThreadContext *thread, PShmPointer shm_pointer)
{
	// if (shm_pointer) *shm_pointer = EMPTY_SHM; - done by get_mem
	ShmQueue *queue = (ShmQueue *)get_mem(thread, shm_pointer, sizeof(ShmQueue), SHM_QUEUE_DEBUG_ID);
	init_container((ShmContainer *)queue, sizeof(ShmQueue), SHM_TYPE_QUEUE);
	queue->head = EMPTY_SHM;
	queue->new_head = NONE_SHM;
	queue->tail = EMPTY_SHM;
	queue->new_tail = NONE_SHM;
	queue->count = 0;
	queue->type_desc = get_pchar_type_desc(thread);
	queue->changes_shm = EMPTY_SHM;
	return queue;
}

// Allocate new value for the queue. Operates on detached objects, thus does not block the queue.
// Returns private pointer to the new item (shared shm_pointer)
ShmValueHeader *
shm_queue_new_value(ThreadContext *thread, QueueRef queue, ShmInt item_size, ShmInt type, PShmPointer shm_pointer)
{
	return new_shm_value(thread, item_size, type, shm_pointer);
}

ShmQueueCell *
shm_queue_new_cell(ThreadContext *thread, QueueRef queue, PShmPointer shm_pointer)
{
	if (shm_pointer) *shm_pointer = EMPTY_SHM;
	int total_size = sizeof(ShmQueueCell);
	ShmQueueCell *new_block = (ShmQueueCell *)get_mem(thread, shm_pointer, total_size, SHM_QUEUE_CELL_DEBUG_ID);
	memclear(new_block, total_size);
	init_cell((ShmCell *)new_block, total_size, SHM_TYPE_QUEUE_CELL);
	new_block->next = EMPTY_SHM;
	new_block->new_next = NONE_SHM;
	return new_block;
}

void
shm_queuecell_get_next(ThreadContext *thread, ShmQueueCell *cell, PShmPointer next)
{
	if (shm_cell_have_write_lock(thread, &cell->lock))
	{
		// cell is locked by me
		if (cell->new_next == NONE_SHM)
			*next = cell->next;
		else
			*next = cell->new_next;
	}
	else
		*next = cell->next;
}

int
shm_queue_changes_check_inited(ThreadContext *thread, ShmQueue *queue, ShmQueueChanges **rslt)
{
	if ( ! SBOOL(queue->changes_shm) )
	{
		ShmPointer shm_pointer;
		ShmQueueChanges *new = (ShmQueueChanges *)get_mem(thread, &shm_pointer, sizeof(ShmQueueChanges), SHM_QUEUE_CHANGES_DEBUG_ID);
		new->type = SHM_TYPE_QUEUE_CHANGES;
		new->size = sizeof(ShmQueueChanges);
		new->count = 0;
		queue->changes_shm = shm_pointer; // publish
		if (rslt)
			*rslt = new;
		return RESULT_OK;
	}
	else if (rslt)
	{
		*rslt = LOCAL(queue->changes_shm);
	}
	return RESULT_OK;
}

int
shm_queue_changes_push(ThreadContext *thread, QueueRef queue, QueueCellRef cell)
{
	if (SBOOL(p_atomic_shm_pointer_get(&cell.local->lock.transaction_data)))
		return RESULT_OK;
	ShmQueueChanges *changes = NULL;
	shm_queue_changes_check_inited(thread, queue.local, &changes);
	shmassert_msg(changes->count < DELTA_ARRAY_SIZE, "changes->count < DELTA_ARRAY_SIZE");
	int idx = changes->count;
	changes->cells[idx] = cell.shared;
	changes->count++;
	p_atomic_shm_pointer_set(&cell.local->lock.transaction_data, queue.shared);
	return RESULT_OK;
}

int
shm_queue_changes_clear(ThreadContext *thread, ShmQueue *queue)
{
	ShmQueueChanges *changes = LOCAL(queue->changes_shm);
	if (changes)
	{
		changes->count = 0;
	}
	return RESULT_OK;
}

int
shm_queue_commit(ThreadContext *thread, ShmQueue *queue)
{
	shm_cell_check_write_lock(thread, &queue->base.lock);
	// order of operations in this function is not verified for dirty reads.
	ShmQueueChanges *changes = LOCAL(queue->changes_shm);
	for (int i = 0; i < changes->count; ++i)
	{
		ShmQueueCell *cell = LOCAL(changes->cells[i]);
		if (cell->has_new_data)
		{
			cell->has_new_data = false; // leak is better than wild pointer
			shm_pointer_move(thread, &cell->data, &cell->new_data);
		}
		if (cell->new_next != NONE_SHM)
		{
			shm_next_pointer_move(thread, &cell->next, &cell->new_next);
		}
	}

	if (queue->new_tail != NONE_SHM)
	{
		queue->tail = queue->new_tail;
		queue->new_tail = NONE_SHM;
	}
	if (queue->new_head != NONE_SHM)
	{
		shm_next_pointer_move(thread, &queue->head, &queue->new_head);
	}
	return RESULT_OK;
}

int
shm_queue_rollback(ThreadContext *thread, ShmQueue *queue)
{
	shm_cell_check_write_lock(thread, &queue->base.lock);
	// order of operations in this function is not verified for dirty reads.
	ShmQueueChanges *changes = LOCAL(queue->changes_shm);
	// changes are not implemented in the shm_queue_append_do
	for (int i = 0; i < changes->count; ++i)
	{
		ShmQueueCell *cell = LOCAL(changes->cells[i]);
		if (cell->has_new_data)
		{
			cell->has_new_data = false; // leak is better than wild pointer
			shm_pointer_empty(thread, &cell->new_data);
		}
		if (cell->new_next != NONE_SHM)
		{
			shm_next_pointer_empty(thread, &cell->new_next);
		}
	}

	if (queue->new_tail != NONE_SHM)
	{
		// tail is not refcounted
		queue->new_tail = NONE_SHM;
	}
	if (queue->new_head != NONE_SHM)
	{
		shm_next_pointer_empty(thread, &queue->new_head);
	}
	return RESULT_OK;
}

int
shm_queue_unlock(ThreadContext *thread, ShmQueue *queue, ShmInt type)
{
	// order of operations in this function is not verified for dirty reads.
	
	// locks are global for the whole queue now
	// ShmLueueChanges *changes = LOCAL(queue->changes);
	// for (i = 0; i < changes->count; ++i)
	// {
	//	ShmCell *cell = LOCAL(changes->cells[i]);
	//	shm_cell_unlock(thread, cell);
	//}
	shm_queue_changes_clear(thread, queue);

	// shmassert(queue->base.lock.id == thread->self, "queue->lock.id == thread->self");
	if (TRANSACTION_ELEMENT_WRITE == type)
		p_atomic_shm_pointer_set(&queue->base.lock.transaction_data, EMPTY_SHM);
	// queue->base.lock.id = 0;
	_shm_cell_unlock(thread, &queue->base.lock, type);
	return RESULT_OK;
}

// rslt is unacquired, but owned by ShmQueue
int
shm_queue_append_do(ThreadContext *thread, QueueRef queue, ShmPointer value, CellRef *rslt, bool consume)
{
	shm_queue_changes_check_inited(thread, queue.local, NULL);
	rslt->shared = EMPTY_SHM;
	//if (InterlockedCompareExchange(&queue->cell.lock, 1, 0) == 0)
	// take_spinlock(&queue->cell.lock, {}); // modification lock
	// start_transaction(thread, TRANSACTION_TRANSIENT, LOCKING_WRITE, false, NULL);
	bool lock_taken1 = false;
	if_failure(
		transaction_lock_write(thread, &queue.local->base.lock, queue.shared, CONTAINER_QUEUE, &lock_taken1),
		{
			if (consume)
				shm_pointer_release(thread, value);
			transient_abort(thread);
			return status;
		}
	);
	{
		QueueCellRef tail;
		if (queue.local->new_tail != NONE_SHM)
			init_queuecell_ref(queue.local->new_tail, &tail);
		else
			init_queuecell_ref(queue.local->tail, &tail);
		// bool lock_taken2 = false;
		//if (init_queuecell_ref(queue.local->tail, &tail))
		//{
		//	if_failure(transaction_lock_write(thread, &tail.local->cell.lock, tail.shared, CONTAINER_CELL, &lock_taken2),
		//		{
		//			transaction_unlock_local(thread, &queue.local->cell.lock, queue.shared, status, lock_taken1);
		//			return status;
		//		}
		//	);
		// // now tail and head are both locked
		//}
		// new implementation locks the whole queue by locking the ShmQueue
		ShmCell *new_cell = (ShmCell *)shm_queue_new_cell(thread, queue, &rslt->shared);
		if ( ! consume )
            shm_pointer_acq(thread, value);
		new_cell->data = value; // it's local, so we might store it in new_cell->data
        bool rslt_consumed = false;

		if (tail.local)
		{
			shmassert( ! shm_pointer_is_valid(tail.local->next));
			shm_queue_changes_push(thread, queue, tail);
			shm_pointer_empty(thread, &tail.local->new_next);
			tail.local->new_next = rslt->shared; // ensure the tail does not point to unlinked item before becoming visible
			rslt_consumed = true;
		}

		// queue.local->tail = rslt->shared; // now publish the tail item
		queue.local->new_tail = rslt->shared;

		// InterlockedIncrement(&queue.local->count); // increment last on addition, decrement first on removal
		queue.local->count++; // queue is locked
		QueueCellRef head;
		if (queue.local->new_head != NONE_SHM)
			init_queuecell_ref(queue.local->new_head, &head);
		else
			init_queuecell_ref(queue.local->head, &head);
	shmassert((tail.local != NULL) == (head.local != NULL));
		// if (!shm_pointer_is_valid(queue.local->head))
		if ( ! head.local )
		{
			// add a new head
			// assert( ! shm_pointer_is_valid(queue.local->cell.new_head));
			shm_pointer_empty(thread, &queue.local->new_head); // empty queue becomes valid here, so we publish it last, when all the structures are ready
			queue.local->new_head = rslt->shared; // consume
			shmassert(rslt_consumed == false);
			rslt_consumed = true;
		}
		//if (tail.local)
		//	transaction_unlock_local(thread, &tail.local->cell.lock, tail.shared, RESULT_OK, lock_taken2);

		// commit
		//if (thread_transaction_mode == TRANSACTION_TRANSIENT && lock_taken1)
		//	shm_queue_commit(thread, queue);
		//int status = transaction_unlock_local(thread, &queue.local->cell.lock, queue.shared, RESULT_OK, lock_taken1);
		// Should register the changes with queue->changes_shm here
		transient_commit(thread);
		rslt->local = new_cell;
		return RESULT_OK;
	};
}

// Append new cells, pointing to the existing "value" (value->refcount == 1).
// Takes ownership over "value".
// rslt is a private pointer to the new cell (shared "new_cell_shm").
// Returns a regular RESULT_XX status.
int
shm_queue_append_consume(ThreadContext *thread, QueueRef queue, ShmPointer value, CellRef *rslt)
{
	return shm_queue_append_do(thread, queue, value, rslt, true);
}

int
shm_queue_append(ThreadContext *thread, QueueRef queue, ShmPointer value, CellRef *rslt)
{
	return shm_queue_append_do(thread, queue, value, rslt, false);
}

// Acquires the first cell in the queue
ShmCell *
do_shm_queue_get_first(ThreadContext *thread, QueueRef queue, PShmPointer shm_pointer, bool acquire)
{
	if (shm_pointer) *shm_pointer = EMPTY_SHM;
	ShmPointer data = EMPTY_SHM;
	if (shm_cell_have_write_lock(thread, &queue.local->base.lock))
	{
		// cell is locked by me
		if (SBOOL(queue.local->new_head))
			data = queue.local->new_head;
		else
			data = queue.local->head;
	}
	else
		data = queue.local->head;

	if (!shm_pointer_is_valid(data))
		return NULL;
	//while (1)
	{
		// queue->head might be released during our operation
		//if (InterlockedCompareExchange(&queue->cell.lock, 1, 0) == 0)
		// take_spinlock(&queue->cell.lock, {}); - not needed with memory releaser(GC)
		{
			if (shm_pointer_is_valid(data))
			{
				if (shm_pointer)
					*shm_pointer = data;
				ShmCell *item = (ShmCell *)LOCAL(data);
				if (acquire)
					p_atomic_int_inc(&item->refcount);
				// release_spinlock(&queue->cell.lock);
				return item;
			}
			else
			{
				// release_spinlock(&queue->cell.lock);
				return NULL;
			}
		}
	}
}

ShmCell *
shm_queue_acq_first(ThreadContext *thread, QueueRef queue, PShmPointer shm_pointer)
{
	return do_shm_queue_get_first(thread, queue, shm_pointer, true);
}

ShmCell *
shm_queue_get_first(ThreadContext *thread, QueueRef queue, PShmPointer shm_pointer)
{
	return do_shm_queue_get_first(thread, queue, shm_pointer, false);
}

ShmCell *
do_shm_queue_cell_get_next(ThreadContext *thread, ShmCell *item, PShmPointer next_shm, bool acquire)
{
	if (next_shm) *next_shm = EMPTY_SHM;
	if (!item)
		return NULL;
	ShmQueueCell *queue_item = (ShmQueueCell *)item;
	if (!shm_pointer_is_valid(queue_item->next))
		return NULL;
	//while (1)
	{
		//if (InterlockedCompareExchange(&queue_item->cell.lock, 1, 0) == 0)
		//take_spinlock(&queue_item->cell.lock, {}); - not needed with memory releaser(GC)
		{
			// Double check under lock.
			// The next item cannot be unlinked without locking this item, so queue_item->next->refcount >= 1.
			if (shm_pointer_is_valid(queue_item->next))
			{
				if (next_shm)
					*next_shm = queue_item->next;
				ShmCell *next_item = (ShmCell *)LOCAL(queue_item->next);
				if (acquire)
					p_atomic_int_inc(&next_item->refcount);
				//release_spinlock(&queue_item->cell.lock);
				return next_item;
			}
			else
			{
				//release_spinlock(&queue_item->cell.lock);
				return NULL;
			}
		}
	}
}

// "item" shall be already acquired, so it won't be released, and we can safely lock it to step.
// Returns acquired next cell.
ShmCell *
shm_queue_cell_acq_next(ThreadContext *thread, ShmCell *item, PShmPointer next_shm)
{
	return do_shm_queue_cell_get_next(thread, item, next_shm, true);
}

ShmCell *
shm_queue_cell_get_next(ThreadContext *thread, ShmCell *item, PShmPointer next_shm)
{
	return do_shm_queue_cell_get_next(thread, item, next_shm, false);
}

// end ShmQueue

// ///////////////////////////////////////////////////////////////////////

// ShmList

ShmList *
new_shm_list(ThreadContext *thread, PShmPointer result)
{
	ShmList *list = get_mem(thread, result, sizeof(ShmList), SHM_LIST_DEBUG_ID);
	init_container((ShmContainer *)list, sizeof(ShmList), SHM_TYPE_LIST);
	list->top_block = EMPTY_SHM;
	list->count = 0;
	list->new_count = -1;
	list->deleted = 0;
	list->new_deleted = -1;
	list->type_desc = get_pchar_type_desc(thread);
	list->changes_shm = EMPTY_SHM;
	list->inited = 0;
	return list;
}

int
shm_list_changes_check_inited(ThreadContext *thread, ShmList *list, ShmListChanges **rslt)
{
	if (!SBOOL(list->changes_shm))
	{
		ShmPointer shm_pointer;
		ShmListChanges *new = (ShmListChanges *)get_mem(thread, &shm_pointer, sizeof(ShmListChanges), SHM_LIST_CHANGES_DEBUG_ID);
		new->type = SHM_TYPE_LIST_CHANGES;
		new->size = sizeof(ShmListChanges);
		new->count = 0;
		list->changes_shm = shm_pointer; // publish
		if (rslt)
			*rslt = new;
		return RESULT_OK;
	}
	else if (rslt)
	{
		*rslt = LOCAL(list->changes_shm);
	}
	return RESULT_OK;
}

int
shm_list_changes_push(ThreadContext *thread, ShmList *list, ShmListBlock *block, ShmInt block_index, ShmInt index)
{
	if (block->cells[index].changed)
		return RESULT_OK;
	ShmListChanges *changes = NULL;
	shm_list_changes_check_inited(thread, list, &changes);
	shmassert(changes->count < DELTA_ARRAY_SIZE);
	if (changes->count >= DELTA_ARRAY_SIZE)
		return RESULT_FAILURE;
	int idx = changes->count;
	changes->cells[idx].block_index = block_index;
	changes->cells[idx].item_index = index;
	changes->count++;
	block->cells[index].changed = true;
	return RESULT_OK;
}

int
shm_list_changes_clear(ThreadContext *thread, ShmList *list)
{
	ShmListChanges *changes = LOCAL(list->changes_shm);
	if (changes)
	{
		changes->count = 0;
	}
	return RESULT_OK;
}

ShmListCounts
shm_list_block_get_count(ShmListBlock *block, bool owned)
{
	ShmListCounts result = SHM_LIST_INVALID_COUNTS;

	if (owned && block->new_count != -1)
		result.count = block->new_count;
	else
		result.count = block->count;

	if (owned && block->new_deleted != -1)
		result.deleted = block->new_deleted;
	else
		result.deleted = block->deleted;

	return result;
}

ShmListCounts
shm_list_index_get_count(ShmListIndexItem *item, bool owned)
{
	ShmListCounts rslt = { .count = item->count, .deleted = item->deleted };
	if (owned && item->new_count != -1)
		rslt.count = item->new_count;
	if (owned && item->new_deleted != -1)
		rslt.deleted = item->new_deleted;
	return rslt;
}

ShmListCounts
shm_list_get_fast_count(ThreadContext *thread, ShmList *list, bool owned)
{
	ShmListCounts rslt = { .count = p_atomic_int_get(&list->count), .deleted = p_atomic_int_get(&list->deleted) };
	if (owned)
	{
		ShmInt new_count = p_atomic_int_get(&list->new_count);
		ShmInt new_deleted = p_atomic_int_get(&list->new_deleted);
		if (new_count != -1)
			rslt.count = new_count;
		if (new_deleted != -1)
			rslt.deleted = new_deleted;
	}
	return rslt;
}

ShmPointer
shm_list_cell_get_data(ShmListCell *cell, bool owned)
{
	if (owned && cell->has_new_data)
		return cell->new_data;
	else
		return cell->data;
}
void
shm_list_block_verify_clean(ShmListBlock *block)
{
	if (block)
	{
		shmassert(block->new_count == -1);
		shmassert(block->new_deleted == -1);
		shmassert(block->refcount > 0);
		shmassert(block->capacity >= block->count + block->deleted);
		shmassert(block->capacity >= block->count);
	}
}

int
shm_list_get_count(ThreadContext *thread, ListRef list, ShmListCounts *result, bool debug)
{
	transient_check_clear(thread);
	if_failure(
		transaction_lock_read(thread, &list.local->base.lock, list.shared, CONTAINER_LIST, NULL),
		{
			transient_abort(thread);
			return status;
		}
	);
	shm_cell_check_read_write_lock(thread, &list.local->base.lock);
	bool owned = shm_cell_have_write_lock(thread, &list.local->base.lock);

	ShmRefcountedBlock *top_block = LOCAL(list.local->top_block);
	ShmListCounts collected_counts = { 0, 0 };
	if (top_block == NULL)
		; // zero
	else if (SHM_TYPE_LIST_BLOCK == top_block->type)
	{
		if (debug)
		{
			ShmListBlock *block = (ShmListBlock *)top_block;
			ShmListCounts cnts = shm_list_block_get_count(block, owned);
			collected_counts.count = cnts.count;
			collected_counts.deleted = cnts.deleted;
		}
	}
	else if (SHM_TYPE_LIST_INDEX == top_block->type)
	{
		ShmListIndex *index_block = (ShmListIndex *)top_block;
		if (debug)
		{
			for (int i = 0; i < index_block->index_size; ++i)
			{
				ShmListCounts cnts = shm_list_index_get_count(&index_block->cells[i], owned);
				collected_counts.count += cnts.count;
				collected_counts.deleted += cnts.deleted;
			}
		}
	}
	else
		shmassert(false);

	ShmListCounts counts = shm_list_get_fast_count(thread, list.local, owned);

	if (debug)
	{
		shmassert(counts.count == collected_counts.count);
		shmassert(counts.deleted == collected_counts.deleted);
	}

	transient_commit(thread);
	*result = counts;
	return RESULT_OK;
}

static void
shm_list_validate_indexed_block(ShmListIndexItem *index_item, ShmListBlock *block)
{
	shmassert(block->type == SHM_TYPE_LIST_BLOCK);
	shmassert(index_item->count == block->count);
	shmassert(index_item->new_count == block->new_count);
	shmassert(index_item->deleted == block->deleted);
	shmassert(index_item->new_deleted == block->new_deleted);
}

// either list_index or first_block is present. I know C data types are horrible.
ShmListBlockRef
shm_list_get_block(int block_index, ShmListIndex *list_index, ShmListBlockRef first_block, ShmListIndexItem **index_item)
{
	ShmListBlockRef result;
	if (NULL == list_index)
	{
		shmassert(block_index == 0);
		return first_block;
	}

	shmassert(block_index >= 0 && block_index < list_index->index_size);
	ShmListIndexItem *item = &list_index->cells[block_index];
	*index_item = item;
	result.shared = item->block;
	result.local = LOCAL(result.shared);

	if (result.local)
	{
		shm_list_validate_indexed_block(item, result.local);
	}
	return result;
}

typedef struct {
	ShmListIndex *index;
	ShmListIndexItem *cell;
	int cell_ii;
	ShmPointer block_shm;
} shm_list__index_desc;

typedef struct {
	ShmInt itemindex;
	ShmListBlock *block;
} shm_list__block_desc;

// Almost never fails
// Item is accessed by result.block[result.itemindex]
shm_list__block_desc
shm_list_get_item_desc(ThreadContext *thread, ListRef list, ShmInt itemindex, bool owned, shm_list__index_desc *index_desc)
{
	if (index_desc) {
		index_desc->index = NULL;
		index_desc->cell = NULL;
		index_desc->cell_ii = - 1;
		index_desc->block_shm = EMPTY_SHM;
	}

	if (owned)
		shm_cell_check_read_write_lock(thread, &list.local->base.lock);

	ShmListIndex *list_index = NULL;
	ShmListBlockRef first_block = { EMPTY_SHM, NULL };
	ShmRefcountedBlock *top_block = LOCAL(list.local->top_block);
	int block_count = 0;
	if (top_block == NULL)
	{
		shm_list__block_desc rslt;
		rslt.block = NULL;
		rslt.itemindex = -1;
		return rslt;
	}
	else if (SHM_TYPE_LIST_BLOCK == top_block->type)
	{
		first_block.local = (ShmListBlock *)top_block;
		first_block.shared = list.local->top_block;
		block_count = 1;
	}
	else if (SHM_TYPE_LIST_INDEX == top_block->type)
	{
		list_index = (ShmListIndex *)top_block;
		block_count = list_index->index_size;
	}
	else
		shmassert_msg(false, "Invalid top_block->type");

	ShmListCounts counts = shm_list_get_fast_count(thread, list.local, owned);

	ShmInt local_ii = itemindex + counts.deleted;
	int target_block_index = 0;
	if (list_index)
	{
		for (int idx = 0; idx < block_count; ++idx)
		{
			ShmListCounts block_counts = shm_list_index_get_count(&list_index->cells[idx], owned);
			if (local_ii >= block_counts.count + block_counts.deleted)
			{
				local_ii = local_ii - (block_counts.count + block_counts.deleted);
				target_block_index = idx + 1;
			}
			else
				break;
		}
		if (target_block_index == list_index->index_size)
		{
			// index is outside range
			shm_list__block_desc rslt;
			rslt.block = NULL;
			rslt.itemindex = -1;
			return rslt;
		}

		shmassert(target_block_index >= 0 && target_block_index < list_index->index_size);
	}
	else
		shmassert(target_block_index == 0);

	ShmListIndexItem *index_item = NULL;
	ShmListBlockRef block = shm_list_get_block(target_block_index, list_index, first_block, &index_item);

	if (index_desc) {
		index_desc->index = list_index;
		index_desc->cell = index_item;
		index_desc->cell_ii = target_block_index;
		index_desc->block_shm = index_item ? index_item->block : first_block.shared;
	}

	shm_list__block_desc rslt;
	rslt.block = block.local;
	rslt.itemindex = local_ii;
	return rslt;
}

int
shm_list_get_item_do(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer *result, bool acquire)
{
	*result = EMPTY_SHM;
	if_failure(
		transaction_lock_read(thread, &list.local->base.lock, list.shared, CONTAINER_LIST, NULL),
		{
			transient_abort(thread);
			return status;
		}
	);
	shm_cell_check_read_write_lock(thread, &list.local->base.lock);
	bool owned = shm_cell_have_write_lock(thread, &list.local->base.lock);

	ShmListCounts count = shm_list_get_fast_count(thread, list.local, owned);
	// single check for index out of upper range
	if (index >= count.count)
	{
		transient_commit(thread);
		return RESULT_INVALID;
	}

	shm_list__block_desc block_desc = shm_list_get_item_desc(thread, list, index, owned, NULL);
	shmassert(block_desc.block);

	ShmListCounts cnts = shm_list_block_get_count(block_desc.block, owned);
	shmassert(block_desc.itemindex >= cnts.deleted);
	if (block_desc.itemindex >= cnts.deleted + cnts.count)
	{
		transient_abort(thread);
		shmassert(false);
		return RESULT_FAILURE;
	}
	int ii = block_desc.itemindex;
	shmassert(ii >= 0 && ii < block_desc.block->capacity);

	ShmListCell *cell = NULL;
	cell = &block_desc.block->cells[block_desc.itemindex];
	if (cell)
	{
		ShmPointer value = shm_list_cell_get_data(cell, owned);
		if (acquire && SBOOL(value))
			shm_pointer_acq(thread, value); // must acquire inside transient transaction
		*result = value;
	}
	transient_commit(thread);
	return RESULT_OK;
}

int
shm_list_get_item(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer *result)
{
	return shm_list_get_item_do(thread, list, index, result, false);
}

int
shm_list_acq_item(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer *result)
{
	return shm_list_get_item_do(thread, list, index, result, true);
}

// not tested yet
int
shm_list_set_item_raw(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer value, bool consume)
{
	// heavily modified version of shm_list_set_item_do
	ShmListCounts count = shm_list_get_fast_count(thread, list.local, true);
	// single check for index out of upper range
	if (index >= count.count)
	{
		if (consume && SBOOL(value))
			shm_pointer_release(thread, value);
		return RESULT_INVALID;
	}

	// cannot manipulate transacted data here anyway
	bool owned = false;
	shm_list__block_desc block_desc = shm_list_get_item_desc(thread, list, index, owned, NULL);
	shmassert(block_desc.block);

	ShmListCounts cnts = shm_list_block_get_count(block_desc.block, owned);
	shmassert(block_desc.itemindex >= cnts.deleted);
	if (block_desc.itemindex >= cnts.deleted + cnts.count)
	{
		shmassert(false);
		return RESULT_FAILURE;
	}
	int ii = block_desc.itemindex;
	shmassert(ii >= 0 && ii < block_desc.block->capacity);

	ShmListCell *cell = NULL;
	cell = &block_desc.block->cells[block_desc.itemindex];
	if (cell)
	{
		if (!consume)
			shm_pointer_acq(thread, value);
		shm_pointer_move(thread, &cell->data, &value);
	}
	return RESULT_OK;
}

// not tested yet
int
shm_list_set_item_do(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer value, bool consume)
{
	shm_list_changes_check_inited(thread, list.local, NULL);
	if_failure(
		transaction_lock_write(thread, &list.local->base.lock, list.shared, CONTAINER_LIST, NULL),
		{
			if (consume && SBOOL(value))
				shm_pointer_release(thread, value);
			transient_abort(thread);
			return status;
		}
	);
	shm_cell_check_write_lock(thread, &list.local->base.lock);
	bool owned = true;

	ShmListCounts count = shm_list_get_fast_count(thread, list.local, true);
	// single check for index out of upper range
	if (index >= count.count)
	{
		transient_commit(thread);
		if (consume && SBOOL(value))
			shm_pointer_release(thread, value);
		return RESULT_INVALID;
	}

	shm_list__block_desc block_desc = shm_list_get_item_desc(thread, list, index, owned, NULL);
	shmassert(block_desc.block);

	ShmListCounts cnts = shm_list_block_get_count(block_desc.block, owned);
	shmassert(block_desc.itemindex >= cnts.deleted);
	if (block_desc.itemindex >= cnts.deleted + cnts.count)
	{
		transient_abort(thread);
		shmassert(false);
		return RESULT_FAILURE;
	}
	int ii = block_desc.itemindex;
	shmassert(ii >= 0 && ii < block_desc.block->capacity);

	ShmListCell *cell = NULL;
	cell = &block_desc.block->cells[block_desc.itemindex];
	if (cell)
	{
		if (cell->has_new_data && shm_pointer_is_valid(cell->new_data))
			shm_pointer_empty(thread, &cell->new_data);
		if (!consume)
			shm_pointer_acq(thread, value);
		shm_pointer_move(thread, &cell->new_data, &value);
		cell->new_data = true;
	}
	transient_commit(thread);
	return RESULT_OK;
}

int
shm_list_set_item(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer value)
{
	return shm_list_set_item_do(thread, list, index, value, false);
}

int
shm_list_consume_item(ThreadContext *thread, ListRef list, ShmInt index, ShmPointer value)
{
	return shm_list_set_item_do(thread, list, index, value, true);
}

void
fputc_ascii(char c, FILE *file)
{
	fputc(c, file);
	fputc('\0', file);
}

void
shm_list_print_to_file(FILE *file, ShmList *list)
{
	ShmListIndex *list_index = NULL;
	ShmListBlock *first_block = NULL;
	ShmRefcountedBlock *top_block = LOCAL(list->top_block);
	int block_count = 0;
	if (top_block == NULL)
		shmassert(false);
	else if (SHM_TYPE_LIST_BLOCK == top_block->type)
	{
		first_block = (ShmListBlock *)top_block;
		block_count = 1;
	}
	else if (SHM_TYPE_LIST_INDEX == top_block->type)
	{
		list_index = (ShmListIndex *)top_block;
		block_count = list_index->index_size;
	}
	else
		shmassert_msg(false, "Invalid top_block->type");

	for (int idx = 0; idx < block_count; ++idx)
	{
		ShmListBlock *block;
		if (list_index)
			block = LOCAL(list_index->cells[idx].block);
		else
			block = first_block;

		shmassert(block);

		for (int item = 0; item < block->count; ++item)
		{
			ShmValueHeader *header = LOCAL(block->cells[item].data);
			if (header)
			{
				if (shm_type_get_type(header->type) == shm_type_get_type(SHM_TYPE_UNICODE))
				{
					RefUnicode s = shm_ref_unicode_get(block->cells[item].data);
					for (int i = 0; i < s.len; ++i)
					{
						// little endian
						Py_UCS4 c = s.data[i];
						fputc(c & 0xFF, file);
						fputc((c & 0xFF00) >> 8, file);
					}
				}
				else
				{
					char *s = shm_value_get_data(header);
					int len = shm_value_get_length(header);
					for (int i = 0; i < len; ++i)
					{
						fputc_ascii(s[i], file);
					}
				}
				fputc_ascii('\r', file);
				fputc_ascii('\n', file);
			}
			else
			{
				fputc_ascii('n', file);
				fputc_ascii('o', file);
				fputc_ascii('n', file);
				fputc_ascii('e', file);
				fputc_ascii('\r', file);
				fputc_ascii('\n', file);
			}
		}

		fputc_ascii('-', file);
		fputc_ascii('-', file);
		fputc_ascii('\r', file);
		fputc_ascii('\n', file);
	}
}

ShmListBlock *
shm_list_new_block(ThreadContext *thread, ShmList *list, ShmPointer *result_shm, int capacity, int debug_id)
{
	// this alignment assumption might actually fail for 64 bits
	int block_size = isizeof(ShmListBlockHeader) + isizeof(ShmListCell) * capacity;
	ShmListBlock* rslt = new_shm_refcounted_block(thread, result_shm, block_size, SHM_TYPE_LIST_BLOCK, debug_id);
	memset(CAST_VL(&rslt->cells[0]), EMPTY_SHM, isizeof(ShmListCell) * capacity);
	rslt->capacity = capacity;
	rslt->count = 0;
	rslt->new_count = -1;
	rslt->deleted = 0;
	rslt->new_deleted = -1;
	rslt->count_added_after_relocation = 0;
	return rslt;
}

void
shm_list_init_cell(ShmListCell *new_cell, ShmPointer container_shm)
{
	memclear(new_cell, sizeof(ShmListCell));
	// ShmContainedBlock
	new_cell->type = SHM_TYPE_LIST_CELL;
	new_cell->size = sizeof(ShmListCell);
	new_cell->container = container_shm;
	// ShmCellBase
	new_cell->data = EMPTY_SHM;
	new_cell->has_new_data = false;
	new_cell->new_data = EMPTY_SHM;
	// ShmListCell
	new_cell->changed = false;
}

// 64k / 28 = 2300 max elements in the single block.
// 512k / 12 = 24k max blocks in index.
// 2.3k * 24k = 55M max elements, 1500 Mb of memory.
ShmInt
shm_list_max_index_size() {
	shmassert(max_heap_block_size != 0);
	ShmInt rslt = (max_heap_block_size - isizeof(ShmListIndexHeader)) / isizeof(ShmListIndexItem);
	shmassert(rslt > 0);
	return rslt;
}

static ShmInt
shm_list_max_block_capacity()
{
	return (medium_size_map[4] - isizeof(ShmListBlockHeader)) / isizeof(ShmListCell);
}

ShmList *
new_shm_list_with_capacity(ThreadContext *thread, PShmPointer result, ShmInt capacity)
{
	ShmList *list = new_shm_list(thread, result);
	shmassert(list->inited == false);
	int full_blocks_count = capacity / shm_list_max_block_capacity();
	int last_block_count = capacity % shm_list_max_block_capacity();
	int index_capacity = last_block_count == 0 ? full_blocks_count : full_blocks_count + 1;
	shmassert(index_capacity >= 0 && index_capacity < shm_list_max_index_size());
	if (index_capacity > 1)
	{
		int index_size = isizeof(ShmListIndexHeader) + isizeof(ShmListIndexItem) * index_capacity;
		ShmPointer new_index_shm = EMPTY_SHM;
		ShmListIndex *new_index_block = new_shm_refcounted_block(thread, &new_index_shm, index_size,
		                                                         SHM_TYPE_LIST_INDEX, SHM_LIST_INDEX_DEBUG_ID);
		new_index_block->index_size = index_capacity;
		for (int idx = 0; idx < index_size; idx++)
		{
			int block_capacity = shm_list_max_block_capacity();
			int block_count = block_capacity;
			if (idx == index_size - 1)
				if (last_block_count != 0)
				{
					block_count = last_block_count;
					block_capacity = block_count >= 8 ? block_count : 8;
				}

			ShmPointer block_shm = EMPTY_SHM;
			ShmListBlock *block = shm_list_new_block(thread, list, &block_shm,
			                                         block_capacity, SHM_LIST_BLOCK_DEBUG_ID);
			block->count = block_count;
		}
		shm_pointer_move(thread, &list->top_block, &new_index_shm);
		list->count = capacity;
	}
	else
    {
        ShmPointer top_block_shm = EMPTY_SHM;
        int top_block_capacity = last_block_count >= 8 ? last_block_count : 8;
        ShmListBlock *top_block = shm_list_new_block(thread, list, &top_block_shm,
                                                     top_block_capacity, SHM_LIST_BLOCK_DEBUG_ID);
        top_block->count = last_block_count;
        shm_pointer_move(thread, &list->top_block, &top_block_shm);
        list->count = capacity;
    }
    list->inited = true;
    return list;
}

static int shm_list_append_do__old_count;
static int shm_list_append_do__new_count;
static ShmListBlock *shm_list_append_do__last_block;

// Not adapted for dirty reads.
//
int
shm_list_append_do(ThreadContext *thread, ListRef list, ShmPointer value, bool consume, ShmInt *out_index)
{
	shm_list_append_do__old_count = -1;
	shm_list_append_do__new_count = -1;
	shm_list_append_do__last_block = NULL;

	shm_list_changes_check_inited(thread, list.local, NULL);
	// rslt->shared = EMPTY_SHM;
	bool lock_taken = false;
	if_failure(
		transaction_lock_write(thread, &list.local->base.lock, list.shared, CONTAINER_LIST, &lock_taken),
		{
			if (consume && SBOOL(value))
				shm_pointer_release(thread, value);
			transient_abort(thread);
			return status;
		}
	);
	shm_cell_check_write_lock(thread, &list.local->base.lock);
	// Below we ensure that the block has no previous transaction data if lock_taken = true.
	// We might consider checking all the blocks this way.

	{
		bool owned = true;

		ShmListBlock *tail_block = NULL;
		ShmPointer tail_block_shm = EMPTY_SHM;
		// ShmListIndex *list_index = NULL;
		// ShmListIndexItem *index_item = NULL;
		// int index_cell_ii = 0;
		// int old_count = -1;
		// index_item, list_index, old_count local vars should stay in sync
		shm_list__index_desc index_desc = {
			.index = NULL,
			.cell = NULL,
			.cell_ii = -1,
			.block_shm = EMPTY_SHM,
		};

		ShmRefcountedBlock *top_block = LOCAL(list.local->top_block);
		if (top_block == NULL)
		{
			// create initial block
			shmassert(list.local->inited == false);
			ShmPointer new_top_shm = EMPTY_SHM;
			tail_block = shm_list_new_block(thread, list.local, &new_top_shm, 8, SHM_LIST_BLOCK_FIRST_DEBUG_ID);
			tail_block_shm = new_top_shm;
			top_block = (ShmRefcountedBlock *)tail_block;
			index_desc.index = NULL;
			index_desc.cell = NULL;
			index_desc.cell_ii = 0;
			index_desc.block_shm = new_top_shm;
			shm_pointer_move(thread, &list.local->top_block, &new_top_shm);
			list.local->inited = true;
		}
		else if (SHM_TYPE_LIST_BLOCK == top_block->type)
		{
			tail_block = (ShmListBlock *)top_block;
			tail_block_shm = list.local->top_block;
			index_desc.index = NULL;
			index_desc.cell = NULL;
			index_desc.cell_ii = 0;
			index_desc.block_shm = tail_block_shm;
		}
		else if (SHM_TYPE_LIST_INDEX == top_block->type)
		{
			index_desc.index = (ShmListIndex *)top_block;
			index_desc.cell_ii = index_desc.index->index_size - 1;
			index_desc.cell = &index_desc.index->cells[index_desc.cell_ii];
			shmassert(index_desc.index->index_size > 0);
			shmassert(isizeof(ShmListIndexHeader) + ((intptr_t)index_desc.cell - (intptr_t)&index_desc.index->cells[0]) <
					index_desc.index->size);
			tail_block = LOCAL(index_desc.cell->block);
			tail_block_shm = index_desc.cell->block;
			index_desc.block_shm = tail_block_shm;
			shm_list_validate_indexed_block(index_desc.cell, tail_block);
		}
		else
		{
			shmassert(false);
			if (consume && SBOOL(value))
				shm_pointer_release(thread, value);
			return RESULT_FAILURE;
		}

		ShmInt old_capacity = tail_block->capacity;
		ShmListCounts old_cnts = shm_list_block_get_count(tail_block, owned);
		shmassert(old_cnts.count >= 0);
		shm_list_append_do__old_count = old_cnts.count;

		shmassert(tail_block);
		shmassert(tail_block->type == SHM_TYPE_LIST_BLOCK);
		int tail_block_id = mm_block_get_debug_id(tail_block, tail_block_shm);
		shmassert(SHM_LIST_BLOCK_FIRST_DEBUG_ID == tail_block_id || SHM_LIST_BLOCK_DEBUG_ID == tail_block_id);

		if (index_desc.index)
			shmassert(index_desc.cell);
		if (lock_taken)
		{
			// verify it has no data from previous transactions, see desc at the begginning of func.
			shm_list_block_verify_clean(tail_block);
			if (index_desc.index)
			{
				shmassert(index_desc.cell);
				shmassert(index_desc.cell->new_count == -1);
				shmassert(index_desc.cell->new_deleted == -1);
			}
		}

		ShmInt max_index_size = shm_list_max_index_size();
		ShmInt max_block_capacity = shm_list_max_block_capacity();

		if (old_cnts.count + old_cnts.deleted >= old_capacity)
		{
			// Block reallocation is constrained by indexes from ShmListChanges.
			// We can safely grow the blocks and index, but cannot shift existing offsets.
			// Moreover, we are not forced to rollback the reallocation for as long as we keep the indexes,
			// thus no "ShmList->new_index" and "ShmListIndex->new_cells".
			if (old_capacity >= max_block_capacity)
			{
				// create a new block and rebuild index
				ShmPointer new_tail_shm = EMPTY_SHM;
				ShmListBlock *old_tail = tail_block;
				ShmListIndex *old_index = index_desc.index;
				tail_block = shm_list_new_block(thread, list.local, &new_tail_shm, max_block_capacity / 4, SHM_LIST_BLOCK_DEBUG_ID);

				int new_index_capacity = 2; // first block already exists
				if (index_desc.index)
					new_index_capacity = index_desc.index->index_size + 1;
				shmassert(new_index_capacity > 0 && new_index_capacity < max_index_size);
				// alignment is not a problem here because everything is ShmInt
				int index_size = isizeof(ShmListIndexHeader) + isizeof(ShmListIndexItem) * new_index_capacity;
				ShmPointer new_index_shm = EMPTY_SHM;
				ShmListIndex *new_index_block = new_shm_refcounted_block(thread, &new_index_shm, index_size, SHM_TYPE_LIST_INDEX, SHM_LIST_INDEX_DEBUG_ID);
				new_index_block->index_size = new_index_capacity;

				int last_idx = new_index_capacity - 1;
				// clone the old index into new one
				if (old_index)
				{
					shmassert(index_desc.index->index_size > 0);
					memcpy(CAST_VL(&new_index_block->cells[0]), CAST_VL(&old_index->cells[0]),
					       sizeof(ShmListIndexItem) * (puint)old_index->index_size);
					// Clear the references from original index, because they are contained in the new index now
					for (int i = 0; i < old_index->index_size; i++)
						old_index->cells[i].block = EMPTY_SHM;
				}
				else
				{
					shmassert(list.local->top_block == index_desc.block_shm);
					// move top_block into first position
					shm_pointer_move(thread, &new_index_block->cells[0].block, &list.local->top_block);
					new_index_block->cells[0].count = old_tail->count;
					new_index_block->cells[0].new_count = old_tail->new_count;
					new_index_block->cells[0].deleted = old_tail->deleted;
					new_index_block->cells[0].new_deleted = old_tail->new_deleted;
				}

				memset(CAST_VL(&new_index_block->cells[last_idx]), 0xF9, sizeof(ShmListIndexItem));

				new_index_block->cells[last_idx].block = new_tail_shm; // consume
				new_index_block->cells[last_idx].count = tail_block->count;
				new_index_block->cells[last_idx].new_count = tail_block->new_count;
				new_index_block->cells[last_idx].deleted = tail_block->deleted;
				new_index_block->cells[last_idx].new_deleted = tail_block->new_deleted;

				// index_item, list_index, old_count local vars should stay in sync
				index_desc.cell = &new_index_block->cells[last_idx];
				index_desc.index = new_index_block;
				index_desc.cell_ii = last_idx;
				index_desc.block_shm = new_tail_shm;
				old_cnts.count = tail_block->count;
				old_cnts.deleted = tail_block->deleted;
				shmassert(old_cnts.count == 0);
				shmassert(old_cnts.deleted == 0);

				// shm_pointer_move is not thread-safe on weak ordering arch
				shm_pointer_move(thread, &list.local->top_block, &new_index_shm);
			}
			else
			{
				// Grow-reallocate the block.
				ShmInt new_capacity;
				if (old_capacity > 64)
					new_capacity = old_capacity + old_capacity / 4;
				else
					new_capacity = old_capacity * 2;

				if (new_capacity > max_block_capacity)
					new_capacity = max_block_capacity;

				ShmPointer new_tail_shm = EMPTY_SHM;
				int block_size = isizeof(ShmListBlockHeader) + isizeof(ShmListCell) * new_capacity;
				int old_block_size = isizeof(ShmListBlockHeader) + isizeof(ShmListCell) * old_capacity;
				shmassert(old_block_size < block_size);
				ShmListBlock *old_tail_block = tail_block;
				shmassert(index_desc.block_shm != EMPTY_SHM);
				int old_tail_block_id = mm_block_get_debug_id(old_tail_block, index_desc.block_shm);
				shmassert(SHM_LIST_BLOCK_FIRST_DEBUG_ID == old_tail_block_id || SHM_LIST_BLOCK_DEBUG_ID == old_tail_block_id);

				tail_block = shm_list_new_block(thread, list.local, &new_tail_shm, new_capacity, old_tail_block_id);
				shmassert(tail_block->size == block_size);

				shm_list_append_do__last_block = tail_block;

				tail_block->count = old_tail_block->count;
				tail_block->new_count = old_tail_block->new_count;
				tail_block->deleted = old_tail_block->deleted;
				tail_block->new_deleted = old_tail_block->new_deleted;

				memcpy(CAST_VL(&tail_block->cells[0]), CAST_VL(&old_tail_block->cells[0]), (size_t)(isizeof(ShmListCell) * old_capacity));
				memset(CAST_VL(&tail_block->cells[old_capacity]), 0xF7, (size_t)(block_size - old_block_size));

				if (index_desc.cell)
					shm_pointer_move(thread, &index_desc.cell->block, &new_tail_shm);
				else
					shm_pointer_move(thread, &list.local->top_block, &new_tail_shm);
			}
		}

		int idx = old_cnts.count + old_cnts.deleted;
		shmassert(idx < tail_block->capacity);
		ShmListCell *new_cell = &tail_block->cells[idx];
		shm_list_init_cell(new_cell, list.shared);

		// update cell
		if (!consume && SBOOL(value))
			shm_pointer_acq(thread, value);
		shm_pointer_move(thread, &new_cell->new_data, &value);
		new_cell->has_new_data = true;
		// update block
		tail_block->new_count = old_cnts.count + 1;
		// update index
		if (index_desc.cell)
			index_desc.cell->new_count = old_cnts.count + 1;
		// update list
		ShmListCounts old_total_counts = shm_list_get_fast_count(thread, list.local, owned);
		p_atomic_int_set(&list.local->new_count, old_total_counts.count + 1);

		if (out_index)
			*out_index = old_total_counts.count;

		shm_list_append_do__new_count = old_cnts.count + 1;

		shm_list_changes_push(thread, list.local, tail_block, index_desc.cell_ii, idx);
		transient_commit(thread);
		// rslt->local = new_cell;

		return RESULT_OK;
	};
}

int
shm_list_append(ThreadContext *thread, ListRef list, ShmPointer value, ShmInt *index)
{
	return shm_list_append_do(thread, list, value, false, index);
}

int
shm_list_append_consume(ThreadContext *thread, ListRef list, ShmPointer value, ShmInt *index)
{
	return shm_list_append_do(thread, list, value, true, index);
}

// result is acquired
int
shm_list_popleft(ThreadContext *thread, ListRef list, ShmPointer *result)
{
	shm_pointer_empty(thread, result);

	bool lock_taken = false;
	if_failure(
		transaction_lock_write(thread, &list.local->base.lock, list.shared, CONTAINER_LIST, &lock_taken),
		{
			transient_abort(thread);
			return status;
		}
	);

	shm_cell_check_write_lock(thread, &list.local->base.lock);
	// Below we ensure that the block has no previous transaction data if lock_taken = true.
	// We might consider checking all the blocks this way.

	bool owned = true;

	shm_list__index_desc index_desc;
	shm_list__block_desc block_desc = shm_list_get_item_desc(thread, list, 0, owned, &index_desc);
	if (block_desc.itemindex == -1)
	{
		transient_commit(thread);
		return RESULT_OK;
	}
	shmassert(block_desc.block);

	if (index_desc.index)
		shmassert(index_desc.cell);
	if (lock_taken)
	{
		// verify it has no data from previous transactions, see desc at the begginning of func.
		shm_list_block_verify_clean(block_desc.block);
		if (index_desc.index)
		{
			shmassert(index_desc.cell->new_count == -1);
			shmassert(index_desc.cell->new_deleted == -1);
		}
	}

	ShmListCounts old_cnts = shm_list_block_get_count(block_desc.block, owned);
	if (block_desc.itemindex >= old_cnts.count + old_cnts.deleted)
	{
		shmassert(false); // should've been handled by shm_list_get_item_desc
		transient_commit(thread);
		return RESULT_OK;
	}
	int ii = block_desc.itemindex;
	shmassert(ii >= 0 && ii < block_desc.block->capacity);

	ShmListCell *cell = &block_desc.block->cells[ii];
	// Update cell
	// shm_pointer_empty(thread, &cell->new_data);
	if (cell->has_new_data)
		shm_pointer_move(thread, result, &cell->new_data);
	else
	{
		*result = cell->data;
		shm_pointer_acq(thread, *result);
		cell->new_data = EMPTY_SHM;
	}
	cell->has_new_data = true;
	// Update block
	block_desc.block->new_count = old_cnts.count - 1;
	block_desc.block->new_deleted = old_cnts.deleted + 1;
	// Update index
	if (index_desc.cell)
	{
		index_desc.cell->new_count = old_cnts.count - 1;
		index_desc.cell->new_deleted = old_cnts.deleted + 1;
	}
	// Update list
	ShmListCounts old_total_counts = shm_list_get_fast_count(thread, list.local, owned);
	p_atomic_int_set(&list.local->new_count, old_total_counts.count - 1);
	p_atomic_int_set(&list.local->new_deleted, old_total_counts.deleted + 1);

	shm_list_changes_push(thread, list.local, block_desc.block, index_desc.cell_ii, ii);

	transient_commit(thread);
	shmassert(SBOOL(*result));
	return RESULT_OK;
}

int
shm_list_commit(ThreadContext *thread, ShmList *list)
{
	shm_cell_check_write_lock(thread, &list->base.lock);

	ShmListChanges *changes = LOCAL(list->changes_shm);
	shmassert(changes);

	ShmListIndex *list_index = NULL;
	ShmListBlockRef first_block = {EMPTY_SHM, NULL};
	ShmRefcountedBlock *top_block = LOCAL(list->top_block);
	if (top_block == NULL)
		shmassert(false);
	else if (SHM_TYPE_LIST_BLOCK == top_block->type)
	{
		first_block.local = (ShmListBlock *)top_block;
		first_block.shared = list->top_block;
	}
	else if (SHM_TYPE_LIST_INDEX == top_block->type)
		list_index = (ShmListIndex *)top_block;
	else
		shmassert_msg(false, "Invalid top_block->type");

	// We should be sorting the changes list by block_index and then processing their items for each one block in batches
	// Dirty reads are kinda supported for growing the block size, because new_size is applied after the modification cycle thus showing the smallest valid block frame.
	// Dirty reads are not implementing for removal -- the removal is not implemented at all.
	for (int i = 0; i < changes->count; ++i)
	{
		ShmListChangeItem *item = &changes->cells[i];
		ShmListIndexItem *index_item = NULL;
		ShmListBlockRef block = shm_list_get_block(item->block_index, list_index, first_block, &index_item);

		shmassert(block.local);
		ShmListCounts counts = shm_list_block_get_count(block.local, true);

		if (index_item)
		{
			// done by shm_list_get_block
			// shmassert(index_item->count == old_count);
			// shmassert(index_item->new_count == new_count);
		}
		else
		{
			ShmListCounts total_counts = shm_list_get_fast_count(thread, list, true);
			shmassert(total_counts.count == counts.count);
			shmassert(total_counts.deleted == counts.deleted);
		}
		// shmassert(counts.count != EVIL_MARK);

		// this calculation holds true for as long as new elements are appended to the tail and old elements are removed from the head.
		// "deleted" count can only grow by removing items from "count". Thus new_deleted + new_count >= deleted + count.
		ShmInt max_actual_capacity = counts.count + counts.deleted;
		shmassert(max_actual_capacity <= block.local->capacity);

		ShmInt ii = item->item_index; // this index is from start of the block, not from the first actual data item.
		shmassert(ii >= 0 && ii < max_actual_capacity);

		if (block.local->cells[ii].has_new_data)
		{
			block.local->cells[ii].has_new_data = false; // leak is better than wild pointer
			if (ii >= counts.deleted)
				shmassert(block.local->cells[ii].new_data != EMPTY_SHM);
			else
				shmassert(block.local->cells[ii].new_data == EMPTY_SHM);
			shm_pointer_move_atomic(thread, &block.local->cells[ii].data, &block.local->cells[ii].new_data);
			shmassert(block.local->cells[ii].changed);
			block.local->cells[ii].changed = false;
		}
	}
	// second pass for setting the block->count, thus only valid part of block would be visible.
	bool rebuild_index = false;
	for (int i = 0; i < changes->count; ++i)
	{
		ShmListChangeItem *item = &changes->cells[i];
		ShmListIndexItem *index_item = NULL;
		ShmListBlockRef block = shm_list_get_block(item->block_index, list_index, first_block, &index_item);
		shmassert(block.local);
		ShmInt new_count = block.local->new_count;
		ShmInt new_deleted = block.local->new_deleted;
		if (new_count != -1)
		{
			// not sure about the order
			p_atomic_int_set(&block.local->count, new_count);
			p_atomic_int_set(&block.local->new_count, -1);
			if (index_item)
			{
				shmassert(index_item->new_count == new_count);
				p_atomic_int_set(&index_item->count, new_count);
				p_atomic_int_set(&index_item->new_count, -1);
			}

			block.local->count_added_after_relocation += new_count - block.local->count;

			if (new_count == 0)
				rebuild_index = true;
		}
		if (new_deleted != -1)
		{
			p_atomic_int_set(&block.local->deleted, new_deleted);
			p_atomic_int_set(&block.local->new_deleted, -1);
			if (index_item)
			{
				shmassert(index_item->new_deleted == new_deleted);
				p_atomic_int_set(&index_item->deleted, new_deleted);
				p_atomic_int_set(&index_item->new_deleted, -1);
			}
		}
	}

	if (rebuild_index && list_index != NULL) {
		// Some blocks are empty now, delete them.
		// Do not delete the last block.
		shmassert(list_index->index_size > 1);
		int first_valid = -1;
		for (int i = 0; i < list_index->index_size; i++)
		{
			shmassert(list_index->cells[i].new_count == -1); // transaction committed
			shmassert(list_index->cells[i].new_deleted == -1); // transaction committed
			if (first_valid != -1)
				shmassert(list_index->cells[i].count != 0); // can't have empty blocks in the middle
			if (list_index->cells[i].count != 0)
				first_valid = i;
		}
		// Do not delete the last block.
		if (first_valid == -1)
			first_valid = list_index->index_size - 1;
		int final_index_size = list_index->index_size - first_valid;
		if (final_index_size < 2) {
			// single block left
			int last_index = list_index->index_size - 1;
			shm_pointer_move(thread, &list->top_block, &list_index->cells[last_index].block);
			list->count = list_index->cells[last_index].count;
			list->new_count = list_index->cells[last_index].new_count;
			list->deleted = list_index->cells[last_index].deleted;
			list->new_deleted = list_index->cells[last_index].new_deleted;
			for (int i = 0; i < last_index; i++)
				shm_pointer_empty(thread, &list_index->cells[i].block);
		}
		else
		{
			shmassert(final_index_size > 0 && final_index_size < shm_list_max_index_size());

			ShmListCounts old_totals = shm_list_get_fast_count(thread, list, true);
			ShmInt delta_deleted = 0;
			ShmListCounts new_counts = { 0, 0 };

			for (int i = 0; i < first_valid; i++)
			{
				ShmListCounts index_counts = shm_list_index_get_count(&list_index->cells[i], true);
				shmassert(index_counts.deleted >= 0);
				shmassert(index_counts.count == 0);
				delta_deleted += index_counts.deleted;
			}

			for (int i = first_valid; i < list_index->index_size; i++)
			{
				ShmListCounts index_counts = shm_list_index_get_count(&list_index->cells[i], true);
				shmassert(index_counts.deleted >= 0);
				shmassert(index_counts.count != 0);
				new_counts.deleted += index_counts.deleted;
				new_counts.count += index_counts.count;
			}

			shmassert(delta_deleted + new_counts.deleted == old_totals.deleted);
			shmassert(new_counts.count == old_totals.count);

			// alignment is not a problem here because everything is ShmInt
			int index_size = isizeof(ShmListIndexHeader) + isizeof(ShmListIndexItem) * final_index_size;
			ShmPointer new_index_shm = EMPTY_SHM;
			ShmListIndex *new_index_block = new_shm_refcounted_block(thread, &new_index_shm, index_size, SHM_TYPE_LIST_INDEX, SHM_LIST_INDEX_DEBUG_ID);
			new_index_block->index_size = final_index_size;

			memcpy(CAST_VL(&new_index_block->cells[0]), CAST_VL(&list_index->cells[first_valid]),
					sizeof(ShmListIndexItem) * (puint)final_index_size);
			// clear copied blocks, release the deleted.
			for (int i = 0; i < first_valid; i++)
				shm_pointer_empty(thread, &list_index->cells[i].block);
			for (int i = first_valid; i < list_index->index_size - 1; i++)
				p_atomic_shm_pointer_set(&list_index->cells[i].block, EMPTY_SHM);

			p_atomic_shm_pointer_set(&list->top_block, new_index_shm);
			list->new_deleted = new_counts.deleted;
		}
	}

	if (list->new_count != -1)
	{
		p_atomic_int_set(&list->count, list->new_count);
		p_atomic_int_set(&list->new_count, -1);
	}
	if (list->new_deleted != -1)
	{
		p_atomic_int_set(&list->deleted, list->new_deleted);
		p_atomic_int_set(&list->new_deleted, -1);
	}

	return RESULT_OK;
}

int
shm_list_rollback(ThreadContext *thread, ShmList *list)
{
	shm_cell_check_write_lock(thread, &list->base.lock);

	ShmListChanges *changes = LOCAL(list->changes_shm);
	shmassert(changes);

	ShmListIndex *list_index = NULL;
	ShmListBlockRef first_block = { EMPTY_SHM, NULL };
	ShmRefcountedBlock *top_block = LOCAL(list->top_block);
	if (top_block == NULL)
		shmassert(false);
	else if (SHM_TYPE_LIST_BLOCK == top_block->type)
	{
		first_block.local = (ShmListBlock *)top_block;
		first_block.shared = list->top_block;
	}
	else if (SHM_TYPE_LIST_INDEX == top_block->type)
		list_index = (ShmListIndex *)top_block;
	else
		shmassert_msg(false, "Invalid top_block->type");

	for (int i = 0; i < changes->count; ++i)
	{
		ShmListChangeItem *item = &changes->cells[i];
		ShmListIndexItem *index_item = NULL;
		ShmListBlockRef block = shm_list_get_block(item->block_index, list_index, first_block, &index_item);
		shmassert(block.local);
		ShmListCounts counts = shm_list_block_get_count(block.local, true);
		if (index_item)
		{
			// done by shm_list_get_block
			// shmassert(index_item->count == old_count);
			// shmassert(index_item->new_count == new_count);
		}
		else
		{
			ShmListCounts total_counts = shm_list_get_fast_count(thread, list, true);
			shmassert(total_counts.count == counts.count);
			shmassert(total_counts.deleted == counts.deleted);
		}
		// shmassert(counts.count != EVIL_MARK);

		// this calculation holds true for as long as new elements are appended to the tail and old elements are removed from the head.
		// "deleted" count can only grow by removing items from "count". Thus new_deleted + new_count >= deleted + count.
		ShmInt max_actual_capacity = counts.count + counts.deleted;
		shmassert(max_actual_capacity <= block.local->capacity);

		ShmInt ii = item->item_index;
		shmassert(ii >= 0 && ii < max_actual_capacity);

		if (block.local->cells[ii].has_new_data)
		{
			block.local->cells[ii].has_new_data = false; // leak is better than wild pointer
			if (ii >= counts.deleted)
				shmassert(block.local->cells[ii].new_data != EMPTY_SHM);
			else
				shmassert(block.local->cells[ii].new_data == EMPTY_SHM);
			shm_pointer_empty_atomic(thread, &block.local->cells[ii].new_data);

			shmassert(block.local->cells[ii].changed);
			block.local->cells[ii].changed = false;
		}
	}
	for (int i = 0; i < changes->count; ++i)
	{
		ShmListChangeItem *item = &changes->cells[i];
		ShmListIndexItem *index_item = NULL;
		ShmListBlockRef block = shm_list_get_block(item->block_index, list_index, first_block, &index_item);
		shmassert(block.local);
		ShmInt new_count = block.local->new_count;
		ShmInt new_deleted = block.local->new_deleted;

		if (new_count != -1)
		{
			p_atomic_int_set(&block.local->new_count, -1);
			if (index_item)
			{
				shmassert(index_item->new_count == new_count);
				p_atomic_int_set(&index_item->new_count, -1);
			}
		}

		if (new_deleted != -1)
		{
			p_atomic_int_set(&block.local->new_deleted, -1);
			if (index_item)
			{
				shmassert(index_item->new_deleted == new_deleted);
				p_atomic_int_set(&index_item->new_deleted, -1);
			}
		}
	}

	if (list->new_count != -1)
	{
		p_atomic_int_set(&list->new_count, -1);
	}
	if (list->new_deleted != -1)
	{
		p_atomic_int_set(&list->new_deleted, -1);
	}
	return RESULT_OK;
}

int
shm_list_unlock(ThreadContext *thread, ShmList *list, ShmInt type)
{
	if (TRANSACTION_ELEMENT_WRITE == type)
		p_atomic_shm_pointer_set(&list->base.lock.transaction_data, EMPTY_SHM);
	shm_list_changes_clear(thread, list);
	_shm_cell_unlock(thread, &list->base.lock, type);
	return RESULT_OK;
}
// end of ShmList


// Start of ShmDict
void
shm_dict_init_key(ShmDictKey *key, const char *s)
{
	key->key = (char *)(intptr_t)s;
	key->key_ucs = NULL;
	key->key_owned = false;
	key->keysize = (int)strlen(s);
	key->hash = hash_string_ascii(key->key, key->keysize);
}

void
shm_dict_init_key_consume(ShmDictKey *key, const char *s)
{
	key->key = (char *)(intptr_t)s;
	key->key_ucs = NULL;
	key->key_owned = true;
	key->keysize = (int)strlen(s);
	key->hash = hash_string_ascii(key->key, key->keysize);
}

void
shm_dict_free_key(ShmDictKey *key)
{
	if (key->key_owned)
	{
		char *tmp = key->key;
		key->key = NULL;
		free(tmp);
	}
}

// ShmDict
ShmDict *
new_shm_dict(ThreadContext *thread, PShmPointer shm_pointer)
{
	// if (shm_pointer) *shm_pointer = EMPTY_SHM; - done by get_mem
	ShmDict *dict = (ShmDict*) get_mem(thread, shm_pointer, sizeof(ShmDict), SHM_DICT_DEBUG_ID);
	init_cell((ShmCell*) dict, sizeof(ShmDict), SHM_TYPE_DICT);
	return dict;
}

ShmPointer
shm_dict_element_owned_get_value(ShmDictElement *element)
{
	if (element->has_new_data)
		return element->new_data;
	else
		return element->data;
}

ShmPointer
shm_dict_element_get_value(ShmDictElement *element, bool owned)
{
	if ( ! element)
		return EMPTY_SHM;
	if (owned)
		return shm_dict_element_owned_get_value(element);
	else
		return element->data;
}

int
shm_dict_push_delta(ThreadContext *thread, PShmPointer delta, int type, ShmPointer element, ShmPointer array)
{
	ShmDictDeltaArray *delta_array = LOCAL(*delta);
	if ( ! delta_array)
	{
		delta_array = get_mem(thread, delta, sizeof(ShmDictDeltaArray), SHM_DICT_DELTA_ARRAY_DEBUG_ID);
		if ( ! delta_array) return RESULT_FAILURE;
		delta_array->type = SHM_TYPE_DICT_DELTA;
		delta_array->size = sizeof(ShmDictDeltaArray);
		delta_array->capacity = DELTA_ARRAY_SIZE;
		delta_array->count = 0;
	}

	/*char buffer[20];
	OutputDebugStringA("delta push ");
	_itoa_s(delta_array->count, buffer, 20, 10);
	OutputDebugStringA(buffer);
	OutputDebugStringA(" ");
	_itoa_s(delta_array->size, buffer, 20, 10);
	OutputDebugStringA(buffer);
	OutputDebugStringA("\n");*/
	if (!(delta_array->count < delta_array->capacity))
		DebugBreak();
	// shmassert_msg(delta_array->count < delta_array->size, "delta array overflow");
	if ( ! (delta_array->count < delta_array->size) ) return RESULT_FAILURE;

	int idx = delta_array->count;
	delta_array->deltas[idx].type = type;
	delta_array->deltas[idx].value = element;
	delta_array->deltas[idx].parent_element = array;
	delta_array->count++;

	return RESULT_OK;
}

typedef vl2 struct {
	bool owned;
	uint32_t hash;
	const char *key;
	int keysize;

	ShmDictElement *first_empty;
	ShmPointer first_empty_shm;
	int first_empty_level;
	ShmPointer first_empty_array_shm;

	ShmDictElement *deepest;
	ShmPointer deepest_shm;
	int deepest_level;
	ShmPointer deepest_array_shm;
} DictFindData;

void
dict_init_find_data(DictFindData *data)
{
	memset(data, 0, sizeof(DictFindData));
	data->first_empty_shm = EMPTY_SHM;
	data->first_empty_array_shm = EMPTY_SHM;
	data->deepest_shm = EMPTY_SHM;
	data->deepest_array_shm = EMPTY_SHM;
}

void
dict_init_node(ShmDictElementArray *array, int size, int capacity)
{
	array->type = SHM_TYPE_DICT_ELEMENT_ARRAY;
	array->size = size;
	ShmDictElement *elements = &array->first;
	for (int i = 0; i < capacity; ++i)
	{
		elements[i].data = EMPTY_SHM;
		elements[i].has_new_data = false;
		elements[i].new_data = EMPTY_SHM;
		elements[i].key = EMPTY_SHM;
		elements[i].key_debug = NULL;
		elements[i].nested = EMPTY_SHM;
	}
}

// deepest is the first empty node found for hash, no matter spare or in use.
// Returns true only if at some level it passed the "deepest" with perfect key match and in-use.

ShmDictElement *
find_nested(ShmDictElementArray *array, ShmPointer array_shm, uint32_t mask, uint32_t level,
	DictFindData *find_data, PShmPointer result_shm)
{
	if (array == NULL) return NULL;
	ShmDictElement *elements = &array->first;
	shmassert(level < (sizeof(uint32_t) * 8 - DICT_LEVEL_BITS - 1) / DICT_LEVEL_BITS);
	uint32_t level_mult = 1 << (level*DICT_LEVEL_BITS);
	uint32_t prev_levels_mask = level_mult - 1;
	uint32_t all_levels_mask = (level_mult << DICT_LEVEL_BITS) - 1;
	uint32_t level_mask = all_levels_mask & (~prev_levels_mask);
	// e.g. for level = 2 and DICT_LEVEL_BITS = 2: level_mult = 0x10; prev_levels_mask = 0xF; all_levels_mask = 0x3F; level_mask = 0x30;

	// for (int idx = 0; i < DICT_LEVEL_SIZE; ++i) {
	//   if (level_mult*i == (hash & level_mask)) {
	uint32_t idx = (find_data->hash & level_mask) / level_mult;
	// fprintf(stderr, "level %i idx %i\n", level, idx);
	shmassert_msg(0 <= idx && idx < DICT_LEVEL_SIZE, "idx >= 0 && idx < DICT_LEVEL_SIZE");
	find_data->deepest = &elements[idx];
	find_data->deepest_level = level;
	find_data->deepest_shm = shm_pointer_shift(array_shm, ((char*)&elements[idx]) - ((char*)array));
	find_data->deepest_array_shm = array_shm;

	bool filled = false;
	bool found = false;
	ShmPointer data = shm_dict_element_get_value(&elements[idx], find_data->owned);
	if (data != EMPTY_SHM && data != DEBUG_SHM)
	{
		filled = true;
		if (elements[idx].hash == find_data->hash)
		{
			const ShmDictKeyString *block = LOCAL(elements[idx].key);
			const char *data_key = CAST_VL(&block->data);
			if (find_data->keysize == elements[idx].keysize && strncmp(data_key, find_data->key, find_data->keysize) == 0)
			{
				found = true;
			}
		}
	}
	if (found)
	{
		*result_shm = find_data->deepest_shm;
		return find_data->deepest;
	}
	else
	{
		if ( ! filled && find_data->first_empty == NULL)
		{
			find_data->first_empty = &elements[idx];
			find_data->first_empty_level = level;
			find_data->first_empty_shm = shm_pointer_shift(array_shm, ((char*)&elements[idx]) - ((char*)array));
			find_data->first_empty_array_shm = array_shm;
		}
	}
	ShmDictElementArray *nested = LOCAL(elements[idx].nested);
	return find_nested(nested, elements[idx].nested, mask, level + 1, find_data, result_shm);
}

ShmDictElement *
shm_dict_do_find(ShmDictElementArray *root, ShmPointer root_shm, DictFindData *find_data, PShmPointer result_shm)
{
	uint32_t mask = ~((1 << DICT_LEVEL_BITS) - 1); // FFFFFFF8 for DICT_LEVEL_SIZE = 8
	shmassert(false);
	return find_nested(root, root_shm, mask, 0, find_data, result_shm);
}

int
shm_dict_find_append_element(ThreadContext *thread, DictRef dict, ShmDictKey *key, bool can_append,
		ShmDictElement **result, PShmPointer result_shm, PShmPointer array_shm)
{
	bool lock_taken = false;
	// start_transaction(thread, TRANSACTION_TRANSIENT, LOCKING_WRITE, false, NULL);
	if_failure(transaction_lock_write(thread, &dict.local->lock, dict.shared, CONTAINER_DICT_DELTA, &lock_taken),
	{
		return status;
	});
	ShmPointer root_shm = dict.local->data;
	ShmDictElementArray *root = LOCAL(dict.local->data);
	if ( ! root)
	{
		if (!can_append)
		{
			*result = NULL;
			*result_shm = EMPTY_SHM;
			*array_shm = EMPTY_SHM;
			return RESULT_OK;
		}
		int array_size = sizeof(ShmDictElementArray) + sizeof(ShmDictElement) * (DICT_LEVEL_SIZE - 1);
		root = (ShmDictElementArray *) get_mem(thread, &root_shm, array_size, SHM_DICT_ELEMENT_ARRAY_DEBUG_ID);
		if ( ! root) {
			// transaction_unlock_local(thread, &dict.local->lock, dict.shared, RESULT_FAILURE, lock_taken);
			// abort_transaction(thread, NULL);
			return RESULT_FAILURE;
		}
		shmassert(SBOOL(root_shm));
		dict_init_node(root, array_size, DICT_LEVEL_SIZE);
		dict.local->data = root_shm;
		// Let's just allocate it on demand and never deallocate
		// shm_dict_push_delta(dict.local, DICT_DELTA_NEW_ROOT, root_shm, dict.shared);
	}

	uint32_t mask = ((1 << DICT_LEVEL_BITS) - 1); // 7 for DICT_LEVEL_SIZE = 8
	DictFindData find_data;
	dict_init_find_data(&find_data);
	find_data.owned = true;
	find_data.hash = key->hash;
	find_data.key = key->key;
	find_data.keysize = key->keysize;
	*result = find_nested(root, root_shm, mask, 0, &find_data, result_shm);
	if ( ! *result)
	{
		if (!can_append)
		{
			*result = NULL;
			*result_shm = EMPTY_SHM;
			*array_shm = EMPTY_SHM;
			return RESULT_OK;
		}
		shmassert_msg(find_data.deepest && find_data.deepest_level >= 0, "find_data.deepest && find_data.deepest_level >= 0");
		shmassert_msg(find_data.deepest_array_shm != EMPTY_SHM, "find_data.deepest_array_shm != EMPTY_SHM");
		// ShmPointer data = shm_dict_element_owned_get_value(find_data.deepest);
		// element is empty when both new_data and data are empty, meaning a committed removal of the value.
		// if (data != EMPTY_SHM)
		if (find_data.first_empty == NULL)
		{
			// deepest is already used, obviously.
			// Still no way to reclaim other empty nodes except deepest.
			ShmPointer node_shm;
			int array_size = sizeof(ShmDictElementArray) + sizeof(ShmDictElement) * (DICT_LEVEL_SIZE - 1);
			ShmDictElementArray *node = get_mem(thread, &node_shm, array_size, SHM_DICT_ELEMENT_ARRAY_DEBUG_ID);
			if ( ! node) {
				// transaction_unlock_local(thread, &dict.local->lock, dict.shared, RESULT_FAILURE, lock_taken);
				// abort_transaction(thread, NULL);
				return RESULT_FAILURE;
			}
			dict_init_node(node, array_size, DICT_LEVEL_SIZE);
			shmassert(shm_dict_push_delta(thread, &dict.local->delta, DICT_DELTA_NEW_NODE, node_shm, find_data.deepest_shm));
			find_data.deepest->nested = node_shm;
			ShmDictElement *elements = &node->first;

			// find_data.deepest_level ++;
			*array_shm = node_shm;
			int index = (key->hash >> (DICT_LEVEL_BITS*(find_data.deepest_level+1))) & mask;
			shmassert_msg(index >= 0 && index < DICT_LEVEL_SIZE, "index >= 0 && index < DICT_LEVEL_SIZE");
			// fprintf(stderr, "created new node at level %i for insertion at idx %i hash %08x\n", find_data.deepest_level, index, key->hash);
			*result = &elements[index];
			*result_shm = shm_pointer_shift(node_shm, ((char*)&elements[index]) - ((char*)node));
		}
		else
		{
			*result = find_data.first_empty;
			*result_shm = find_data.first_empty_shm;
			*array_shm = find_data.first_empty_array_shm;
		}
		// claim the item
		if (SBOOL((*result)->key))
		{
			free_mem(thread, (*result)->key, -1);
			(*result)->key = EMPTY_SHM;
		}
		int memsize = sizeof(ShmAbstractBlock) + key->keysize;
		ShmDictKeyString *block = get_mem(thread, &(*result)->key, memsize, SHM_DICT_ELEMENT_KEY_DEBUG_ID);
		block->type = SHM_TYPE_DICT_ELEMENT_ARRAY;
		block->size = memsize;
		char *s = CAST_VL(&block->data);
		memcpy(s, key->key, (size_t)key->keysize);
		(*result)->keysize = key->keysize;
		(*result)->hash = key->hash;
		(*result)->key_debug = s;
	}

	// int status = transaction_unlock_local(thread, &dict.local->lock, dict.shared, RESULT_OK, lock_taken);
	// commit_transaction(thread, NULL);
	if ( ! *result)
		return RESULT_FAILURE;
	else
		return RESULT_OK;
}

int
shm_dict_set_consume(ThreadContext *thread, DictRef dict, ShmDictKey *key,
		ShmPointer value, ShmDictElement **result, DictProfiling *profiling)
{
	if (result)
		*result = NULL;
	bool lock_taken1 = false;
	uint64_t tmp;
	tmp = rdtsc();
	// start_transaction(thread, TRANSACTION_TRANSIENT, LOCKING_WRITE, false, NULL);
	if (profiling) profiling->counter1 += rdtsc() - tmp;
	tmp = rdtsc();
	if_failure(transaction_lock_write(thread, &dict.local->lock, dict.shared, CONTAINER_DICT_DELTA, &lock_taken1),
	{
		if (SBOOL(value))
			shm_pointer_release(thread, value);
		transient_abort(thread);
		return status;
	});
	if (profiling) profiling->counter2 += rdtsc() - tmp;
	tmp = rdtsc();
	{
		//ShmDictElement *element = shm_dict_find(thread, dict, hash, key, keysize));
		//if ( ! element)
		ShmDictElement *element = NULL;
		ShmPointer element_shm = EMPTY_SHM;
		ShmPointer array_shm = EMPTY_SHM;
		{
			if_failure(shm_dict_find_append_element(thread, dict, key, value != EMPTY_SHM, &element, &element_shm, &array_shm),
			{
				if (SBOOL(value))
					shm_pointer_release(thread, value);
				transient_abort(thread);
				return status;
			});
		}
		if (profiling) profiling->counter3 += rdtsc() - tmp;
		tmp = rdtsc();
		if (element == NULL && value == EMPTY_SHM)
		{
			transient_commit(thread);
			return RESULT_OK;
		}
		shmassert_msg(element != NULL, "shm_dict_set_consume: element");
		tmp = rdtsc();
		if (shm_dict_element_get_value(element, true) != value)
		{
			if (profiling) profiling->counter4 += rdtsc() - tmp;
			tmp = rdtsc();
			shmassert(shm_dict_push_delta(thread, &dict.local->delta, DICT_DELTA_CHANGED, value, element_shm));

			// release valid value only
			if (element->has_new_data && shm_pointer_is_valid(element->new_data))
				shm_pointer_empty(thread, &element->new_data);
			if (true || !element->has_new_data || element->new_data != element->data)
			{
				element->has_new_data = true;
				element->new_data = value; // consume
			}
			else
			{
				// we can live without this branch
				element->has_new_data = false;
				element->new_data = EMPTY_SHM;
				shm_pointer_release(thread, value);
			}
			if (profiling) profiling->counter5 += rdtsc() - tmp;
		}
		else
		{
			tmp = rdtsc();
			shm_pointer_release(thread, value); // just release
			if (profiling) profiling->counter6 += rdtsc() - tmp;
		}

		tmp = rdtsc();
		if (result)
			*result = element;
		// transaction_unlock_local(thread, &dict.local->lock, dict.shared, RESULT_OK, lock_taken1);
		transient_commit(thread);
		if (profiling) profiling->counter7 += rdtsc() - tmp;
		return RESULT_OK;
	};
}

int
shm_dict_set_empty(ThreadContext *thread, DictRef dict, ShmDictKey *key, DictProfiling *profiling)
{
	return shm_dict_set_consume(thread, dict, key, EMPTY_SHM, NULL, profiling);
}

int
shm_dict_get(ThreadContext *thread, DictRef dict, ShmDictKey *key,
		PShmPointer result_value, ShmDictElement **result_element)
{
	*result_value = EMPTY_SHM;
	*result_element = NULL;
	bool lock_taken = false;
 	if_failure(transaction_lock_read(thread, &dict.local->lock, dict.shared, CONTAINER_DICT_DELTA, &lock_taken),
	{
		transient_abort(thread);
		return status;
	});
	shm_cell_check_read_write_lock(thread, &dict.local->lock);
	ShmPointer root_shm = dict.local->data;
	ShmDictElementArray *root = LOCAL(dict.local->data);
	if (root)
	{
		uint32_t mask = ~((1 << DICT_LEVEL_BITS) - 1); // FFFFFFF8 for DICT_LEVEL_SIZE = 8, FFFFFFFC for DICT_LEVEL_SIZE = 4
		ShmPointer found_shm = EMPTY_SHM;
		DictFindData find_data;
		dict_init_find_data(&find_data);
		find_data.owned = shm_cell_have_write_lock(thread, &dict.local->lock);
		find_data.hash = key->hash;
		find_data.key = key->key;
		find_data.keysize = key->keysize;
		ShmDictElement *found = find_nested(root, root_shm, mask, 0, &find_data, &found_shm);

		if (found) {
			ShmPointer value = shm_dict_element_get_value(found, find_data.owned);
			if (value != EMPTY_SHM && value != DEBUG_SHM)
			{
				*result_value = value;
				*result_element = found;
			}
		}
	}
	// transaction_unlock_local(thread, &dict.local->lock, dict.shared, RESULT_OK, lock_taken);
	transient_commit(thread);
	return RESULT_OK;
}

void 
shm_dict_destroy(ThreadContext *thread, ShmDict *dict, ShmPointer dict_shm)
{
	// TBD
}

int
dict_nested_count(ShmDictElement *root, bool owned)
{
	if (root == NULL)
		return 0;
	int rslt = 0;
	for (int i = 0; i < DICT_LEVEL_SIZE; ++i)
	{
		if (shm_dict_element_get_value(&root[i], owned) != EMPTY_SHM)
			rslt++;
		ShmDictElement *nested = (ShmDictElement *)LOCAL(root[i].nested);
		if (nested)
			rslt += dict_nested_count(nested, owned);
	}
	return rslt;
}

int
shm_dict_get_count_debug(ThreadContext *thread, DictRef *dict)
{
	int rslt = 0;
	ShmDictElement *root = (ShmDictElement *)shm_pointer_to_pointer_no_side(dict->local->data);
	if (root)
	{
		rslt += dict_nested_count(root, shm_cell_have_write_lock(thread, &dict->local->lock));
		return rslt;
	}
	else
		return -1;
}

int
shm_dict_get_count(ThreadContext *thread, DictRef dict)
{
	int rslt = 0;
	ShmDictElement *root = (ShmDictElement *)LOCAL(dict.local->data);
	if (!root)
		return -1;
	rslt += dict_nested_count(root, shm_cell_have_write_lock(thread, &dict.local->lock));
	return rslt;
}

void
shm_dict_delta_clear(ThreadContext *thread, ShmDictDeltaArray *delta_array)
{
	delta_array->count = 0;
}

int
shm_dict_rollback(ThreadContext *thread, ShmDict *dict)
{
	shm_cell_check_write_lock(thread, &dict->lock);

	ShmDictDeltaArray *delta_array = LOCAL(dict->delta);
	if (delta_array)
	{
 		for (int i = 0; i < delta_array->count; ++i)
		{
			switch (delta_array->deltas[i].type)
			{
			case DICT_DELTA_NEW_ROOT: // created root array
				continue;
				break;
			case DICT_DELTA_NEW_NODE: // created nested array
			{
				ShmDictElement *array = LOCAL(delta_array->deltas[i].parent_element);
				if (!array)
				{
					return RESULT_FAILURE;
				}
				// free the new element on abort?
				break;
			}
			case DICT_DELTA_DEL_ROOT: // removed root array
				break;
			case DICT_DELTA_DEL_NODE: // removed nested array
				break;
			case DICT_DELTA_CHANGED:
			{
				// both value and element are not refcounted
				ShmDictElement *item = LOCAL(delta_array->deltas[i].parent_element);
				if (item->has_new_data && item->new_data == delta_array->deltas[i].value)
				{
					item->has_new_data = false;
					shm_pointer_move(thread, &item->data, &item->new_data); // swap pointer and free unused one
				}
				// otherwise deltas[i].value is probably freed already
				break;
			}
			}
		}
	}
	// OutputDebugStringA("delta rollback");
	return RESULT_OK;
}

int
shm_dict_commit(ThreadContext *thread, ShmDict *dict)
{
	shm_cell_check_write_lock(thread, &dict->lock);

	int rslt = RESULT_OK;
	// OutputDebugStringA("delta commit");
	ShmDictDeltaArray *delta_array = LOCAL(dict->delta);
	if (delta_array)
	{
 		for (int i = 0; i < delta_array->count; ++i)
		{
			switch (delta_array->deltas[i].type)
			{
			case DICT_DELTA_NEW_ROOT: // created root array
				continue;
				break;
			case DICT_DELTA_NEW_NODE: // created nested array
			{
				ShmDictElement *array = LOCAL(delta_array->deltas[i].parent_element);
				if (!array)
				{
					rslt = RESULT_FAILURE;
					break;
				}
				// do nothing, we only free the new element on abort
				break;
			}
			case DICT_DELTA_DEL_ROOT: // removed root array
				break;
			case DICT_DELTA_DEL_NODE: // removed nested array
				break;
			case DICT_DELTA_CHANGED:
			{
				ShmDictElement *item = LOCAL(delta_array->deltas[i].parent_element);
				// shmassert(item->new_data != item->data, NULL);
				if (item->has_new_data && item->new_data == delta_array->deltas[i].value)
				{
					item->has_new_data = false;
					shm_pointer_move(thread, &item->data, &item->new_data); // swap pointer and free unused one
				}
				// otherwise deltas[i].value is probably freed already
			}
			}
		}
	}

	// shm_dict_delta_clear(thread, dict);
	return rslt;
}

int
shm_dict_unlock(ThreadContext *thread, ShmDict *dict, ShmInt type)
{
	// shmassert(dict->lock.id == thread->self, "dict->lock.id == thread->self");
	if (TRANSACTION_ELEMENT_WRITE == type)
		p_atomic_shm_pointer_set(&dict->lock.transaction_data, EMPTY_SHM);
	ShmDictDeltaArray *delta_array = LOCAL(dict->delta);
	if (delta_array)
		shm_dict_delta_clear(thread, delta_array);
	_shm_cell_unlock(thread, &dict->lock, type);
	return RESULT_OK;
}

// end ShmDict

// start ShmUnDict
void
shm_undict_table_destroy(ThreadContext *thread, hash_table_header *header);
void
shm_undict_index_destroy(ThreadContext *thread, hash_table_index_header *index, bool deep);

ShmUnDict *
new_shm_undict(ThreadContext *thread, PShmPointer shm_pointer)
{
	// if (shm_pointer) *shm_pointer = EMPTY_SHM; - done by get_mem
	ShmUnDict *undict = get_mem(thread, shm_pointer, sizeof(ShmUnDict), SHM_UNDICT_DEBUG_ID);
	init_container((ShmContainer *)undict, sizeof(ShmUnDict), SHM_TYPE_UNDICT);
	undict->class_name = EMPTY_SHM;
	undict->buckets = EMPTY_SHM;
	undict->count = 0;
	undict->deleted_count = 0;
	undict->delta_buckets = EMPTY_SHM; // ShmUnDictTableHeader
	undict->delta_count = 0;
	undict->delta_deleted_count = 0;
	return undict;
}

int
_shm_dict_get_element_size(ShmInt type)
{
	if (type == SHM_TYPE_UNDICT_TABLE || type == SHM_TYPE_UNDICT_INDEX)
		return sizeof(hash_bucket);
	else if (type == SHM_TYPE_UNDICT_DELTA_TABLE || type == SHM_TYPE_UNDICT_DELTA_INDEX)
		return sizeof(hash_delta_bucket);
	else
	{
		shmassert_msg(false, "Invalid unordered dict table type");
		return 0;
	}
}

int
shm_dict_get_element_size(hash_table_header *header)
{
	return _shm_dict_get_element_size(header->type);
}

hash_table_header *
new_dict_table(ThreadContext *thread, ShmPointer *out_pntr, ShmInt type, int log_count, int item_size)
{
	type = type & (~1);
	shmassert(log_count > 0 && log_count < SHM_UNDICT_LOGCOUNT_LIMIT); // 64k * 4k
	*out_pntr = EMPTY_SHM;
	const int element_size = _shm_dict_get_element_size(type);
	shmassert(element_size == item_size);
	ShmWord block_total_size = isizeof(hash_table_header) - isizeof(hash_bucket) + element_size * (1 << log_count);
	if (block_total_size < 7000)
	{
		// simple in-place table
		hash_table_header *new_block = new_shm_refcounted_block(thread, out_pntr, block_total_size, type,
			type == SHM_TYPE_UNDICT_DELTA_TABLE ? SHM_UNDICT_DELTA_TABLE_DEBUG_ID : SHM_UNDICT_TABLE_DEBUG_ID);
		new_block->is_index = false;
		new_block->log_count = log_count;
		return new_block;
	}
	else
	{
		int log_index = 0;
		do {
			log_index++;
			block_total_size = isizeof(hash_table_header) - isizeof(hash_bucket) + element_size * (1 << (log_count - log_index));
		} while (block_total_size >= 7000);
		shmassert(log_index > 0 && log_index < 16); // 64k * 4k = 256M seems decent

		ShmWord index_total_size = isizeof(hash_table_index_header) - isizeof(ShmPointer) + isizeof(ShmPointer) * (1 << log_index);
		hash_table_index_header *new_block = new_shm_refcounted_block(thread, out_pntr, index_total_size, type | 1,
			type == SHM_TYPE_UNDICT_DELTA_TABLE ? SHM_UNDICT_DELTA_TABLE_INDEX_DEBUG_ID : SHM_UNDICT_TABLE_INDEX_DEBUG_ID);
		new_block->is_index = true;
		new_block->log_count = log_count;
		new_block->index_log_size = log_index;
		new_block->relocated = false;

		ShmPointer *index_blocks = &new_block->blocks;
		for (int idx = 0; idx < (1 << log_index); ++idx)
		{
			shmassert(index_blocks[idx] != guard_bytes);
			hash_table_header *subblock = new_shm_refcounted_block(thread, &index_blocks[idx], block_total_size, type,
				type == SHM_TYPE_UNDICT_DELTA_TABLE ? SHM_UNDICT_DELTA_TABLE_BLOCK_DEBUG_ID : SHM_UNDICT_TABLE_BLOCK_DEBUG_ID);
			subblock->log_count = log_count - log_index;
			subblock->is_index = false;
		}

		return (hash_table_header *)new_block;
	}
}

// ShmUndict

bool
shm_undict_get_table(ShmPointer p, hash_table_header **header_pntr, int item_size)
{
	hash_table_header *header = LOCAL(p);
	if (header_pntr)
		*header_pntr = header;

	if (header == NULL)
		return false;
	const int element_size = shm_dict_get_element_size(header);
	shmassert(element_size == item_size);

	return true;
}

bool
shm_undict_compare_false(hash_bucket *bucket, ShmUnDictKey *key)
{
	return false;
}

bool
shm_undict_compare_key1(hash_bucket *bucket, ShmUnDictKey *key)
{
	RefUnicode s = shm_ref_unicode_get(bucket->key);

	if (key->keysize != s.len)
		return false;
	else
	{
		for (int i = 0; i < s.len; ++i)
		{
			if (s.data[i] != key->key1[i])
				return false;
		}
		return true;
	}
}
bool
shm_undict_compare_key2(hash_bucket *bucket, ShmUnDictKey *key)
{
	RefUnicode s = shm_ref_unicode_get(bucket->key);

	if (key->keysize != s.len)
		return false;
	else
	{
		for (int i = 0; i < s.len; ++i)
		{
			if (s.data[i] != key->key2[i])
				return false;
		}
		return true;
	}
}
bool
shm_undict_compare_key4(hash_bucket *bucket, ShmUnDictKey *key)
{
	RefUnicode s = shm_ref_unicode_get(bucket->key);

	return key->keysize == s.len && memcmp(CAST_VL(s.data), key->key4, (size_t)(s.len * isizeof(Py_UCS4))) == 0;
}

bool
shm_undict_compare_ref_string(hash_bucket *bucket, ShmUnDictKey *key)
{
	RefUnicode s = shm_ref_unicode_get(bucket->key);
	RefUnicode s2 = shm_ref_unicode_get(key->key_shm);

	return s2.len == s.len && memcmp(CAST_VL(s.data), CAST_VL(s2.data), (size_t)(s.len * isizeof(Py_UCS4))) == 0;
}

typedef enum {
    key_invalid, key1, key2, key4, key_shm
} ShmUndictKeyType;

ShmUndictKeyType
shm_undict_key_type(ShmUnDictKey *key)
{
	if (key->key1)
	{
		shmassert(!key->key2);
		shmassert(!key->key4);
		shmassert(!SBOOL(key->key_shm));
		return key1;
	}
	else if (key->key2)
	{
		shmassert(!key->key1);
		shmassert(!key->key4);
		shmassert(!SBOOL(key->key_shm));
		return key2;
	}
	else if (key->key4)
	{
		shmassert(!key->key1);
		shmassert(!key->key2);
		shmassert(!SBOOL(key->key_shm));
		return key4;
	}
	else if (SBOOL(key->key_shm))
	{
		shmassert(!key->key1);
		shmassert(!key->key2);
		shmassert(!key->key4);
		return key_shm;
	}
	shmassert(false);
	return key_invalid;
}

hash_key_compare
shm_undict_key_compare_func(ShmUnDictKey *key)
{
	switch (shm_undict_key_type(key))
	{
	case key1:
		return shm_undict_compare_key1;
	case key2:
		return shm_undict_compare_key2;
	case key4:
		return shm_undict_compare_key4;
	case key_shm:
		return shm_undict_compare_ref_string;
	case key_invalid:
		shmassert(false);
	}
	shmassert(false);
	return NULL;
}

ShmPointer
shm_undict_key_to_ref_string(ThreadContext *thread, ShmUnDictKey *key)
{
	switch (shm_undict_key_type(key))
	{
	case key1:
		return shm_ref_unicode_new_ascii(thread, key->key1, key->keysize);
	case key2:
		shmassert_msg(false, "Not implemented");
		break;
	case key4:
		return shm_ref_unicode_new(thread, key->key4, key->keysize);
		break;
	case key_shm:
		shm_pointer_acq(thread, key->key_shm);
		return key->key_shm;
		break;
    case key_invalid:
		shmassert(false);
	}
	shmassert(false);
	return EMPTY_SHM;
}

void
shm_undict_validate_header(hash_table_header *header, bool is_delta)
{
	if (header->is_index)
		shmassert(header->type == (is_delta ? SHM_TYPE_UNDICT_DELTA_INDEX : SHM_TYPE_UNDICT_INDEX));
	else
		shmassert(header->type == (is_delta ? SHM_TYPE_UNDICT_DELTA_TABLE : SHM_TYPE_UNDICT_TABLE));

	shmassert(SHM_TYPE_RELEASE_MARK != (SHM_TYPE_RELEASE_MARK & (puint)header->type));
	// UnDict's blocks are always owned by the UnDict, so refcount can't become zero thus release_tag is always unchanged.
	shmassert(0 == header->release_count);
	shmassert(0 == header->revival_count);
}

// It's safe to pass a random itemindex here because its boundaries are verified.
// Returns End of Table status
bool
shm_undict_get_bucket_at_index(ThreadContext *thread, ShmUnDict *dict, int itemindex, ShmPointer *result_key, ShmPointer *result_value)
{
	// Doesn't support handling own transient transaction yet
	shmassert(thread->transaction_mode == TRANSACTION_PERSISTENT || thread->transaction_mode == TRANSACTION_TRANSIENT);
	if (itemindex < 0)
	{
		if (result_key) *result_key = EMPTY_SHM;
		if (result_value) *result_value = EMPTY_SHM;
		return true;
	}
	hash_table_header *header = LOCAL(dict->buckets);
	if (!header)
	{
		if (result_key) *result_key = EMPTY_SHM;
		if (result_value) *result_value = EMPTY_SHM;
		return true;
	}
	const int element_size = shm_dict_get_element_size(header);
	shmassert(element_size == sizeof(hash_bucket));
	if (itemindex >= (1 << header->log_count))
	{
		if (result_key) *result_key = EMPTY_SHM;
		if (result_value) *result_value = EMPTY_SHM;
		return true;
	}
	hash_bucket *item = get_bucket_at_index(header, itemindex, element_size);

	switch (bucket_get_state(item))
	{
	case HASH_BUCKET_STATE_DELETED_RESERVED:
		if (result_key) *result_key = EMPTY_SHM;
		if (result_value) *result_value = EMPTY_SHM;
		return false;
		break;
	case HASH_BUCKET_STATE_SET:
	{
		ShmPointer key = item->key;
		ShmPointer value = item->value;
		if (result_key)
		{
			if (SBOOL(key))
				shm_pointer_acq(thread, key);
			*result_key = key;
		}
		if (result_value)
		{
			if (SBOOL(value))
				shm_pointer_acq(thread, value);
			*result_value = value;
		}
		return false;

		break;
	}
	case HASH_BUCKET_STATE_DELETED:
	case HASH_BUCKET_STATE_EMPTY:
		if (result_key) *result_key = EMPTY_SHM;
		if (result_value) *result_value = EMPTY_SHM;
		return false;
		break;
	default:
		shmassert(false);
		if (result_key) *result_key = EMPTY_SHM;
		if (result_value) *result_value = EMPTY_SHM;
		return true;
	}
}

void
shm_undict_grow_table(ThreadContext *thread, ShmUnDict *dict, bool is_delta, int increment, int item_size,
		hash_table_header **created_header, volatile ShmInt *created_bucket_count)
{
	ShmPointer old_buckets;
	if (is_delta)
		old_buckets = dict->delta_buckets;
	else
		old_buckets = dict->buckets;

	hash_table_header *header = LOCAL(old_buckets);
	shm_undict_validate_header(header, is_delta);
	const int element_size = shm_dict_get_element_size(header);
	shmassert(element_size == item_size);
	shmassert(header->log_count < SHM_UNDICT_LOGCOUNT_LIMIT);
	if (!header->is_index)
	{
		const int old_size = isizeof(hash_table_header) - isizeof(hash_bucket) + element_size * (1 << header->log_count);
		shmassert(header->size == old_size);
	}

	shmassert(increment > 0 && increment < SHM_UNDICT_LOGCOUNT_LIMIT);
	int new_log_count = header->log_count + increment;
	shmassert(new_log_count < SHM_UNDICT_LOGCOUNT_LIMIT);
	ShmPointer new_table_shm;
	hash_table_header *new_header = new_dict_table(thread, &new_table_shm, header->type, new_log_count, element_size);

	hash_func_args new_table_args;
	ShmInt new_count = 0;
	ShmInt new_deleted_count = 0;
	new_table_args.thread = thread;
	new_table_args.bucket_item_size = element_size;
	new_table_args.header = new_header;
	new_table_args.item_count = &new_count;
	new_table_args.deleted_count = &new_deleted_count;
	new_table_args.bucket_count = 1 << new_log_count;
	new_table_args.compare_key = shm_undict_compare_false; // We just copy, we don't need strict comparision.

	ShmUnDictKey key = EMPTY_SHM_UNDICT_KEY;

	p_atomic_int_set(&header->relocated, true);

	for (long src_index = 0; src_index < (1 << header->log_count); ++src_index)
	{
		hash_bucket *src_item = get_bucket_at_index(header, src_index, element_size);

		bool is_deleted = false;
		switch (bucket_get_state(src_item))
		{
		case HASH_BUCKET_STATE_DELETED_RESERVED:
			shmassert(is_delta);
			is_deleted = true;
			// pass through
		case HASH_BUCKET_STATE_SET:
		{
			key.hash = src_item->key_hash;
			find_position_result find_rslt = hash_find_position(new_table_args, &key);
			shmassert(find_rslt.found == -1);
			shmassert(find_rslt.last_free >= 0);
			shmassert(find_rslt.found != -2);
			hash_bucket *new_item = get_bucket_at_index(new_table_args.header, find_rslt.last_free, new_table_args.bucket_item_size);
			shm_pointer_move_atomic(thread, &new_item->key, &src_item->key);
			shmassert(shm_pointer_refcount(thread, new_item->key) > 0);
			shm_pointer_move_atomic(thread, &new_item->key_hash, &src_item->key_hash);
			shm_pointer_move_atomic(thread, &new_item->value, &src_item->value);
			if (is_delta)
				((hash_delta_bucket *)new_item)->orig_item = ((hash_delta_bucket *)src_item)->orig_item;

			if (is_deleted)
				new_deleted_count++;
			else
				new_count++;

			break;
		}
		case HASH_BUCKET_STATE_DELETED:
			// skip
			shmassert(SBOOL(src_item->key) == false);
			break;
		case HASH_BUCKET_STATE_EMPTY:
			// skip
			break;
		default:
			shmassert(false);
		}
	}
	// shm_undict_debug_print(thread, dict);

	// we've already trashed the table -- might be a good idea to unlink the index->table references
	if (header->is_index)
		shm_undict_index_destroy(thread, (hash_table_index_header *)header, false);

	if (is_delta)
	{
		shmassert(new_count == dict->delta_count);
		shmassert(new_deleted_count == dict->delta_deleted_count);
		shm_pointer_move(thread, &dict->delta_buckets, &new_table_shm);
	}
	else
	{
		shmassert(new_count == dict->count);
		shmassert(new_deleted_count == 0);
		shm_pointer_move(thread, &dict->buckets, &new_table_shm);
	}

	if (created_header)
		*created_header = new_header;
	if (created_bucket_count)
		*created_bucket_count = new_table_args.bucket_count;
}

bool
grow_or_allocate_delta(ThreadContext *thread, ShmUnDict *dict, hash_func_args *delta_args, hash_table_header **delta_header)
{
	if (*delta_args->item_count + *delta_args->deleted_count > delta_args->bucket_count / 2 + 1)
	{
		shmassert(delta_args->header);
		shm_undict_grow_table(thread, dict, true, 1, delta_args->bucket_item_size, &delta_args->header, &delta_args->bucket_count);
		return true;
	}
	shmassert((delta_args->header != NULL) == (*delta_header != NULL));
	if (!delta_args->header)
	{
		new_dict_table(thread, &dict->delta_buckets, SHM_TYPE_UNDICT_DELTA_TABLE, 2, sizeof(hash_delta_bucket)); // 4 items by default
		shm_undict_get_table(dict->delta_buckets, delta_header, delta_args->bucket_item_size);
		delta_args->bucket_count = 1 << (*delta_header)->log_count;
		return true;
	}
	return false;
}

int
shm_undict_set_do(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key,
    ShmPointer value, bool consume, DictProfiling *profiling)
{
	bool lock_taken1 = false;
	uint64_t tmp;
	tmp = rdtsc();
	if (profiling) profiling->counter1 += rdtsc() - tmp;
	tmp = rdtsc();
	bool had_write_lock = shm_cell_have_write_lock(thread, &dict.local->lock);
	if_failure(transaction_lock_write(thread, &dict.local->lock, dict.shared, CONTAINER_UNORDERED_DICT, &lock_taken1),
	{
		if (SBOOL(value) && consume)
			shm_pointer_release(thread, value);
		transient_abort(thread);
		return status;
	});
	shm_cell_check_write_lock(thread, &dict.local->lock);
	if (!had_write_lock)
	{
		shmassert(dict.local->delta_buckets == EMPTY_SHM);
		shmassert(dict.local->delta_count == 0);
		shmassert(dict.local->delta_deleted_count == 0);
	}
	if (profiling) profiling->counter2 += rdtsc() - tmp;
	tmp = rdtsc();
	{
		//ShmDictElement *element = shm_dict_find(thread, dict, hash, key, keysize));
		//if ( ! element)
		hash_func_args delta_args;
		delta_args.thread = thread;
		delta_args.bucket_item_size = sizeof(hash_delta_bucket);
		shm_undict_get_table(dict.local->delta_buckets, &delta_args.header, delta_args.bucket_item_size);
		delta_args.item_count = &dict.local->delta_count;
		delta_args.deleted_count = &dict.local->delta_deleted_count;
		delta_args.bucket_count = delta_args.header ? (1 << delta_args.header->log_count) : 0;
		delta_args.compare_key = shm_undict_key_compare_func(key);
		hash_func_args orig_args;
		orig_args.thread = thread;
		orig_args.bucket_item_size = sizeof(hash_bucket);
		shm_undict_get_table(dict.local->buckets, &orig_args.header, orig_args.bucket_item_size);
		orig_args.item_count = &dict.local->delta_count;
		orig_args.deleted_count = &dict.local->delta_deleted_count;
		orig_args.bucket_count = orig_args.header ? (1 << orig_args.header->log_count) : 0;
		orig_args.compare_key = shm_undict_key_compare_func(key);
		bool modified = false;
		{
			find_position_result rslt_delta = { -1, -1 };
			hash_delta_bucket *found_delta_bucket = NULL;
			hash_delta_bucket *new_delta_bucket = NULL;
			shmassert(SBOOL(dict.local->delta_buckets) == (delta_args.header != NULL));
			if (delta_args.header)
			{
				if (*delta_args.item_count + *delta_args.deleted_count > delta_args.bucket_count / 2 + 1)
					shm_undict_grow_table(thread, dict.local, true, 1, delta_args.bucket_item_size, &delta_args.header, &delta_args.bucket_count);
				for (int cycle = 1; cycle <= 2; ++cycle)
				{
					rslt_delta = hash_find_position(delta_args, key);

					if (rslt_delta.found >= 0)
					{
						// replacing previously modified item
						shmassert(rslt_delta.found < delta_args.bucket_count);
						found_delta_bucket = (hash_delta_bucket *)
							get_bucket_at_index(delta_args.header, rslt_delta.found, delta_args.bucket_item_size);
					}
					else if (rslt_delta.last_free >= 0)
					{
						shmassert(rslt_delta.last_free < delta_args.bucket_count);
						new_delta_bucket = (hash_delta_bucket *)
							get_bucket_at_index(delta_args.header, rslt_delta.last_free, delta_args.bucket_item_size);
					}

					if (rslt_delta.found == -2)
					{
						shmassert(cycle != 2);
						shmassert(!found_delta_bucket && !new_delta_bucket);
						shm_undict_grow_table(thread, dict.local, true, 1, delta_args.bucket_item_size, &delta_args.header, &delta_args.bucket_count);
					}
					if (rslt_delta.found == -1 && rslt_delta.last_free == -1)
						break;
				}
			}
			shmassert(rslt_delta.found > -2);
			if (found_delta_bucket)
			{
				switch (bucket_get_state((hash_bucket*)found_delta_bucket))
				{
				case HASH_BUCKET_STATE_SET:
				case HASH_BUCKET_STATE_DELETED_RESERVED:
				{
					// update the existing delta item
					bool was_valid = found_delta_bucket->value != EMPTY_SHM;
					bool set_valid = value != EMPTY_SHM;
					if (consume)
						shm_pointer_move(thread, &found_delta_bucket->value, &value);
					else
						shm_pointer_copy(thread, &found_delta_bucket->value, value);
					if (was_valid != set_valid)
					{
						if (was_valid)
						{
							(*delta_args.deleted_count)++;
							(*delta_args.item_count)--;
						}
						else
						{

							(*delta_args.deleted_count)--;
							(*delta_args.item_count)++;
						}
					}

					modified = true;
					break;
				}
				default:
					shmassert(false);
				}
			}
			else
			{
				find_position_result persist_probe = hash_find_position(orig_args, key);
				if (persist_probe.found >= 0)
				{
					if (grow_or_allocate_delta(thread, dict.local, &delta_args, &delta_args.header)) {
						find_position_result new_delta_probe = hash_find_position(delta_args, key);
						shmassert(new_delta_probe.found == -1 && new_delta_probe.last_free >= 0);
						new_delta_bucket = (hash_delta_bucket *)
							get_bucket_at_index(delta_args.header, new_delta_probe.last_free, delta_args.bucket_item_size);
					}
					shmassert(new_delta_bucket);
					shmassert(persist_probe.found < orig_args.bucket_count);
					hash_bucket *bucket = get_bucket_at_index(orig_args.header, persist_probe.found, orig_args.bucket_item_size);
					// create a linked item in the dict.local->delta_buckets
					shmassert(bucket_get_state(bucket) == HASH_BUCKET_STATE_SET);
					// validate_bucket(bucket); - done by hash_find_position
					shm_pointer_copy(thread, &new_delta_bucket->key, bucket->key);
					new_delta_bucket->key_hash = bucket->key_hash;
					if (consume)
						shm_pointer_move(thread, &new_delta_bucket->value, &value);
					else
						shm_pointer_copy(thread, &new_delta_bucket->value, value);
					new_delta_bucket->orig_item = persist_probe.found;

					if (value == EMPTY_SHM)
						delta_args.deleted_count++;
					else
						delta_args.item_count++;

					modified = true;
				}
				else
				{
					if (value != EMPTY_SHM)
					{
						// create a brand new item in the dict.local->delta_buckets
						if (grow_or_allocate_delta(thread, dict.local, &delta_args, &delta_args.header)) {
							find_position_result new_delta_rslt = hash_find_position(delta_args, key);
							shmassert(new_delta_rslt.found == -1 && new_delta_rslt.last_free >= 0);
							new_delta_bucket = (hash_delta_bucket *)
								get_bucket_at_index(delta_args.header, new_delta_rslt.last_free, delta_args.bucket_item_size);
						}
						new_delta_bucket->key = shm_undict_key_to_ref_string(thread, key);
						new_delta_bucket->key_hash = key->hash;
						if (consume)
							shm_pointer_move(thread, &new_delta_bucket->value, &value);
						else
							shm_pointer_copy(thread, &new_delta_bucket->value, value);
						new_delta_bucket->orig_item = -1;

						dict.local->delta_count++;
						modified = true;
					}
				}
			}
		}
		if (profiling) profiling->counter3 += rdtsc() - tmp;
		if (modified)
		{
			tmp = rdtsc();
			// we probably need to verify the dict.lical->lock.changes_shm value
			// shmassert(shm_dict_push_delta(thread, &dict.local->delta, DICT_DELTA_CHANGED, value, element_shm));
		}
		if (SBOOL(value))
		{
			tmp = rdtsc();
			// release in case the value was not moved
			if (consume)
				shm_pointer_release(thread, value);
			if (profiling) profiling->counter6 += rdtsc() - tmp;
		}

		tmp = rdtsc();
		transient_commit(thread);
		if (profiling) profiling->counter7 += rdtsc() - tmp;
		return RESULT_OK;
	};
}

int
shm_undict_consume_item(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key,
		ShmPointer value, DictProfiling *profiling)
{
	return shm_undict_set_do(thread, dict, key, value, true, profiling);
}

int
shm_undict_set_item(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key,
		ShmPointer value, DictProfiling *profiling)
{
	return shm_undict_set_do(thread, dict, key, value, false, profiling);
}

int
shm_undict_set_empty(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key,
		DictProfiling *profiling)
{
	return shm_undict_set_do(thread, dict, key, EMPTY_SHM, true, profiling);
}

void
release_delta_table(ThreadContext *thread, ShmUnDict *dict, bool invalidated);
void
release_persistent_table(ThreadContext *thread, ShmUnDict *dict);

// kinda short version of shm_undict_commit
int
shm_undict_set_item_raw(ThreadContext *thread, ShmUnDict *dict, ShmUnDictKey *key, ShmPointer value, bool consume)
{
	shmassert(dict->delta_buckets == EMPTY_SHM);
	shmassert(dict->delta_count == 0);
	shmassert(dict->delta_deleted_count == 0);

	hash_func_args persist_args;
	persist_args.thread = thread;
	persist_args.bucket_item_size = sizeof(hash_bucket);
	if (!SBOOL(dict->buckets))
		new_dict_table(thread, &dict->buckets, SHM_TYPE_UNDICT_TABLE, 2, sizeof(hash_bucket)); // 4 items by default

	shm_undict_get_table(dict->buckets, &persist_args.header, persist_args.bucket_item_size);
	shmassert(persist_args.header);
	persist_args.item_count = &dict->count;
	persist_args.deleted_count = &dict->deleted_count;
	persist_args.bucket_count = 1 << persist_args.header->log_count;
	persist_args.compare_key = shm_undict_compare_ref_string;

	if (*persist_args.item_count + *persist_args.deleted_count > persist_args.bucket_count / 2 + 1)
		shm_undict_grow_table(thread, dict, false, 1, persist_args.bucket_item_size, &persist_args.header, &persist_args.bucket_count);

	find_position_result persist_rslt = { .found = -1, .last_free = -1 };
	for (int cycle = 1; cycle <= 4; ++cycle)
	{
		shmassert(cycle < 4);

		persist_rslt = hash_find_position(persist_args, key);
		if (persist_rslt.found == -2)
			shm_undict_grow_table(thread, dict, false, 1, persist_args.bucket_item_size, &persist_args.header, &persist_args.bucket_count);
		else
			break;
	}
	shmassert(persist_rslt.found > -2);

	shmassert(persist_rslt.found == -1 && persist_rslt.last_free >= 0); // we append only
	if (value != EMPTY_SHM)
	{
		// allocate a new persistent item
		hash_bucket *last_free = get_bucket_at_index(persist_args.header, persist_rslt.last_free, sizeof(hash_bucket));
		if (consume)
			shm_pointer_move(thread, &last_free->value, &value);
		else
			shm_pointer_copy(thread, &last_free->value, value);

		last_free->key = shm_undict_key_to_ref_string(thread, key);
		last_free->key_hash = key->hash;
		(*persist_args.item_count)++;
	}

	return RESULT_OK;
}

int
shm_undict_commit(ThreadContext *thread, ShmUnDict *dict)
{
	shm_cell_check_write_lock(thread, &dict->lock);
	// move data from delta into persistent table
	hash_table_header *delta_header;
	if (shm_undict_get_table(dict->delta_buckets, &delta_header, sizeof(hash_delta_bucket)))
	{
		bool persistent_relocated = false;
		hash_func_args persist_args;
		persist_args.thread = thread;
		persist_args.bucket_item_size = sizeof(hash_bucket);
		if (!SBOOL(dict->buckets))
		{
			new_dict_table(thread, &dict->buckets, SHM_TYPE_UNDICT_TABLE, 2, sizeof(hash_bucket)); // 4 items by default
			persistent_relocated = true;
		}
		shm_undict_get_table(dict->buckets, &persist_args.header, persist_args.bucket_item_size);
		shmassert(persist_args.header);
#pragma message ("dict->count is probably not calculated correctly")
		persist_args.item_count = &dict->count;
		persist_args.deleted_count = &dict->deleted_count;
		persist_args.bucket_count = 1 << persist_args.header->log_count;
		persist_args.compare_key = shm_undict_compare_ref_string;
		ShmUnDictKey key = EMPTY_SHM_UNDICT_KEY;

		for (int delta_index = 0; delta_index < (1 << delta_header->log_count); ++delta_index)
		{
			if (*persist_args.item_count + *persist_args.deleted_count > persist_args.bucket_count / 2 + 1)
			{
				shm_undict_grow_table(thread, dict, false, 1, persist_args.bucket_item_size, &persist_args.header, &persist_args.bucket_count);
				persistent_relocated = true;
			}

			hash_delta_bucket *delta_bucket = (hash_delta_bucket *)get_bucket_at_index(delta_header, delta_index, sizeof(hash_delta_bucket));
			switch (bucket_get_state((hash_bucket *)delta_bucket))
			{
			case HASH_BUCKET_STATE_DELETED_RESERVED:
			case HASH_BUCKET_STATE_SET:
			{
				// if (*persist_args.item_count + *persist_args.deleted_count > persist_args.bucket_count / 2 + 1)
				//      shm_undict_grow_table(thread, dict, false, 1, persist_args.bucket_item_size, &persist_args.header, &persist_args.bucket_count);

				find_position_result persist_rslt = { .found = -1, .last_free = -1 };
				key.hash = delta_bucket->key_hash;
				key.key_shm = delta_bucket->key;
				for (int cycle = 1; cycle <= 4; ++cycle)
				{
					shmassert(cycle < 4);

					persist_rslt = hash_find_position(persist_args, &key);
					if (persist_rslt.found == -2)
					{
						shm_undict_grow_table(thread, dict, false, 1, persist_args.bucket_item_size, &persist_args.header, &persist_args.bucket_count);
						persistent_relocated = true;
					}
					else
						break;
				}
				shmassert(persist_rslt.found > -2);
				if (delta_bucket->orig_item >= 0)
				{
					shmassert(persist_rslt.found >= 0);
					if (!persistent_relocated)
						shmassert(persist_rslt.found == delta_bucket->orig_item);

					hash_bucket *found_bucket = get_bucket_at_index(persist_args.header, persist_rslt.found, sizeof(hash_bucket));

					shmassert(bucket_get_state(found_bucket) == HASH_BUCKET_STATE_SET);
					int persist_state = bucket_get_state(found_bucket);
					bool was_valid = persist_state == HASH_BUCKET_STATE_SET;
					bool set_valid = delta_bucket->value != EMPTY_SHM;
					// modification
					shm_pointer_copy(thread, &found_bucket->value, delta_bucket->value);
					// shm_pointer_empty(thread, &delta_buckets[delta_index].key);
					delta_bucket->key_hash = 1; // effectively making it HASH_BUCKET_STATE_DELETED, which is invalid for delta table.

					if (was_valid != set_valid)
					{
						if (was_valid)
						{
							(*persist_args.deleted_count)++;
							(*persist_args.item_count)--;
						}
						else
						{
							(*persist_args.item_count)++;
							if (persist_state == HASH_BUCKET_STATE_DELETED)
								(*persist_args.deleted_count)--;
						}
					}
					if (set_valid == false && was_valid)
					{
						// The element is in temporary HASH_BUCKET_STATE_DELETED_RESERVED state
						uint32_t hash = found_bucket->key_hash;
						 // Save the hash because it's gonna be modified
						ShmPointer deleted_key = bucket_delete(found_bucket);
						found_bucket = NULL;
						shmassert(shm_pointer_refcount(thread, deleted_key) > 0);
						shm_pointer_release(thread, deleted_key);
						// now the persist_rslt.found's state is HASH_BUCKET_STATE_DELETED
						hash_compact_tail(persist_args, persist_rslt.found, hash);
					}
				}
				else
				{
					shmassert(persist_rslt.last_free >= 0);
					if (delta_bucket->value != EMPTY_SHM)
					{
						// allocate a new persistent item
						hash_bucket *last_free = get_bucket_at_index(persist_args.header, persist_rslt.last_free, sizeof(hash_bucket));
						// modification
						shm_pointer_copy(thread, &last_free->value, delta_bucket->value);
						shm_pointer_copy(thread, &last_free->key, delta_bucket->key);
						shmassert(shm_pointer_refcount(thread, delta_bucket->key) >= 2);
						last_free->key_hash = delta_bucket->key_hash;
						(*persist_args.item_count)++;
						delta_bucket->key_hash = 1; // effectively making it HASH_BUCKET_STATE_DELETED, which is invalid for delta table.
					}
				}
				break;
			}
			case HASH_BUCKET_STATE_EMPTY:
				// skip
				break;
			default:
				shmassert(false);
			}
		}

		dict->delta_count = 0;
		dict->delta_deleted_count = 0;
		release_delta_table(thread, dict, true);
		shm_pointer_empty(thread, &dict->delta_buckets);
	}
	else if (dict->delta_buckets == NONE_SHM)
	{
		release_persistent_table(thread, dict);
		dict->delta_buckets = EMPTY_SHM;
	}
	shmassert(dict->delta_buckets == EMPTY_SHM);
	shmassert(dict->delta_count == 0);
	shmassert(dict->delta_deleted_count == 0);
	shm_cell_check_write_lock(thread, &dict->lock);
	return RESULT_OK;
}

int
shm_undict_rollback(ThreadContext *thread, ShmUnDict *dict)
{
	shm_cell_check_write_lock(thread, &dict->lock);
	// clear the delta table and release the values (and keys if not owned by persistent table)
	release_delta_table(thread, dict, false);
	shmassert(dict->delta_buckets == EMPTY_SHM);
	shmassert(dict->delta_count == 0);
	shmassert(dict->delta_deleted_count == 0);
	return RESULT_OK;
}

int
shm_undict_unlock(ThreadContext *thread, ShmUnDict *dict, ShmInt type)
{
	// shmassert(dict->lock.id == thread->self, "dict->lock.id == thread->self");
	if (TRANSACTION_ELEMENT_WRITE == type)
		p_atomic_shm_pointer_set(&dict->lock.transaction_data, EMPTY_SHM);
	_shm_cell_unlock(thread, &dict->lock, type);
	return RESULT_OK;
}

int
_shm_undict_get_count(ThreadContext *thread, UnDictRef dict, ShmInt *rslt, bool commit)
{
	if_failure(transaction_lock_read(thread, &dict.local->lock, dict.shared, CONTAINER_UNORDERED_DICT, NULL),
	{
		transient_abort(thread);
		return status;
	});
	shm_cell_check_read_lock(thread, &dict.local->lock);

	*rslt = p_atomic_int_get(&dict.local->count);

	if (commit)
		transient_commit(thread);
	return RESULT_OK;
}

int
shm_undict_get_count(ThreadContext *thread, UnDictRef dict, ShmInt *rslt)
{
	return _shm_undict_get_count(thread, dict, rslt, true);
}

int
shm_undict_get_do(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key, ShmPointer *value, bool acquire)
{
	shmassert(value);
	shmassert(*value == EMPTY_SHM);
	if_failure(transaction_lock_read(thread, &dict.local->lock, dict.shared, CONTAINER_UNORDERED_DICT, NULL),
	{
		transient_abort(thread);
		return status;
	});
	shm_cell_check_read_lock(thread, &dict.local->lock);

	if (shm_cell_have_write_lock(thread, &dict.local->lock))
	{
		// search the item in the delta table first
		hash_func_args delta_args;
		delta_args.thread = thread;
		delta_args.bucket_item_size = sizeof(hash_delta_bucket);
		shm_undict_get_table(dict.local->delta_buckets, &delta_args.header, delta_args.bucket_item_size);
		delta_args.item_count = &dict.local->delta_count;
		delta_args.deleted_count = &dict.local->delta_deleted_count;
		delta_args.bucket_count = delta_args.header ? (1 << delta_args.header->log_count) : 0;
		delta_args.compare_key = shm_undict_key_compare_func(key);

		find_position_result delta_rslt = hash_find_position(delta_args, key);
		if (delta_rslt.found >= 0)
		{
			hash_delta_bucket *found = (hash_delta_bucket *)get_bucket_at_index(delta_args.header, delta_rslt.found, delta_args.bucket_item_size);
			ShmInt state = bucket_get_state((hash_bucket *)found);
			shmassert(state == HASH_BUCKET_STATE_SET || state == HASH_BUCKET_STATE_DELETED_RESERVED);
			ShmPointer retval = EMPTY_SHM;
			if (state == HASH_BUCKET_STATE_SET)
			{
				retval = found->value;
				if (acquire)
				{
					if (SBOOL(retval))
						shm_pointer_acq(thread, retval);
				}
				else
					shmassert_msg(thread->transaction_mode != TRANSACTION_TRANSIENT,
					             "Handling pointer within a transient transaction without incrementing refcount leads to undefined behaivour.");
			}
			*value = retval;
			transient_commit(thread);
			return RESULT_OK;
		}
	}
	else
		shmassert(EMPTY_SHM == dict.local->delta_buckets);

	hash_func_args persist_args;
	persist_args.thread = thread;
	persist_args.bucket_item_size = sizeof(hash_bucket);
	shm_undict_get_table(dict.local->buckets, &persist_args.header, persist_args.bucket_item_size);
	persist_args.item_count = &dict.local->count;
	persist_args.deleted_count = &dict.local->deleted_count;
	persist_args.bucket_count = persist_args.header ? (1 << persist_args.header->log_count) : 0;
	persist_args.compare_key = shm_undict_key_compare_func(key);

	ShmPointer retval2 = EMPTY_SHM;
	find_position_result persist_rslt = hash_find_position(persist_args, key);
	if (persist_rslt.found >= 0)
	{
		hash_bucket *found = get_bucket_at_index(persist_args.header, persist_rslt.found, persist_args.bucket_item_size);
		ShmInt state = bucket_get_state(found);
		shmassert(state == HASH_BUCKET_STATE_SET);
		retval2 = found->value;
		if (acquire)
		{
			if (SBOOL(retval2))
				shm_pointer_acq(thread, retval2);
		}
		else
			shmassert_msg(thread->transaction_mode != TRANSACTION_TRANSIENT,
			             "Handling pointer within a transient transaction without incrementing refcount leads to undefined behaivour.");
	}
	*value = retval2;

	transient_commit(thread);
	return RESULT_OK;
}

int
shm_undict_get(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key, ShmPointer *value)
{
	return shm_undict_get_do(thread, dict, key, value, false);
}

int
shm_undict_acq(ThreadContext *thread, UnDictRef dict, ShmUnDictKey *key, ShmPointer *value)
{
	return shm_undict_get_do(thread, dict, key, value, true);
}

volatile char *
shm_undict_debug_scan_for_item(ShmUnDict *dict, ShmUnDictKey *key)
{
	hash_table_header *header;
	shm_undict_get_table(dict->buckets, &header, sizeof(hash_bucket));
	int item_count = (1 << header->log_count);
	for (int idx = 0; idx < item_count; ++idx)
	{
		hash_bucket *bucket = get_bucket_at_index(header, idx, sizeof(hash_bucket));
		if (bucket->key_hash == key->hash)
			return (volatile char *)bucket;
	}
	return NULL;
}

void
print_items_to_file(FILE *pFile, ShmUnDict *dict)
{
	hash_table_header *header;
	shm_undict_get_table(dict->buckets, &header, sizeof(hash_bucket));
	int bucket_count = (1 << header->log_count);
	for (int idx = 0; idx < bucket_count; ++idx)
	{
		hash_bucket *bucket = get_bucket_at_index(header, idx, sizeof(hash_bucket));
		RefUnicode s = shm_ref_unicode_get(bucket->key);
		if (s.data)
		{
			for (int i = 0; i < s.len; ++i)
				fputc((unsigned char)s.data[i], pFile);
			if (SBOOL(bucket->value))
			{
				fprintf(pFile, ": %d\n", bucket->value);
			}
			else
				fprintf(pFile, ": empty\n");
		}
		else
			fprintf(pFile, "none\n");
	}
}


void
shm_undict_debug_print(ThreadContext *thread, ShmUnDict *dict, bool full)
{
	hash_table_header *persist_header;
	shm_undict_get_table(dict->buckets, &persist_header, sizeof(hash_bucket));

	if (full)
		printf("===========================\n");
	if (!persist_header)
	{
		printf("EMPTY\n");
		return;
	}
	int count = 0;
	for (int index = 0; index < (1 << persist_header->log_count); ++index)
	{
		hash_bucket *bucket = get_bucket_at_index(persist_header, index, sizeof(hash_bucket));
		if (bucket_get_state(bucket) == HASH_BUCKET_STATE_SET)
		{
			count++;
			if (full)
			{
				RefUnicode s = shm_ref_unicode_get(bucket->key);
				ShmValueHeader *val = LOCAL(bucket->value);
				const char *data = shm_value_get_data(val);
				for (int i = 0; i < s.len; ++i)
					putchar((unsigned char)s.data[i]);
				putchar('=');
				// for (int i = 0; i < shm_value_get_length(val); ++i)
				//	putchar(data[i]);
				printf("%s", data);
				putchar('\n');
			}
		}
	}
	printf("count: %d\n", count);
	if (full)
		printf("===========================\n");
}

void
release_delta_table(ThreadContext *thread, ShmUnDict *dict, bool invalidated)
{
	hash_table_header *delta_header;
	if (shm_undict_get_table(dict->delta_buckets, &delta_header, sizeof(hash_delta_bucket)))
	{
		for (int delta_index = 0; delta_index < (1 << delta_header->log_count); ++delta_index)
		{
			hash_bucket *bucket = get_bucket_at_index(delta_header, delta_index, sizeof(hash_delta_bucket));
			shmassert(bucket->key != ((__ShmPointer)-1));
			switch (bucket_get_state(bucket))
			{
			case HASH_BUCKET_STATE_DELETED_RESERVED:
			case HASH_BUCKET_STATE_SET:
				if (SBOOL(bucket->value))
					shm_pointer_empty(thread, &bucket->value);
				if (SBOOL(bucket->key))
				{
					shm_pointer_empty(thread, &bucket->key);
					bucket->key = 0; // memset-friendly
				}
				break;
			case HASH_BUCKET_STATE_DELETED:
				shmassert_msg(invalidated, "state = HASH_BUCKET_STATE_DELETED");
				break;
			case HASH_BUCKET_STATE_EMPTY:
				break;
			default:
				shmassert(false);
			}
		}
		dict->delta_count = 0;
		dict->delta_deleted_count = 0;
		shm_pointer_empty(thread, &dict->delta_buckets);
	}
}

void
release_persistent_table(ThreadContext *thread, ShmUnDict *dict)
{
	hash_table_header *header;
	if (shm_undict_get_table(dict->buckets, &header, sizeof(hash_bucket)))
	{
		for (int index = 0; index < (1 << header->log_count); ++index)
		{
			hash_bucket *bucket = get_bucket_at_index(header, index, sizeof(hash_bucket));
			shmassert(bucket->key != ((__ShmPointer)-1));
			switch (bucket_get_state(bucket))
			{
			case HASH_BUCKET_STATE_DELETED_RESERVED:
				shmassert(false);
				break;
			case HASH_BUCKET_STATE_DELETED:
			case HASH_BUCKET_STATE_EMPTY:
				break;
			case HASH_BUCKET_STATE_SET:
				shm_pointer_empty(thread, &bucket->value);
				if (SBOOL(bucket->key))
				{
					shm_pointer_empty(thread, &bucket->key);
					bucket->key = 0; // memset-friendly
				}
				break;
			default:
				shmassert(false);
			}
		}
		dict->count = 0;
		dict->deleted_count = 0;
		shm_pointer_empty(thread, &dict->buckets);
	}
}

int
shm_undict_clear(ThreadContext *thread, UnDictRef dict)
{
	if_failure(transaction_lock_write(thread, &dict.local->lock, dict.shared, CONTAINER_UNORDERED_DICT, NULL),
	{
		transient_abort(thread);
		return status;
	});
	release_delta_table(thread, dict.local, false);

	shmassert(dict.local->delta_buckets == EMPTY_SHM);
	shmassert(dict.local->delta_count == 0);
	shmassert(dict.local->delta_deleted_count == 0);

	dict.local->delta_buckets = NONE_SHM;

	return RESULT_OK;
}

void
shm_undict_destroy(ThreadContext *thread, ShmUnDict *dict, ShmPointer dict_shm)
{
	// Clear all references in data items so we won't need several more cycles to dereference them
	// (after destroying the block and index itself)
	hash_table_header *persistent = LOCAL(dict->buckets);
	if (persistent)
	{
		shm_undict_validate_header(persistent, false);
		if (persistent->is_index)
			shm_undict_index_destroy(thread, (hash_table_index_header *)persistent, true);
		else
			shm_undict_table_destroy(thread, persistent);
	}
	if (dict->buckets != EMPTY_SHM)
		shm_pointer_empty_atomic(thread, &dict->buckets);

	hash_table_header *delta = LOCAL(dict->delta_buckets);
	if (delta)
	{
		shm_undict_validate_header(delta, true);
		if (delta->is_index)
			shm_undict_index_destroy(thread, (hash_table_index_header *)delta, true);
		else
			shm_undict_table_destroy(thread, delta);
	}
	if (dict->delta_buckets != EMPTY_SHM)
		shm_pointer_empty_atomic(thread, &dict->delta_buckets);
}

void
shm_undict_table_destroy(ThreadContext *thread, hash_table_header *header)
{
	if (header->relocated) return;
	header->relocated = true;
	shmassert(header->log_count > 0 && header->log_count < 16);
	int cnt = 1 << header->log_count;
	if (header->type == SHM_TYPE_UNDICT_DELTA_TABLE)
	{
		hash_delta_bucket *buckets = (hash_delta_bucket *)&header->buckets;
		for (int idx = 0; idx < cnt; idx++)
		{
			shm_pointer_empty_atomic(thread, &buckets[idx].key);
			shm_pointer_empty_atomic(thread, &buckets[idx].value);
		}
	}
	else
	{
		hash_bucket *buckets = &header->buckets;
		for (int idx = 0; idx < cnt; idx++)
		{
			shm_pointer_empty_atomic(thread, &buckets[idx].key);
			shm_pointer_empty_atomic(thread, &buckets[idx].value);
		}
	}
}

void
shm_undict_index_destroy(ThreadContext *thread, hash_table_index_header *index, bool deep)
{
	if (index->relocated) return;
	index->relocated = true;
	ShmPointer *index_blocks = &index->blocks;
	int index_count = 1 << index->index_log_size;
	for (int idx = 0; idx < index_count; ++idx)
	{
		shmassert(index_blocks[idx] != guard_bytes);
		if (deep)
		{
			hash_table_header *subblock = LOCAL(index_blocks[idx]);
			shmassert(subblock->type == SHM_TYPE_UNDICT_DELTA_TABLE || subblock->type == SHM_TYPE_UNDICT_TABLE);
			shm_undict_table_destroy(thread, subblock);
		}
		if (SBOOL(p_atomic_shm_pointer_get(&index_blocks[idx])))
			shm_pointer_empty_atomic(thread, &index_blocks[idx]);
	}
}

// end ShmUnDict

void
shm_refcounted_block_before_release(ThreadContext *thread, ShmPointer shm_pointer, ShmRefcountedBlock *block)
{
	int obj_type = SHM_TYPE(block->type);
	if (obj_type == SHM_TYPE(SHM_TYPE_LIST_BLOCK))
	{
		int id = mm_block_get_debug_id(block, shm_pointer);
		shmassert(SHM_LIST_BLOCK_FIRST_DEBUG_ID == id || SHM_LIST_BLOCK_DEBUG_ID == id);
	}
	if (obj_type == SHM_TYPE(SHM_TYPE_LIST_INDEX))
	{
		int id = mm_block_get_debug_id(block, shm_pointer);
		shmassert(SHM_LIST_INDEX_DEBUG_ID == id);
	}
}

void
shm_refcounted_block_destroy(ThreadContext *thread, ShmPointer shm_pointer, ShmRefcountedBlock *obj)
{
	// release nested structures for containers
	int obj_type = SHM_TYPE(obj->type);
	switch (obj_type)
	{
	case SHM_TYPE(SHM_TYPE_CELL):
		// shm_cell_destroy(thread, (ShmCell *)obj, shm_pointer);
		break;
	case SHM_TYPE(SHM_TYPE_TUPLE):
		{
			ShmValueHeader *header = LOCAL(shm_pointer);
			int size = shm_value_get_length(header);
			shmassert(size % sizeof(ShmPointer) == 0);
			int count = size / isizeof(ShmPointer);
			__ShmPointer *elements = shm_value_get_data(header);
			if (elements)
				for (int i = 0; i < count; i++)
				{
					if (SBOOL(elements[i]))
						shm_pointer_empty(thread, &elements[i]);
				}
		}
		break;
	case SHM_TYPE(SHM_TYPE_LIST):
		// shm_list_destroy(thread, (ShmList *)obj, shm_pointer);
		break;
	case SHM_TYPE(SHM_TYPE_LIST_BLOCK):
		// not implemented
		{
			int id = mm_block_get_debug_id(obj, shm_pointer);
			shmassert(SHM_LIST_BLOCK_FIRST_DEBUG_ID == id || SHM_LIST_BLOCK_DEBUG_ID == id);
		}
		break;
	case SHM_TYPE(SHM_TYPE_LIST_CELL):
		shmassert(false); // not refcounted
		break;
	case SHM_TYPE(SHM_TYPE_LIST_INDEX):
		{
			int id = mm_block_get_debug_id(obj, shm_pointer);
			shmassert(SHM_LIST_INDEX_DEBUG_ID == id);
		}
		break;
	case SHM_TYPE(SHM_TYPE_LIST_CHANGES):
		break;
	case SHM_TYPE(SHM_TYPE_QUEUE):
		// not implemented
		break;
	case SHM_TYPE(SHM_TYPE_QUEUE_CELL):
		// ??
		break;
	case SHM_TYPE(SHM_TYPE_QUEUE_CHANGES):
		// ??
		break;
	case SHM_TYPE(SHM_TYPE_DICT):
		shm_dict_destroy(thread, (ShmDict *)obj, shm_pointer);
		break;
	case SHM_TYPE(SHM_TYPE_DICT_DELTA):
		// ???
		break;
	case SHM_TYPE(SHM_TYPE_OBJECT):
	case SHM_TYPE(SHM_TYPE_UNDICT):
		shm_undict_destroy(thread, (ShmUnDict *)obj, shm_pointer);
		break;
	case SHM_TYPE(SHM_TYPE_UNDICT_TABLE):
	case SHM_TYPE(SHM_TYPE_UNDICT_DELTA_TABLE):
		/*
		#4  0x00442b8b in _shmassert (condition=false, condition_msg=0x461843 "ref_block", message=0x0,
			file=0x461047 "shm_types.c", line=5844) at shm_types.c:49
		#5  0x004511e9 in shm_pointer_to_refcounted (thread=0xb5aa0014, pointer=1) at shm_types.c:5844
		#6  0x00451325 in shm_pointer_release (thread=0xb5aa0014, pointer=1) at shm_types.c:5864
		#7  0x00451666 in shm_pointer_empty_atomic (thread=0xb5aa0014, pointer=0xb5ba0148)
			at shm_types.c:6023
		#8  0x004505d2 in shm_undict_table_destroy (thread=0xb5aa0014, header=0xb5ba0110)
			at shm_types.c:5624
		#9  0x004514a4 in shm_refcounted_block_destroy (thread=0xb5aa0014, shm_pointer=1048848,
			obj=0xb5ba0110) at shm_types.c:5934
		#10 0x00456393 in free_mem (thread=0xb5aa0014, shm_pointer=1048848, size=88) at MM.c:1251
		#11 0x00451388 in shm_pointer_release (thread=0xb5aa0014, pointer=1048848) at shm_types.c:5875
		#12 0x0045161e in shm_pointer_empty (thread=0xb5aa0014, pointer=0xb5ba00c4) at shm_types.c:6014
		#13 0x004500ca in release_delta_table (thread=0xb5aa0014, dict=0xb5ba0080, invalidated=true)
			at shm_types.c:5525
		#14 0x00450e4b in shm_undict_commit (thread=0xb5aa0014, dict=0xb5ba0080) at shm_types.c:5782
		#15 0x0044680a in transaction_end (thread=0xb5aa0014, rollback=false) at shm_types.c:1926
		#16 0x00447750 in commit_transaction (thread=0xb5aa0014, recursion_count=0x0)
		*/
		shm_undict_table_destroy(thread, (hash_table_header *)obj);
		break;
	case SHM_TYPE(SHM_TYPE_UNDICT_INDEX):
	case SHM_TYPE(SHM_TYPE_UNDICT_DELTA_INDEX):
		break;
		shm_undict_index_destroy(thread, (hash_table_index_header *)obj, true);
		break;
	}
}

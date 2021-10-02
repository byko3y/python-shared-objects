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

//  Simple linear probing hash array

#include <stdbool.h>
#include <stdint.h>
#include "shm_utils.h"
#include "shm_types.h"
#include "unordered_map.h"

int
calc_pos(int base_pos, int step, int bucket_count)
{
	int idx = base_pos + step;
	if (idx >= bucket_count)
	{
		idx = idx - bucket_count;
		if (idx >= base_pos)
			return -1; // full circle
	}
	return idx;
}

int
count_bits(int value)
{
	int rslt = 0;
	while (value)
	{
		if (value % 2 != 0)
			rslt++;
		value = value >> 1;
	}
	return rslt;
}

ShmInt
bucket_get_state(hash_bucket *bucket)
{
	if (bucket->key != 0)
	{
		if (bucket->value == EMPTY_SHM)
			return HASH_BUCKET_STATE_DELETED_RESERVED;
		else
			return HASH_BUCKET_STATE_SET;
	}
	else
	{
		if (bucket->key_hash == 0)
			return HASH_BUCKET_STATE_EMPTY;
		else // if (bucket->key_hash == 1)
			return HASH_BUCKET_STATE_DELETED;
			
	}
}

void
validate_bucket(hash_bucket *bucket)
{
	switch (bucket_get_state(bucket))
	{
	case HASH_BUCKET_STATE_EMPTY:
		shmassert(SBOOL(bucket->key) == false);
		shmassert(bucket->key_hash == 0);
		shmassert(SBOOL(bucket->value) == false);
		break;
	case HASH_BUCKET_STATE_DELETED:
		shmassert(SBOOL(bucket->key) == false);
		shmassert(bucket->key_hash == 1);
		shmassert(SBOOL(bucket->value) == false);
		break;
	case HASH_BUCKET_STATE_DELETED_RESERVED:
		shmassert(SBOOL(bucket->key));
		shmassert(bucket->value == EMPTY_SHM);
		break;
	default:
		shmassert(SBOOL(bucket->key));
		shmassert(bucket->value != EMPTY_SHM);
	}
}

hash_bucket *
get_bucket_at_index(hash_table_header *header, int itemindex, int item_size)
{
	if (header->is_index)
	{
		hash_table_index_header *index_header = (hash_table_index_header *)header;
		ShmInt index_log_size = index_header->index_log_size;
		shmassert(index_log_size != 0);
		// get highest bits for index_index, and lowest for local_index
		int log_local_count = index_header->log_count - index_log_size;
		shmassert(log_local_count > 0 && log_local_count < isizeof(size_t) * 8 - 1);
		int item_mask = (1 << log_local_count) - 1;
		shmassert(item_mask > 0);
		int index_index = itemindex >> log_local_count;
		int local_index = itemindex & item_mask;

		ShmPointer *index_blocks = &index_header->blocks;
		hash_table_header *subblock = LOCAL(index_blocks[index_index]);
		shmassert(subblock->log_count == log_local_count);
		shmassert(local_index < (1 << log_local_count));
		shmassert(local_index >= 0);
		uintptr_t buckets = (uintptr_t)&subblock->buckets;
		shmassert(buckets);

		uintptr_t rslt = buckets + (puint)(local_index * item_size);
		shmassert(rslt < (uintptr_t)subblock + (puint)subblock->size);
		return (hash_bucket *)rslt;
	}
	else
	{
		uintptr_t buckets = (uintptr_t)header + sizeof(hash_table_header) - sizeof(hash_bucket);
		shmassert(buckets);
		uintptr_t rslt = buckets + (puint)(itemindex * item_size);
		shmassert(rslt < (uintptr_t)header + (puint)header->size);
		return (hash_bucket *)rslt;
	}
}

int calc_base_index(uint32_t hash, int bucket_count)
{
	shmassert(bucket_count > 0);
	unsigned int rslt = hash & ((unsigned int)bucket_count - 1);
	return (int)rslt;
}

int
hash_max_collision_size(ShmInt bucket_count)
{
	int rslt = bucket_count / 8;
	if (rslt < 4)
		rslt = 4 - 1;

	return rslt;
}
 
find_position_result
hash_find_position(hash_func_args args, ShmUnDictKey *key)
{
	if (args.header == NULL)
	{
		find_position_result rslt = { -1, -1 };
		return rslt;
	}
	shmassert(count_bits(args.bucket_count) == 1);
	int base_index = calc_base_index(key->hash, args.bucket_count);
	int attempt_number = 0;
	int first_deleted_pos = -1;
	int max_attempts = hash_max_collision_size(args.bucket_count);

	while (true)
	{
		if (attempt_number > max_attempts)
		{
			find_position_result rslt = { -2, -2 };
			return rslt;
		}
		int test_pos = calc_pos(base_index, attempt_number, args.bucket_count);
		if (test_pos == -1)
		{
			// full circle
			shmassert(false);
			find_position_result rslt = { -1, -1 };
			return rslt;
		}
		hash_bucket *bucket = get_bucket_at_index(args.header, test_pos, args.bucket_item_size);
		validate_bucket(bucket);
		switch (bucket_get_state(bucket))
		{
		case HASH_BUCKET_STATE_SET:
		case HASH_BUCKET_STATE_DELETED_RESERVED:
			if (!args.compare_key || args.compare_key(bucket, key))
			{
				find_position_result rslt = { .found = test_pos, .last_free = -1 };
				return rslt;
			}
			break;
		case HASH_BUCKET_STATE_DELETED:
			if (first_deleted_pos != -1)
				first_deleted_pos = test_pos;
			break;
		case HASH_BUCKET_STATE_EMPTY:
			{
				find_position_result rslt;
				rslt.found = -1;
				if (first_deleted_pos != -1)
					rslt.last_free = first_deleted_pos;
				else
					rslt.last_free = test_pos;

				return rslt;
			}
			break;
		default:
			shmassert(false);
		}

		attempt_number++;
	}
}

bool same_bucket(uint32_t hash1, uint32_t hash2, uint32_t mask)
{
	return (hash1 & mask) == (hash2 & mask);
}

void
swap_buckets(hash_bucket *bucket1, hash_bucket *bucket2)
{
	hash_bucket tmp;
	tmp = *bucket1;
	*bucket1 = *bucket2;
	*bucket2 = tmp;
}

void
bucket_empty_deleted(hash_bucket *bucket)
{
	shmassert(bucket->value == EMPTY_SHM);
	shmassert(bucket_get_state(bucket) == HASH_BUCKET_STATE_DELETED);
	bucket->key = 0; // memset-friendly
}

// returns deleted key
ShmPointer
bucket_delete(hash_bucket *bucket)
{
	shmassert(bucket->value == EMPTY_SHM);
	shmassert(SBOOL(bucket->key));

	ShmPointer key_shm = bucket->key;
	bucket->key = 0; // memset-friendly
	bucket->key_hash = 1;
	return key_shm;
}

void
hash_compact_tail(hash_func_args args, hash_index deleted_index, uint32_t base_hash)
{
	hash_bucket *prev_bucket = NULL;
	shmassert(deleted_index < args.bucket_count);
	uint32_t mask = (uint32_t)args.bucket_count - 1;
	int base_item = calc_base_index(base_hash, args.bucket_count);
	int distance = deleted_index - base_item;
	if (distance < 0)
		distance += args.bucket_count;
	// hash table should almost never have such high collision rate
	shmassert(distance < hash_max_collision_size(args.bucket_count) + 2);
	int step_num = distance + 1; // next item after deleted
	shmassert(step_num < args.bucket_count);

	int deleted_pos = calc_pos(base_item, distance, args.bucket_count);
	shmassert(deleted_pos == deleted_index);
	prev_bucket = get_bucket_at_index(args.header, deleted_index, args.bucket_item_size);
	shmassert(prev_bucket);

	while (true)
	{
		// "8" or "bucket_count div 4" on a single item's chain is a trigger for resizing,
		// but combined chains might incidentally become larger than that, so we cannot rely on any chanin length here.
		int test_pos = calc_pos(base_item, step_num, args.bucket_count);
		if (test_pos == -1)
		{
			// full circle
			shmassert(false);
			return;
		}
		// p/x *(hash_bucket[4]*)&args.header->buckets
		hash_bucket *bucket = get_bucket_at_index(args.header, test_pos, args.bucket_item_size);
		validate_bucket(bucket);
		switch (bucket_get_state(bucket))
		{
		case HASH_BUCKET_STATE_DELETED:
			// no need to swap two deleted items
			break;
		case HASH_BUCKET_STATE_EMPTY:
			if (prev_bucket != NULL)
			{
				// We need to delete all the deleted items from the tail.
				// Currently deleting only one.
				if (bucket_get_state(prev_bucket) == HASH_BUCKET_STATE_DELETED)
				{
					bucket_empty_deleted(prev_bucket);
					if (args.deleted_count)
						args.deleted_count--;
				}
			}
			return;
			break;
		case HASH_BUCKET_STATE_DELETED_RESERVED:
		case HASH_BUCKET_STATE_SET:
			if (same_bucket(base_hash, bucket->key_hash, mask))
			{
				// move the item closer to the base. Cannot touch other bucket's chain coz we might move the bucket before its base position thus corrupting the table.
				hash_bucket *deleted_bucket = get_bucket_at_index(args.header, deleted_pos, args.bucket_item_size);
				shmassert(deleted_bucket != bucket);
				swap_buckets(bucket, deleted_bucket);
				deleted_pos = test_pos;
			}
			break;
		default:
			shmassert(false);
		}

		step_num++;
		prev_bucket = bucket;
	}
}

// typedef struct {
//   ThreadContext *thread;
//   hash_bucket *buckets;
//   int *item_count;
//   int *deleted_count;
//   int bucket_count;
//   ShmDictKey *key;
//   bool owned;
//   hash_key_compare compare_key;
// } hash_func_args;

/*int
hash_set(hash_func_args args, ShmPointer value)
{
	find_position_result rslt = hash_find_position(args);
	hash_bucket *bucket = NULL;
	if (rslt.pos != -1)
		bucket = &args.buckets[rslt.pos];
	if (rslt.is_valid)
	{
		shmassert(bucket_get_state(&args.buckets[rslt.pos], args.owned) == HASH_BUCKET_STATE_SET, NULL);
		shm_pointer_move(args.thread, &args.buckets[rslt.pos].new_data, &value);
		args.buckets[rslt.pos].has_new_data = true;
		return true;
	}
	else if (rslt.pos != -1)
	{
		bucket_get_state(&args.buckets[rslt.pos], args.owned);
		// args.num
	}
	else
	{
		return RESULT_REPEAT;
	}
	return false;
}*/

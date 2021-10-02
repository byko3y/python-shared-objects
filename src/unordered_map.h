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

#include "shm_types.h"

enum hash_bucket_state { HASH_BUCKET_STATE_INVALID, HASH_BUCKET_STATE_EMPTY, HASH_BUCKET_STATE_SET,
	HASH_BUCKET_STATE_DELETED, HASH_BUCKET_STATE_DELETED_RESERVED
};
typedef intptr_t hash_index;

typedef vl struct {
	uint32_t key_hash;
	ShmPointer key; // EMPTY_SHM for empty and deleted (determinted by key_hash), otherwise set
	ShmPointer value;
} hash_bucket;

typedef volatile struct {
	uint32_t key_hash;
	ShmPointer key;
	ShmPointer value;
	hash_index orig_item;
} hash_delta_bucket;

typedef vl struct {
	// type is SHM_TYPE_UNDICT_TABLE or SHM_TYPE_UNDICT_DELTA_TABLE
	SHM_REFCOUNTED_BLOCK
	// hash_table_header
	ShmInt log_count;
	ShmInt is_index;
	ShmInt relocated; // this block's state is invalid
	// hash_bucket[] or hash_delta_bucket[]
	hash_bucket buckets;
} hash_table_header;

typedef vl struct {
	// type is SHM_TYPE_UNDICT_INDEX or SHM_TYPE_UNDICT_DELTA_INDEX
	SHM_REFCOUNTED_BLOCK
	// hash_table_header
	ShmInt log_count;
	ShmInt is_index; // = true
	ShmInt relocated; // this block's state is invalid
	// hash_table_index_header
	ShmInt index_log_size;
	ShmPointer blocks;
} hash_table_index_header;


typedef struct {
	hash_index found;
	hash_index last_free;
} find_position_result;

// shm_dict_init_key(ShmDictKey *key, const char *s)
typedef bool(*hash_key_compare)(hash_bucket *bucket, ShmUnDictKey *key);

typedef struct {
	ThreadContext *thread;
	hash_table_header *header;
	int bucket_item_size;
	volatile ShmInt *item_count;
	volatile ShmInt *deleted_count;
	ShmInt bucket_count;
	hash_key_compare compare_key;
} hash_func_args;

ShmInt
bucket_get_state(hash_bucket *bucket);

hash_bucket *
get_bucket_at_index(hash_table_header *header, int itemindex, int item_size);

void
validate_bucket(hash_bucket *bucket);

find_position_result
hash_find_position(hash_func_args args, ShmUnDictKey *key);

void
bucket_empty_deleted(hash_bucket *bucket);

// returns deleted key
ShmPointer
bucket_delete(hash_bucket *bucket);

void
hash_compact_tail(hash_func_args args, hash_index deleted_index, uint32_t base_hash);

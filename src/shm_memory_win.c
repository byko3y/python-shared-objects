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

#include <stdbool.h>
#include "shm_defs.h"
#include "shm_memory.h"
#include <windows.h>

// Returns a pointer to the block in the current process memory and a valid FileMapping on success. Returns NULL on failure with undefined value of FileMapping field.
// block is a description of the block in shared memory, uninitialized when isnew == true
// block_type is 0 for regular blocks, 1 for superblock of secondary process, 2 for coordinator's superblock
//
// Not thread safe.
ChunkAllocationResult
shm_chunk_allocate(__ShmChunk *block, bool isnew, ShmInt memsize)
{
	ChunkAllocationResult rslt = { NULL, INVALID_HANDLE_VALUE };
	int try_count = 2;
	while (try_count > 0)
	{
		try_count = try_count - 1;

		if (isnew)
		{
			memset(CAST_VL(&block->id[0]), 0, SHM_SAFE_NAME_LENGTH + 1);
			_make_filename(CAST_VL(&block->id[0]));
			// block->used = 0;
			block->size = memsize;
		}

		SetLastError(0);
		HANDLE hFileMapping = CreateFileMappingA(
			(HANDLE)0xFFFFFFFF,
			NULL,
			PAGE_READWRITE,
			//(memsize >> 32) & 0xFFFFFFFF,
			0,
			memsize & 0xFFFFFFFF,
			CAST_VL(&block->id[0]));
		if (hFileMapping == NULL)
		{
			fprintf(stderr, "CreateFileMapping: Error %ld\n",
				GetLastError());
			return rslt;
		};
		if (isnew)
		{
			if (GetLastError() == ERROR_ALREADY_EXISTS)
			{
				CloseHandle(hFileMapping);
				fprintf(stderr, "failed %s\n", &block->id[0]);
				continue;
			};
		}
		else
		{
			if (GetLastError() != ERROR_ALREADY_EXISTS)
			{
				CloseHandle(hFileMapping);
				return rslt; // could not open existing file
			};
			// printf("loaded %s\n", &block->id[0]);
		}
		// MapViewOfFile keeps internal reference to hFileMapping
		LPVOID lpFileMap = MapViewOfFile(hFileMapping,
			FILE_MAP_READ | FILE_MAP_WRITE, 0, 0, 0);
		if (lpFileMap == 0)
		{
			CloseHandle(hFileMapping);
			fprintf(stderr, "MapViewOfFile: Error %ld\n",
				GetLastError());
			return rslt;
		}
		if (isnew)
			memset(lpFileMap, 0x93, memsize);

		rslt.data = lpFileMap;
		rslt.FileMapping = hFileMapping;
		return rslt;
	}

	return rslt;
}

// Thread unsafe
void
shm_chunk_register_handle(ShmChunk *chunk, ShmFileHandle FileMapping, ShmProcessID coordinator_process, bool i_am_the_coordinator)
{
	// coordinator process owns every shared kernel object
	bool is_coordinator = i_am_the_coordinator || ShmGetCurrentProcessId() == coordinator_process;
	if (is_coordinator)
		chunk->handle = FileMapping; // even when we are a different thread, we can still simply pass the handle across threads.
	else
	{
		chunk->handle = handle_to_coordinator(coordinator_process, FileMapping, true); // closing the FileMapping handle
	}

	// return is_coordinator;
}

void
shm_chunk_release(ShmChunk *chunk, void *view, bool is_coordinator)
{
	CloseHandle(chunk->handle);
	UnmapViewOfFile(view);
}

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

#include <sys/mman.h>
#include <sys/stat.h> // For mode constants 
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

// Not thread safe.
ChunkAllocationResult
shm_chunk_allocate(__ShmChunk *block, bool isnew, ShmInt memsize)
{
	ChunkAllocationResult rslt = { NULL, -1 };
	int try_count = 2;
	while (try_count > 0)
	{
		try_count = try_count - 1;

		if (isnew)
		{
			memset(&block->id[0], 0, SHM_SAFE_NAME_LENGTH + 1);
			_make_filename(&block->id[0]);
			// block->used = 0;
			block->size = memsize;
		}
		else
			shmassert(block->size == memsize);

		int fd = shm_open(
			&block->id[0],
			O_RDWR | (isnew ? O_CREAT | O_EXCL : 0),
			S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP
		);
		if (fd == -1)
		{
			if (isnew)
			{
				if (errno == EEXIST)
					continue;
				else
					shmassert_msg(false, "Failed to allocate shared memory");
			}
			else
			{
				shmassert_msg(false, "Failed to open shared memory");
			}
			return rslt;
		}

		if (isnew)
		{
			int trunc_rslt = ftruncate(fd, memsize);
			if (trunc_rslt == -1)
			{
				shm_unlink(&block->id[0]);
				shmassert_msg(false, "Failed to set shared memory size");
				return rslt;
			}
		}
		shmassert(memsize > 0);
		void *map_rslt = mmap(NULL, (size_t)memsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		if (map_rslt == MAP_FAILED)
		{
			if (isnew)
				shm_unlink(&block->id[0]);
			else
				close(fd);

			shmassert_msg(false, "Failed to mmap shared memory");
			return rslt;
		}
		rslt.data = map_rslt;
		rslt.FileMapping = fd;
		return rslt;
	}
	return rslt;
}

// Thread unsafe
void
shm_chunk_register_handle(ShmChunk *chunk, ShmFileHandle FileMapping, ShmProcessID coordinator_process, bool i_am_the_coordinator)
{
	// kernel owns the shared memory. Coordinator unlinks the chunk by chunk->id.
	close(FileMapping);
}

void
shm_chunk_release(ShmChunk *chunk, vl void *view, bool is_coordinator)
{
	if (is_coordinator)
	{
		int rslt = shm_unlink(CAST_VL(&chunk->id[0]));
		shmassert(rslt == 0);
	}

	int rslt2 = munmap(CAST_VL(view), chunk->size);
	shmassert(rslt2 == 0);
}

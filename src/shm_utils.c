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

#include <pmacrosos.h>
#include "shm_defs.h"
#include "shm_utils.h"
#include "assert.h"

#ifdef P_OS_LINUX
	#include <sys/syscall.h>
	#include <unistd.h>
	#include "pmacrosos.h"

	pid_t gettid_impl(void)
	{
		return syscall(SYS_gettid);
	}
#endif

// Debugging
ShmProcessHandle child_processes[64] = {0,};
ShmProcessHandle parent_process = 0;
char *assert_prefix;

void _shmassert(bool condition, char *condition_msg, char *message, char *file, int line)
{
	if (P_UNLIKELY(!(condition))) {
		\
			if (assert_prefix) \
				fputs(assert_prefix, stderr); \
			fprintf(stderr, "assertion \""); \
			fprintf(stderr, "%s", condition_msg); \
			fprintf(stderr, "\" at "); \
			fputs(file, stderr); \
			fprintf(stderr, "->"); \
			fprintf(stderr, "%d", line); \
			if ((message)) \
			{ \
				fprintf(stderr, ":"); \
				char *msg = message; \
				fputs(msg, stderr); \
			} \
			fprintf(stderr, "\n"); \
			if (DebugPauseAll()) \
				assert(false); \
	} \
}

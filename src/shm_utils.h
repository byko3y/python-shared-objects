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

/* Debugging routines */

#include "shm_defs.h"
#include <pmacrosos.h>
#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
	#include <windows.h>
#endif

// shows 666 both in decimal and hexadecimal formats
#define EVIL_MARK 1717966666
#define DEBUG_STRICT true

extern char *assert_prefix;
extern ShmProcessHandle child_processes[MAX_THREAD_COUNT];
extern ShmProcessHandle parent_process;

#if (defined(P_OS_LINUX))
	#include <sys/types.h>
	extern pid_t gettid_impl(void);

	#include "puthread.h"
#endif

static inline ShmThreadID
ShmGetCurrentThreadId(void) {
	#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
		return GetCurrentThreadId();
	#elif defined(P_OS_LINUX)
		// return gettid_impl();
		// use cached value from TLS instead of expensive syscall
		return (ShmThreadID)(intptr_t)(p_uthread_current_id());
	#else
		return -1;
	#endif
}

static inline ShmProcessID
ShmGetCurrentProcessId(void) {
	#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
		return GetCurrentProcessId();
	#elif defined(P_OS_LINUX)
		return gettid_impl();
	#else
		return -1;
	#endif
}

#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
#else
	#include <signal.h>
	#include <syslog.h>
	static inline void
	DebugBreak(void)
	{
		raise(SIGSTOP);
	}
	static inline void
	OutputDebugStringA(const char *msg)
	{
		syslog(LOG_DEBUG, "%s", msg);
	}
#endif

static inline bool
DebugPause(void)
{
#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
	int rslt = 0;
	DWORD error = 0;
	for (int i = 0; i < MAX_THREAD_COUNT; i++)
	{
		if (child_processes[i] != 0)
	{
			rslt = SuspendThread(child_processes[i]);
			if (rslt != 0)
				error = GetLastError();
		}
	}

	if (parent_process != 0)
	{
		rslt = SuspendThread(parent_process);
		if (rslt != 0)
			error = GetLastError();
	}
#else
	for (int i = 0; i < MAX_THREAD_COUNT; i++)
	{
		if (child_processes[i] != 0)
		{
			kill(child_processes[i], SIGSTOP);
		}
	}
	if (parent_process != 0)
		kill(parent_process, SIGSTOP);
#endif

	DebugBreak();

#if (defined(P_OS_WIN) || defined(P_OS_WIN64))
	for (int i = 0; i < MAX_THREAD_COUNT; i++)
	{
		if (child_processes[i] != 0)
		{
			rslt = ResumeThread(child_processes[i]);
			if (rslt != 0)
				error = GetLastError();
		}
	}
	if (parent_process != 0)
		ResumeThread(parent_process);
#else
	for (int i = 0; i < MAX_THREAD_COUNT; i++)
	{
		if (child_processes[i] != 0)
		{
		kill(child_processes[i], SIGCONT);
		}
	}
	if (parent_process != 0)
		kill(parent_process, SIGCONT);
#endif
	bool retval = false;
	return retval;
}

extern void _shmassert(bool condition, char *condition_msg, char *message, char *file, int line);
#define shmassert(condition) _shmassert((condition), (#condition), NULL, __FILE__, __LINE__)
#define shmassert_msg(condition, message) _shmassert((condition), (#condition), (message), __FILE__, __LINE__)

#define DEBUG_LOCK 0
#define DEBUG_SHM_EVENTS true
#define alloc_flinch true
extern bool random_flinch;
extern bool transient_pause;
extern bool reclaimer_debug_info;

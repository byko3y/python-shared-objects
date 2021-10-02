/*
 * The MIT License
 *
 * Copyright (C) 2010-2019 Alexander Saprykin <saprykin.spb@gmail.com>
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

#include "pmem.h"
// #include "pmutex.h"
#include "patomic.h"
#include "puthread.h"
#include "puthread-private.h"

#include <stdlib.h>
#include <time.h>
#include <string.h>

#include <process.h>
#include <windows.h>

typedef HRESULT (WINAPI * PWin32SetThreadDescription) (HANDLE hThread, PCWSTR lpThreadDescription);
typedef HANDLE puthread_hdl;

struct PUThread_ {
	PUThreadBase		base;
	puthread_hdl		hdl;
	PUThreadFunc		proxy;
};

struct PUThreadKey_ {
	DWORD			key_idx;
	PDestroyFunc		free_func;
};

typedef struct PUThreadDestructor_ PUThreadDestructor;

struct PUThreadDestructor_ {
	DWORD			key_idx;
	PDestroyFunc		free_func;
	PUThreadDestructor	*next;
};

/*
 * For thread names:
 * https://docs.microsoft.com/en-us/visualstudio/debugger/how-to-set-a-thread-name-in-native-code
 */

const DWORD MS_VC_THREAD_NAME_EXCEPTION = 0x406D1388;

#pragma pack(push, 8)
typedef struct tagTHREADNAME_INFO
{
	DWORD  dwType;     /* Must be 0x1000.                        */
	LPCSTR szName;     /* Pointer to name (in user addr space).  */
	DWORD  dwThreadID; /* Thread ID (-1 = caller thread).        */
	DWORD  dwFlags;    /* Reserved for future use, must be zero. */
} THREADNAME_INFO;
#pragma pack(pop)

#ifndef P_CC_MSVC
static void *pp_uthread_name_veh_handle = NULL;

static LONG __stdcall
pp_uthread_set_thread_name_veh (PEXCEPTION_POINTERS except_info)
{
	if (except_info->ExceptionRecord != NULL &&
	    except_info->ExceptionRecord->ExceptionCode == MS_VC_THREAD_NAME_EXCEPTION)
		return EXCEPTION_CONTINUE_EXECUTION;

	return EXCEPTION_CONTINUE_SEARCH;
}
#endif

/* Rest of definitions */

static PWin32SetThreadDescription     pp_uthread_set_descr_func  = NULL;

static puint __stdcall pp_uthread_win32_proxy (ppointer data);

static puint __stdcall
pp_uthread_win32_proxy (ppointer data)
{
	PUThread *thread = data;

	thread->proxy (thread);

	_endthreadex (0);

	return 0;
}

void
p_uthread_win32_thread_detach (void)
{
	/* pboolean was_called;

	do {
		PUThreadDestructor *destr;

		was_called = FALSE;

		mdestr = (PUThreadDestructor *) p_atomic_pointer_get ((const PVOID volatile *) &pp_uthread_tls_destructors);

		while (destr != NULL) {
			ppointer value;

			value = TlsGetValue (destr->key_idx);

			if (value != NULL && destr->free_func != NULL) {
				TlsSetValue (destr->key_idx, NULL);
				destr->free_func (value);
				was_called = TRUE;
			}

			destr = destr->next;
		}
	} while (was_called); */
}

void
p_uthread_init_internal (void)
{
	HMODULE hmodule;

	// if (P_LIKELY (pp_uthread_tls_mutex == NULL))
	//	pp_uthread_tls_mutex = p_mutex_new ();

	hmodule = GetModuleHandleA ("kernel32.dll");

	if (P_UNLIKELY (hmodule == NULL)) {
		P_ERROR ("PUThread::p_uthread_init_internal: failed to load kernel32.dll module");
		return;
	}

	pp_uthread_set_descr_func = (PWin32SetThreadDescription) GetProcAddress (hmodule, "SetThreadDescription");

#ifndef P_CC_MSVC
	pp_uthread_name_veh_handle = AddVectoredExceptionHandler (1, &pp_uthread_set_thread_name_veh);
#endif
}

void
p_uthread_shutdown_internal (void)
{
	// PUThreadDestructor *destr;

	p_uthread_win32_thread_detach ();

	/* destr = pp_uthread_tls_destructors;

	while (destr != NULL) {
		PUThreadDestructor *next_destr = destr->next;

		TlsFree (destr->key_idx);
		p_free (destr);

		destr = next_destr;
	}

	pp_uthread_tls_destructors = NULL;

	if (P_LIKELY (pp_uthread_tls_mutex != NULL)) {
		p_mutex_free (pp_uthread_tls_mutex);
		pp_uthread_tls_mutex = NULL;
	} */

	pp_uthread_set_descr_func = NULL;

#ifndef P_CC_MSVC
	if (pp_uthread_name_veh_handle != NULL) {
		RemoveVectoredExceptionHandler (pp_uthread_name_veh_handle);
		pp_uthread_name_veh_handle = NULL;
	}
#endif
}

PUThread *
p_uthread_create_internal (PUThreadFunc		func,
				   pboolean		joinable,
				   PUThreadPriority	prio,
				   psize		stack_size)
{
	PUThread *ret;

	if (P_UNLIKELY ((ret = p_malloc0 (sizeof (PUThread))) == NULL)) {
		P_ERROR ("PUThread::p_uthread_create_internal: failed to allocate memory");
		return NULL;
	}

	ret->proxy = func;

	if (P_UNLIKELY ((ret->hdl = (HANDLE) _beginthreadex (NULL,
							     (puint) stack_size,
							     pp_uthread_win32_proxy,
							     ret,
							     CREATE_SUSPENDED,
							     NULL)) == NULL)) {
		P_ERROR ("PUThread::p_uthread_create_internal: _beginthreadex() failed");
		p_free (ret);
		return NULL;
	}

	ret->base.joinable = joinable;

	p_uthread_set_priority (ret, prio);

	if (P_UNLIKELY (ResumeThread (ret->hdl) == (DWORD) -1)) {
		P_ERROR ("PUThread::p_uthread_create_internal: ResumeThread() failed");
		CloseHandle (ret->hdl);
		p_free (ret);
	}

	return ret;
}

void
p_uthread_exit_internal (void)
{
	_endthreadex (0);
}

void
p_uthread_wait_internal (PUThread *thread)
{
	if (P_UNLIKELY ((WaitForSingleObject (thread->hdl, INFINITE)) != WAIT_OBJECT_0))
		P_ERROR ("PUThread::p_uthread_wait_internal: WaitForSingleObject() failed");
}

void
p_uthread_set_name_internal (PUThread *thread)
{
	wchar_t		*thr_wname = NULL;
	psize		namelen    = 0;
	HRESULT		hres;
	THREADNAME_INFO	thr_info;

	if (pp_uthread_set_descr_func != NULL) {
		namelen = strlen (thread->base.name);

		if (P_UNLIKELY ((thr_wname = p_malloc0 (sizeof (wchar_t) * (namelen + 1))) == NULL)) {
			P_ERROR ("PUThread::p_uthread_set_name_internal: failed to allocate memory");
			return;
		}

		// mbstowcs(thr_wname, thread->base.name, namelen + 1);
		mbstowcs_s (NULL, thr_wname, namelen + 1, thread->base.name, namelen);

		hres = pp_uthread_set_descr_func (thread->hdl, thr_wname);

		p_free (thr_wname);

		if (P_UNLIKELY (FAILED (hres))) {
			P_ERROR ("PUThread::p_uthread_set_name_internal: failed to set thread description");
			return;
		}
	}

	if (!IsDebuggerPresent ())
		return;

	thr_info.dwType     = 0x1000;
	thr_info.szName     = thread->base.name;
	thr_info.dwThreadID = -1;
	thr_info.dwFlags    = 0;

#ifdef P_CC_MSVC
#  pragma warning(push)
#  pragma warning(disable: 6320 6322)
	__try {
		RaiseException (MS_VC_THREAD_NAME_EXCEPTION,
				0,
				sizeof (thr_info) / sizeof (ULONG_PTR),
				(ULONG_PTR *) &thr_info);
	}
	__except (EXCEPTION_EXECUTE_HANDLER) {}
#  pragma warning(pop)
#else
	if (pp_uthread_name_veh_handle != NULL)
		RaiseException (MS_VC_THREAD_NAME_EXCEPTION,
				0,
				sizeof (thr_info) / sizeof (ULONG_PTR),
				(ULONG_PTR *) &thr_info);
#endif
}

void
p_uthread_free_internal (PUThread *thread)
{
	CloseHandle (thread->hdl);
	p_free (thread);
}

P_LIB_API void
p_uthread_yield (void)
{
	Sleep (0);
}

P_LIB_API pboolean
p_uthread_set_priority (PUThread		*thread,
			PUThreadPriority	prio)
{
	pint native_prio;

	if (P_UNLIKELY (thread == NULL))
		return FALSE;

	switch (prio) {
	case P_UTHREAD_PRIORITY_IDLE:
		native_prio = THREAD_PRIORITY_IDLE;
		break;
	case P_UTHREAD_PRIORITY_LOWEST:
		native_prio = THREAD_PRIORITY_LOWEST;
		break;
	case P_UTHREAD_PRIORITY_LOW:
		native_prio = THREAD_PRIORITY_BELOW_NORMAL;
		break;
	case P_UTHREAD_PRIORITY_NORMAL:
		native_prio = THREAD_PRIORITY_NORMAL;
		break;
	case P_UTHREAD_PRIORITY_HIGH:
		native_prio = THREAD_PRIORITY_ABOVE_NORMAL;
		break;
	case P_UTHREAD_PRIORITY_HIGHEST:
		native_prio = THREAD_PRIORITY_HIGHEST;
		break;
	case P_UTHREAD_PRIORITY_TIMECRITICAL:
		native_prio = THREAD_PRIORITY_TIME_CRITICAL;
		break;
	case P_UTHREAD_PRIORITY_INHERIT:
	default:
		native_prio = GetThreadPriority (GetCurrentThread ());
		break;
	}

	if (P_UNLIKELY (SetThreadPriority (thread->hdl, native_prio) == 0)) {
		P_ERROR ("PUThread::p_uthread_set_priority: SetThreadPriority() failed");
		return FALSE;
	}

	thread->base.prio = prio;

	return TRUE;
}

P_LIB_API P_HANDLE
p_uthread_current_id (void)
{
	return (P_HANDLE) ((psize) GetCurrentThreadId ());
}

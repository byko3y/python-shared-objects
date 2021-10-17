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

#include <stdio.h>
#include <Python.h>

#include <stdlib.h>
#include "shm_types.h"
#include <string.h>
#include <ptypes.h>
#ifndef P_CC_MSVC
    #include "safe_lib.h"
#endif

typedef struct {
	PyObject_HEAD
	//ShmPointer data; // pointer to a process-specific storage of the pointers to value/cell (*data_pointer). Used for GC or recovery after crash.
	//void *local_ref; // local_vars->references[i]
	__ShmPointer data; // pointer to the actual data. Stored at local_pointer address.
	// bool persistent; // whether data's reference counter is increased to account for this object
} ShmBase;

typedef ShmBase ShmTupleObject;
typedef ShmBase ShmListObject;
typedef ShmBase ShmValueObject;
typedef ShmBase ShmDictObject;
typedef ShmBase ShmObjectObject;
typedef ShmBase ShmPromiseObject;

typedef struct {
	PyObject_HEAD
	ShmPointer tuple_shm;
	ShmValueHeader *tuple;
	int itemindex;
} ShmTupleIterObject;

typedef struct {
	PyObject_HEAD
	ShmPointer list_shm;
	ShmList *list;
	bool is_transient;
	int itemindex;
} ShmListIterObject;

typedef struct {
	PyObject_HEAD
	ShmPointer dict_shm;
	ShmUnDict *dict;
	bool is_transient;
	int itemindex; // table is sparse so that's a next item to try
} ShmDictIterObject;

static PyTypeObject ShmObject_Type;
static PyTypeObject ShmValue_Type;
static PyTypeObject ShmTuple_Type;
static PyTypeObject ShmTupleIter_Type;
static PyTypeObject ShmList_Type;
static PyTypeObject ShmListIter_Type;
static PyTypeObject ShmDict_Type;
static PyTypeObject ShmDictIter_Type;
static PyTypeObject ShmPromise_Type;

static PyObject *Shm_Exception;
static PyObject *Shm_Abort;

static ShmPointer guard; // to avoid zero-pointer access
static ShmPointer exit_flag_shm; // for all the children to wait for
static bool *exit_flag; // for all the children to wait for
bool pso_debug_print = false;

#define debug_print(...)  if (pso_debug_print) printf(__VA_ARGS__)

static ThreadContext *thread;

int
tuple_to_shm_tuple(PyObject *obj, __ShmPointer *rslt);
int
list_to_shm_list(PyObject *obj, __ShmPointer *rslt);
int
dict_to_shm_dict(PyObject *obj, __ShmPointer *rslt);
bool
object_to_dict_key(PyObject *key, ShmUnDictKey *dictkey);

static PyObject*
init(PyObject *self, PyObject *args) {
	debug_print("%d. Init coordinator\n", ShmGetCurrentProcessId());
	init_superblock(NULL);
	pint *val1 = get_mem(NULL, &guard, sizeof(pint)*8, VAL_DEBUG_ID);
	exit_flag = get_mem(NULL, &exit_flag_shm, sizeof(pint), EXIT_FLAG_DEBUG_ID);
	SHM_UNUSED(val1);
	debug_print("%d. Coordinator inited\n", ShmGetCurrentProcessId());
	init_thread_context(&thread);
	debug_print("%d. Thread context inited\n", ShmGetCurrentProcessId());

	ShmUnDict *dict = new_shm_undict(thread, &superblock->root_container);
	dict->type = SHM_TYPE_OBJECT;

	start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);

	return PyUnicode_FromString(CAST_VL(&superblock_desc.id[0]));
}

static PyObject*
connect_to_coordinator(PyObject *self, PyObject *args) {
	const char* name;
	if (!PyArg_ParseTuple(args, "s", &name)) {
		return NULL;
	}
	debug_print("%d. Connecting to coordinator: %s!\n", ShmGetCurrentProcessId(), name);
	long rslt = init_superblock(name);
	init_thread_context(&thread);
	debug_print("%d. Thread context inited\n", ShmGetCurrentProcessId());

	start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);

	return PyLong_FromLong(rslt);
}

static bool
check_thread_inited()
{
	if (thread == NULL)
	{
		PyErr_SetString(Shm_Exception, "PSO haven't been initialized through a call to init or connect.");
		return false;
	}
	return true;
}

static PyObject*
set_random_flinch(PyObject *self, PyObject *obj)
{
	if (obj == Py_True)
	{
		random_flinch = true;
		debug_print("%d. Random flinch engaged\n", ShmGetCurrentProcessId());
		Py_RETURN_NONE;
	}
	else if (obj == Py_False || obj == Py_None)
	{
		random_flinch = false;
		debug_print("%d. Random flinch disabled\n", ShmGetCurrentProcessId());
		Py_RETURN_NONE;
	}
	else
	{
		PyErr_SetString(PyExc_TypeError, "set_random_flinch requires boolean argument");
		return NULL;
	}
}

static PyObject*
set_debug_reclaimer(PyObject *self, PyObject *obj)
{
	if (obj == Py_True)
	{
		reclaimer_debug_info = true;
		debug_print("%d. Reclaimer's debug print enabled\n", ShmGetCurrentProcessId());
		Py_RETURN_NONE;
	}
	else if (obj == Py_False || obj == Py_None)
	{
		reclaimer_debug_info = false;
		debug_print("%d. Reclaimer's debug print disabled\n", ShmGetCurrentProcessId());
		Py_RETURN_NONE;
	}
	else
	{
		PyErr_SetString(PyExc_TypeError, "set_debug_reclaimer requires boolean argument");
		return NULL;
	}
}


static PyObject*
set_debug_print(PyObject *self, PyObject *obj)
{
	if (obj == Py_True)
	{
		pso_debug_print = true;
		debug_print("%d. PSO debug print enabled\n", ShmGetCurrentProcessId());
		Py_RETURN_NONE;
	}
	else if (obj == Py_False || obj == Py_None)
	{
		pso_debug_print = false;
		debug_print("%d. PSO debug print disabled\n", ShmGetCurrentProcessId());
		Py_RETURN_NONE;
	}
	else
	{
		PyErr_SetString(PyExc_TypeError, "set_debug_print requires boolean argument");
		return NULL;
	}
}

int64_t total_wait_counter = 0;
int64_t total_wait_loops = 0;

static PyObject*
print_thread_counters(PyObject *self, PyObject *args)
{
	printf("Times:        waited %5d, %5d,     waited2 %5d, %5d,\n",
		thread->private_data->times_waiting, thread->private_data->tickets_waiting,
		thread->private_data->times_waiting2, thread->private_data->tickets_waiting2);
	printf("             repeated %5d, %5d,     aborted1 %5d, %5d,\n",
		thread->private_data->times_repeated, thread->private_data->tickets_repeated,
	thread->private_data->times_aborted1, thread->private_data->tickets_aborted1);
	printf("             aborted2 %5d, %5d,     aborted3 %5d, %5d,\n",
		thread->private_data->times_aborted2, thread->private_data->tickets_aborted2,
		thread->private_data->times_aborted3, thread->private_data->tickets_aborted3);
	printf("             aborted4 %5d, %5d,     aborted5 %5d, %5d,\n",
		thread->private_data->times_aborted4, thread->private_data->tickets_aborted4,
		thread->private_data->times_aborted5, thread->private_data->tickets_aborted5);
	printf("             aborted6 %5d, %5d,    aborted7 %5d, %5d,\n",
		thread->private_data->times_aborted6, thread->private_data->tickets_aborted6,
	thread->private_data->times_aborted7, thread->private_data->tickets_aborted7);
	printf("             aborted8 %5d, %5d,    aborted9 %5d, %5d.\n",
		thread->private_data->times_aborted8, thread->private_data->tickets_aborted8,
		thread->private_data->times_aborted9, thread->private_data->tickets_aborted9);
	printf("reads:      preempted %5d, %5d,\n",
		thread->private_data->times_read_preempted, thread->private_data->tickets_read_preempted);
	printf("            preempted2 %5d, %5d,   preempted3 %5d, %5d,\n",
		thread->private_data->times_read_preempted2, thread->private_data->tickets_read_preempted2,
		thread->private_data->times_read_preempted3, thread->private_data->tickets_read_preempted3);
	printf("              repeated %5d, %5d,       waited %5d, %5d,\n",
		thread->private_data->times_read_repeated, thread->private_data->tickets_read_repeated,
		thread->private_data->times_read_waited, thread->private_data->tickets_read_waited);
	printf("               aborted %5d, %5d.\n",
		thread->private_data->times_read_aborted, thread->private_data->tickets_read_aborted);
	printf("               retry loops count %lli, retry loops %lli\n",
		total_wait_loops, total_wait_counter);

	Py_RETURN_NONE;
}

static PyObject*
global_debug_stop_on_contention(PyObject *self, PyObject *args)
{
	debug_stop_on_contention = true;
	Py_RETURN_NONE;
}

static PyObject*
object_debug_stop_on_contention(PyObject *self, PyObject *arg)
{
	PyTypeObject *type = Py_TYPE(arg);
	if (type == &ShmList_Type || type == &ShmDict_Type ||
		type == &ShmObject_Type || PyType_IsSubtype(type, &ShmObject_Type)) // only ShmObject descendants are supported
	{
		ShmPointer container_shm = ((ShmBase*)arg)->data;
		ShmContainer *block = LOCAL(container_shm);
		shmassert((block->type & SHM_TYPE_CELL) == SHM_TYPE_CELL);
		block->lock.break_on_contention = true;
	}
	Py_RETURN_NONE;
}

static PyObject*
get_contention_count(PyObject *self, PyObject *arg)
{
	PyTypeObject *type = Py_TYPE(arg);
	if (type == &ShmList_Type || type == &ShmDict_Type ||
		type == &ShmObject_Type || PyType_IsSubtype(type, &ShmObject_Type)) // only ShmObject descendants are supported
	{
		ShmPointer container_shm = ((ShmBase*)arg)->data;
		ShmContainer *block = LOCAL(container_shm);
		shmassert((block->type & SHM_TYPE_CELL) == SHM_TYPE_CELL);
		return Py_BuildValue("(ll)", block->lock.read_contention_count, block->lock.write_contention_count);
	}
	Py_RETURN_NONE;
}

static PyObject*
transient_start(PyObject *self, PyObject *args)
{
	if (!check_thread_inited())
		return NULL;
	start_transaction(thread, TRANSACTION_IDLE, LOCKING_WRITE, true, NULL);
	Py_RETURN_NONE;
}

static PyObject*
transient_end(PyObject *self, PyObject *args)
{
	if (!check_thread_inited())
		return NULL;
	commit_transaction(thread, NULL);
	Py_RETURN_NONE;
}

static PyObject*
transient_active(PyObject *self, PyObject *args)
{
	if (!check_thread_inited())
		return NULL;
	if (thread->transaction_mode >= TRANSACTION_IDLE)
		Py_RETURN_TRUE;
	else
		Py_RETURN_FALSE;
}

static PyObject*
transaction_start(PyObject *self, PyObject *args)
{
	if (!check_thread_inited())
		return NULL;
	start_transaction(thread, TRANSACTION_PERSISTENT, LOCKING_WRITE, true, NULL);
	Py_RETURN_NONE;
}

static PyObject*
transaction_commit(PyObject *self, PyObject *args)
{
	if (!check_thread_inited())
		return NULL;
	commit_transaction(thread, NULL);
	Py_RETURN_NONE;
}

static PyObject*
transaction_rollback(PyObject *self, PyObject *args)
{
	if (!check_thread_inited())
		return NULL;
	abort_transaction(thread, NULL);
	Py_RETURN_NONE;
}

static PyObject*
transaction_rollback_retaining(PyObject *self, PyObject *args)
{
	if (!check_thread_inited())
		return NULL;
	abort_transaction_retaining(thread);
	Sleep(0);
	Py_RETURN_NONE;
}

static PyObject*
transaction_active(PyObject *self, PyObject *args)
{
	if (!check_thread_inited())
		return NULL;
	if (thread->transaction_mode >= TRANSACTION_PERSISTENT)
		Py_RETURN_TRUE;
	else
		Py_RETURN_FALSE;
}

#define RETRY_LOOP(action, retry_checks, on_abort, on_failure) do { \
	int __counter = 0; \
	do { \
		int _rslt = RESULT_INVALID; \
		if (thread->transaction_mode == TRANSACTION_NONE) \
		{ \
			PyErr_SetString(Shm_Exception, "Invalid operation outside transaction"); \
			on_abort \
		} \
		_rslt = action; \
		if (_rslt == RESULT_OK) \
		{ \
			if (__counter != 0) \
			{ \
				total_wait_counter += __counter; \
				total_wait_loops += 1; \
			} \
			break; \
		} \
		__counter++; \
		/* debug_print("Retry triggered!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"); */ \
		retry_checks; \
		if (_rslt == RESULT_REPEAT) \
		{ \
			continue; \
		} \
		else if (_rslt == RESULT_FAILURE) \
		{ \
			on_failure \
		} \
		else if (_rslt == RESULT_WAIT || _rslt == RESULT_WAIT_SIGNAL) \
		{ \
			if (__counter < 64) \
				SHM_SMT_PAUSE; \
			else \
				Sleep(0); \
		} \
		else \
		{ \
			/* debug_print("TRANSACTION ABORTED!!!!!!!!!!!!!!!!\n"); */ \
			shmassert(result_is_abort(_rslt)); \
			abort_transaction_retaining(thread); \
			if (thread->transaction_mode != TRANSACTION_PERSISTENT) \
			{ \
				if (__counter < 64) \
					SHM_SMT_PAUSE; \
				else \
					Sleep(0); \
				\
				continue; \
			} \
			else \
			{ \
				total_wait_counter += __counter; \
				total_wait_loops += 1; \
				PyErr_SetString(Shm_Abort, "Transaction aborted"); \
				on_abort \
			} \
		} \
	} \
	while (true); \
} \
while (0) \


int
object_to_shm_value(PyObject *obj, ShmPointer *data)
{
	PyTypeObject *type;
	type = Py_TYPE(obj);
	if (obj == Py_None) {
		*data = NONE_SHM;
		return 0;
	}
	// cpython\Modules\_pickle.c
	else if (obj == Py_False || obj == Py_True)
	{
		ShmValueHeader *val_data = new_shm_value(thread, 1, SHM_TYPE_BOOL, data);
		unsigned char *pval = shm_value_get_data(val_data);
		if (obj == Py_True)
			*pval = 1;
		else
			*pval = 0;

		return 0;
	}
	else if (type == &PyLong_Type)
	{
		long val;
		int overflow;
		val = PyLong_AsLongAndOverflow(obj, &overflow);
		ShmValueHeader *val_data = new_shm_value(thread, sizeof(long), SHM_TYPE_LONG, data);
		long *pval = shm_value_get_data(val_data);
		*pval = val;
		return 0;
	}
	else if (type == &PyFloat_Type)
	{
		double val;
		val = PyFloat_AsDouble(obj);
		if (PyErr_Occurred())
			return -1;
		ShmValueHeader *val_data = new_shm_value(thread, sizeof(double), SHM_TYPE_FLOAT, data);
		double *pval = shm_value_get_data(val_data);
		*pval = val;
		return 0;
	}
	else if (type == &PyBytes_Type)
	{
		Py_ssize_t size = PyBytes_Size(obj);
		const char* val = PyBytes_AsString(obj);
		new_shm_bytes(thread, val, size, data);
		return 0;
	}
	else if (type == &PyUnicode_Type)
	{
		Py_ssize_t size;
		// const char* val = PyUnicode_AsUTF8AndSize(obj, &size);
		size = PyUnicode_GetLength(obj);
		ShmValueHeader *val_data = new_shm_value(thread, size * isizeof(Py_UCS4), SHM_TYPE_UNICODE, data);
		if (!shm_value_get_data(val_data))
			shmassert_msg(false, "NULL value data");
		// strncpy_s(shm_value_get_data(val_data), size + 1, val, size);
		Py_UCS4 *rslt = PyUnicode_AsUCS4(obj, shm_value_get_data(val_data), size, false);
		shmassert(rslt != NULL);
		return 0;
	}
	return -1;
}

// rslt is uninitialized on failure
int
prepare_item_for_shm_container(PyObject *value, ShmPointer *rslt)
{
	PyTypeObject *type = Py_TYPE(value);
	__ShmPointer newval = EMPTY_SHM;
	if (type == &ShmValue_Type || type == &ShmTuple_Type || type == &ShmPromise_Type ||
		type == &ShmList_Type || type == &ShmDict_Type ||
		type == &ShmObject_Type || PyType_IsSubtype(type, &ShmObject_Type)) // only ShmObject descendants are supported
	{
		newval = ((ShmBase*)value)->data;
		if (SBOOL(newval))
			shm_pointer_acq(thread, newval);
		*rslt = newval;
	}
	else if (PyDict_Check(value))
	{
		if (dict_to_shm_dict(value, &newval) < 0)
			shmassert(newval == EMPTY_SHM);
	}
	else if (PyTuple_Check(value))
	{
		if (tuple_to_shm_tuple(value, &newval) < 0)
			shmassert(newval == EMPTY_SHM);
	}
	else if (PyList_Check(value))
	{
		if (list_to_shm_list(value, &newval) < 0)
			shmassert(newval == EMPTY_SHM);
	}
	else if (object_to_shm_value(value, &newval) != 0)
	{
		shmassert(EMPTY_SHM == newval);
		return -1;
	}
	*rslt = newval;
	return (EMPTY_SHM == newval) ? -1 : 0;
}

static int
ShmValue_init(ShmValueObject *self, PyObject *args, PyObject *kwds)
{
	if (!check_thread_inited())
		return -1;
	self->data = EMPTY_SHM;
	/*
	PyObject* objectsRepresentation = PyObject_Repr(args); // returns new instance
	{
		const char* s = PyUnicode_AsUTF8(objectsRepresentation); // returns const reference
		debug_print("Creating value (%s: %s )...\n", args->ob_type->tp_name, s);
	}
	Py_XDECREF(objectsRepresentation);
	*/
	const char* val;
	if (FALSE && PyArg_ParseTuple(args, "s", &val))
	{
		unsigned int cnt = strlen(val);
		debug_print("got string value, len %d: %s\n", cnt, val);
		// cnt = 0 allowed here - will create header-only value
		ShmValueHeader *val_data = new_shm_value(thread, (int)cnt+1, SHM_TYPE_UNICODE, &self->data); // "+1" for null-terminator
		if (!shm_value_get_data(val_data))
			fprintf(stderr, "NULL value data");
		strncpy_s(shm_value_get_data(val_data), cnt + 1, val, cnt);
		//strcpy_s((char*)shm_value_get_data(val_data), cnt, val); // not working, lol
		return 0;
	}
	// _pickle.c
	// static int
	// save(PicklerObject *self, PyObject *obj, int pers_save)
	PyObject *obj;
	if (PyArg_UnpackTuple(args, "", 1, 1, &obj))
	{
		return object_to_shm_value(obj, &self->data);
	}
	return -1;
}

static PyObject *false_str = NULL;
static PyObject *true_str = NULL;

static PyObject* try_object_to_shm_value(PyObject *self, PyObject *obj) {
	ShmPointer shm_value = EMPTY_SHM;
	if (object_to_shm_value(obj, &shm_value) == -1)
	{
		Py_INCREF(Py_None);
		return Py_None;
	}
	// PyObject_GC_New(ShmValueObject, &ShmValue_Type) - we don't support Py_TPFLAGS_HAVE_GC here
	ShmValueObject *rslt = PyObject_New(ShmValueObject, &ShmValue_Type);
	// probably could call type_call/type->tp_new/object_new
	if (rslt == NULL)
	{
		shm_pointer_release(thread, shm_value);
		return NULL;
	}
	rslt->data = shm_value;
	// PyObject_GC_Track(rslt);
	return (PyObject *)rslt;
}

static const char *
bool_repr(unsigned char *self)
{
	PyObject *s;

	if (self == NULL || *self == 0)
		s = true_str ? true_str :
			(true_str = PyUnicode_InternFromString("False"));
	else
		s = false_str ? false_str :
			(false_str = PyUnicode_InternFromString("True"));
	return PyUnicode_AsUTF8(s);
}

// unicodeobject.c
static int
unicode_modifiable(PyObject *unicode)
{
	assert(PyUnicode_Check(unicode));
	if (Py_REFCNT(unicode) != 1)
		return 0;
	// if (_PyUnicode_HASH(unicode) != -1)
	// 	return 0;
	if (PyUnicode_CHECK_INTERNED(unicode))
		return 0;
	if (!PyUnicode_CheckExact(unicode))
		return 0;
	return 1;
}

// Immutable values, just copy them into native CPython objects
PyObject *
abstract_block_to_object(ShmAbstractBlock *data, ShmPointer data_shm)
{
	ShmValueHeader *val_data = (ShmValueHeader *)data;
	switch (SHM_TYPE(data->type)) {
	case SHM_TYPE(SHM_TYPE_BOOL):
	{
		const bool *boolval = shm_value_get_data(val_data);
		return PyBool_FromLong(*boolval);
	}
	case SHM_TYPE(SHM_TYPE_LONG):
	{
		const long *longval = shm_value_get_data(val_data);
		return PyLong_FromLong(*longval);
		break;
	}
	case SHM_TYPE(SHM_TYPE_FLOAT):
	{
		const double *longval = shm_value_get_data(val_data);
		return PyFloat_FromDouble(*longval);
		break;
	}
	case SHM_TYPE(SHM_TYPE_UNICODE):
	{
		const Py_UCS4 *unival = shm_value_get_data(val_data);
		ShmInt size = shm_value_get_size(val_data) / isizeof(Py_UCS4);
		/* Don't do this, kids, coz eventually you going to get:
		 _PyUnicode_CheckConsistency: Assertion `maxchar >= 0x10000' failed.
		PyObject *rslt = PyUnicode_New(size, 0x10ffffU); // 4-byte maxchar
		// from PyUnicode_WriteChar
		if (!PyUnicode_Check(rslt) || !PyUnicode_IS_COMPACT(rslt)) {
			shmassert(false);
			return NULL;
		}
		assert(PyUnicode_IS_READY(rslt));
		if (!unicode_modifiable(rslt))
		{
			shmassert(false);
			return NULL;
		}

		int kind = PyUnicode_KIND(rslt);
		void *data = PyUnicode_DATA(rslt);
		for (ShmInt i = 0; i < size; ++i)
			// PyUnicode_WriteChar(rslt, i, unival[i]);
			PyUnicode_WRITE(kind, data, i, unival[i]);
		*/
		SHM_UNUSED(unicode_modifiable);
		PyObject *rslt = PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, unival, size);
		return rslt;
	}
	case SHM_TYPE(SHM_TYPE_BYTES):
	{
		const char *bytesval = shm_value_get_data(val_data);
		long size2 = shm_value_get_size(val_data);
		return PyBytes_FromStringAndSize(bytesval, size2);
	}
	case SHM_TYPE(SHM_TYPE_TUPLE):
	{
		ShmDictObject *result_obj = PyObject_New(ShmTupleObject, &ShmTuple_Type);
		if (result_obj == NULL)
		{
			PyErr_Format(Shm_Exception, "Error creating ShmTupleObject");
			return NULL;
		}
		shm_pointer_acq(thread, data_shm);
		result_obj->data = data_shm;
		return (PyObject *)result_obj;
	}
	case SHM_TYPE(SHM_TYPE_DEBUG):
		Py_RETURN_NONE;
	default:
		return NULL;
	}
}

PyObject *
shm_pointer_to_object_consume(ShmPointer pntr)
{
	ShmAbstractBlock *block = LOCAL(pntr);
	if (block == NULL)
		Py_RETURN_NONE;
	ShmInt actual_type = shm_type_get_type(block->type);
	if ((block->type & SHM_TYPE_CELL) != SHM_TYPE_CELL)
	{
		PyObject *result_obj = abstract_block_to_object(block, pntr);
		shm_pointer_release(thread, pntr);
		if (result_obj == NULL)
		{
			PyErr_Format(Shm_Exception, "Unknown type of immutable value: %d", actual_type);
			return NULL;
		}
		return result_obj;
		/* ShmValueObject *result_obj = PyObject_New(ShmValueObject, &ShmValue_Type);
		if (result_obj == NULL)
		{
			PyErr_Format(Shm_Exception, "Error creating ShmValueObject");
			return NULL;
		}
		result_obj->data = pntr;
		return (PyObject *)result_obj; */
	}
	else if (actual_type == shm_type_get_type(SHM_TYPE_LIST))
	{
		ShmListObject *result_obj = PyObject_New(ShmListObject, &ShmList_Type);
		if (result_obj == NULL)
		{
			PyErr_Format(Shm_Exception, "Error creating ShmListObject");
			shm_pointer_release(thread, pntr);
			return NULL;
		}
		result_obj->data = pntr;
		return (PyObject *)result_obj;
	}
	else if (actual_type == shm_type_get_type(SHM_TYPE_UNDICT))
	{
		ShmDictObject *result_obj = PyObject_New(ShmDictObject, &ShmDict_Type);
		if (result_obj == NULL)
		{
			PyErr_Format(Shm_Exception, "Error creating ShmDictObject");
			shm_pointer_release(thread, pntr);
			return NULL;
		}
		result_obj->data = pntr;
		return (PyObject *)result_obj;
	}
	else if (actual_type == shm_type_get_type(SHM_TYPE_OBJECT))
	{
		ShmObjectObject *result_obj = NULL;
		ShmUnDict *dict = LOCAL(pntr);
		if (SBOOL(dict->class_name))
		{
			RefUnicode class_path = shm_ref_unicode_get(dict->class_name);
			if (class_path.data == NULL)
			{
				PyErr_Format(Shm_Exception, "Invalid ShmObject without class_name");
				shm_pointer_release(thread, pntr);
				return NULL;
			}
			// parse the tab-separated class path
			int separator = -1;
			for (int i = 0; i < class_path.len; i++)
			{
				if (class_path.data[i] == '\t')
				{
					separator = i;
					break;
				}
			}

			if (separator == -1 || separator == class_path.len - 2)
			{
				PyErr_Format(Shm_Exception, "Invalid ShmObject.class_name");
				shm_pointer_release(thread, pntr);
				return NULL;
			}
			shmassert(class_path.data[class_path.len - 1] == 0); // null-terminator
			PyObject *module_name = PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND,
			                                                  CAST_VL(class_path.data), separator);
			if (module_name == NULL)
			{
				PyErr_Format(Shm_Exception, "Internal error");
				shm_pointer_release(thread, pntr);
				return NULL;
			}
			PyObject *class_name = PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND,
			                                                 CAST_VL(&class_path.data[separator+1]),
			                                                 class_path.len - 1 - separator - 1);
			if (class_name == NULL)
			{
				PyErr_Format(Shm_Exception, "Internal error");
				Py_DECREF(module_name);
				shm_pointer_release(thread, pntr);
				return NULL;
			}
			// _pickle.c: _pickle_Unpickler_find_class_impl()
			PyObject *module = PyImport_Import(module_name);
			PyObject *cls = NULL;
			if (module == NULL)
			{
				PyErr_Format(Shm_Exception, "Failed to load module %U", module_name);
				goto error;
			}
			if (_PyObject_LookupAttr(module, class_name, &cls) < 0)
			{
				PyErr_Format(Shm_Exception, "Internal error");
				Py_DECREF(module);
				goto error;
			}
			Py_DECREF(module);
			if (cls == NULL)
			{
				PyErr_Format(Shm_Exception, "Class %U not found in module %U", class_name, module_name);
				goto error;
			}
			if (!PyType_Check(cls))
			{
				PyErr_Format(Shm_Exception, "%U.%U is not a class", module_name, class_name);
				Py_DECREF(cls);
				goto error;
			}
			PyTypeObject *_cls = (PyTypeObject *)cls;
			if (_cls != &ShmObject_Type && !PyType_IsSubtype(_cls, &ShmObject_Type))
			{
				PyErr_Format(Shm_Exception, "Cannot put into shared memory a class %U that is not derrived from pso.ShmObject", class_name);
				Py_DECREF(cls);
				goto error;
			}
			// _pickle.c: instantiate()
			_Py_IDENTIFIER(__new__);
			PyObject *instance = _PyObject_CallMethodIdObjArgs(cls, &PyId___new__, cls, NULL);
			if (instance == NULL)
			{
				PyErr_Format(Shm_Exception, "Failed to instantiate class %U from module %U", class_name, module_name);
				goto error;
			}
			if (PyObject_IsInstance(instance, cls) != 1)
			{
				PyErr_Format(Shm_Exception, "For some reason %U.%U is not derrived from pso.ShmObject despite the fact its class is.",
				             module_name, class_name);

				goto error;
			}
			result_obj = (ShmObjectObject *)instance;
		error:
			Py_XDECREF(cls);
			cls = NULL;
			Py_DECREF(module_name);
			module_name = NULL;
			Py_XDECREF(class_name);
			class_name = NULL;
			if (result_obj == NULL)
				shm_pointer_release(thread, pntr);
			else
				result_obj->data = pntr;
		}
		else
		{
			result_obj = PyObject_New(ShmObjectObject, &ShmObject_Type);
			if (result_obj == NULL)
			{
				PyErr_Format(Shm_Exception, "Error creating ShmObjectObject");
				shm_pointer_release(thread, pntr);
				return NULL;
			}
			else
				result_obj->data = pntr;
		}
		return (PyObject*)result_obj;
	}
	else if (actual_type == shm_type_get_type(SHM_TYPE_PROMISE))
	{
		ShmPromiseObject *result_obj = PyObject_New(ShmPromiseObject, &ShmPromise_Type);
		if (result_obj == NULL)
		{
			PyErr_Format(Shm_Exception, "Error creating ShmPromiseObject");
			shm_pointer_release(thread, pntr);
			return NULL;
		}
		result_obj->data = pntr;
		return (PyObject *)result_obj;
	}
	else
	{
		PyErr_Format(Shm_Exception, "Unknown type of value: %d", actual_type);
		shm_pointer_release(thread, pntr);
		return NULL;
	}
}

PyObject *
abstract_block_to_repr(ShmValueHeader *val_data, const char *debug_name, void *pntr)
{
	switch (shm_type_get_type(val_data->type)) {
	case SHM_TYPE(SHM_TYPE_BOOL):
	{
		const char *s = bool_repr(shm_value_get_data(val_data));
		return PyUnicode_FromFormat("\"%s\" <%s object at %p>", s, debug_name, pntr);
		break;
	}
	case SHM_TYPE(SHM_TYPE_LONG):
	{
		const long *val = shm_value_get_data(val_data);
		if (val)
			return PyUnicode_FromFormat("%ld <%s object at %p>", *val, debug_name, pntr);
		else
			return PyUnicode_FromFormat("<empty %s object at %p>", debug_name, pntr);
		break;
	}
	case SHM_TYPE(SHM_TYPE_FLOAT):
	{
		const double *val = shm_value_get_data(val_data);
		if (val)
		{
			PyObject *obj = PyFloat_FromDouble(*val);
			PyObject *rslt = PyUnicode_FromFormat("%S <%s object at %p>", *obj, debug_name, pntr);
			Py_DECREF(obj);
			return rslt;
		}
		else
			return PyUnicode_FromFormat("<empty %s object at %p>", debug_name, pntr);

		break;
	}
	case SHM_TYPE(SHM_TYPE_UNICODE):
	{
		int size = shm_value_get_size(val_data) / isizeof(Py_UCS4);
		// Py_UCS1 *buf = calloc((size_t)(size + 1), 1);
		// ASCII_from_UCS4(buf, shm_value_get_data(val_data), size);
		// buf[size] = 0;
		// return PyUnicode_FromFormat("%s <string %s at %p>", buf, debug_name, pntr);
		PyObject *buf = PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, shm_value_get_data(val_data), size);
		shmassert(buf);
		PyObject *rslt = PyUnicode_FromFormat("%U <string %s at %p>", buf, debug_name, pntr);
		Py_DECREF(buf);
		return rslt;
		break;
	}
	case SHM_TYPE(SHM_TYPE_BYTES):
	{
		PyObject *buf = PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, shm_value_get_data(val_data),
		                                          shm_value_get_size(val_data));
		shmassert(buf);
		PyObject *rslt = PyUnicode_FromFormat("%U <bytes %s at %p>", buf, debug_name, pntr);
		Py_DECREF(buf);
		return rslt;
		break;
	}
	case SHM_TYPE(SHM_TYPE_DEBUG):
		return PyUnicode_FromFormat("Debug-type object <%s object at %p>", val_data->type, debug_name, pntr);
		break;
	default:
		return PyUnicode_FromFormat("Unknown type %d <%s object at %p>", val_data->type, debug_name, pntr);
	}
	return NULL;
}

static PyObject*
get_root(PyObject *self, PyObject *args) {
	if (!check_thread_inited())
		return NULL;
	ShmPointer pntr = superblock->root_container;
	shm_pointer_acq(thread, pntr); // TODO: delete this line and you can debug the undict->count problem
	PyObject *rslt = shm_pointer_to_object_consume(pntr);
	return rslt;
}

static PyObject *
ShmValue_repr(ShmListObject *self)
{
	if (self->data == NONE_SHM)
		return PyUnicode_FromFormat("None <empty %s object at %p>", self->ob_base.ob_type->tp_name, self);
	ShmValueHeader *val_data = LOCAL(self->data);
	if ( ! val_data )
		return PyUnicode_FromFormat("<empty %s object at %p>", self->ob_base.ob_type->tp_name, self);
	else
	{
		return abstract_block_to_repr(val_data, self->ob_base.ob_type->tp_name, self);
	}
	// "%s", _PyType_Name(Py_TYPE(ro))
}

static void
ShmBase_dealloc(ShmBase *self)
{
	// if (shm_pointer_is_valid(self->data) && self->persistent)
	if (shm_pointer_is_valid(self->data) && true)
		shm_pointer_release(thread, self->data); // dec ref/release
	else
		debug_print("Nothing to release\n");

	Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyTypeObject ShmValue_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pso.ShmValue",
	.tp_doc = "Shared immutable value.",
	.tp_basicsize = sizeof(ShmValueObject),
	.tp_repr = (reprfunc) ShmValue_repr,
	.tp_dealloc = (destructor) ShmBase_dealloc,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	// .tp_methods = ShmValue_methods,
	// .tp_members = ShmValue_members,
	.tp_init = (initproc) ShmValue_init,
	.tp_new = PyType_GenericNew,
};

// ///////////
// ShmTuple
// ////////////

// Tuple is immutable once created.
int
tuple_to_shm_tuple(PyObject *obj, __ShmPointer *rslt)
{
	int arglen = PyTuple_GET_SIZE(obj);
	*rslt = EMPTY_SHM;
	ShmValueHeader *header = new_shm_value(thread, isizeof(ShmPointer) * arglen, SHM_TYPE_TUPLE, rslt);
	__ShmPointer *elements = shm_value_get_data(header);
	memset(elements, EMPTY_SHM, isizeof(ShmPointer) * arglen);
	for (int i = 0; i < arglen; i++)
	{
		PyObject *item = PyTuple_GET_ITEM(obj, i);
		shmassert(item);
		ShmPointer newval = EMPTY_SHM;
		if (prepare_item_for_shm_container(item, &newval) == -1 || newval == EMPTY_SHM)
		{
			if (!PyErr_Occurred())
				PyErr_Format(Shm_Exception,
				             "could not marshall tuple's item at index %d of type %.200s",
				             i, item->ob_type->tp_name);
			return -1;
		}
		elements[i] = newval;
	}
	return 0;
}

static int
ShmTuple_init(ShmTupleObject *self, PyObject *args, PyObject *kwds)
{
	self->data = EMPTY_SHM;
	if (!check_thread_inited())
		return -1;
	PyObject *obj = NULL;
	if (!PyArg_ParseTuple(args, "O", &obj))
		return -1;
	shmassert(obj != NULL);
	if (PyTuple_Check(obj) == false)
	{
		PyErr_Format(PyExc_TypeError,
		             "cannot initialize ShmTuple from %.200s, tuple required instead",
		             obj->ob_type->tp_name);
	}

	return tuple_to_shm_tuple(obj, &self->data);
}

static Py_ssize_t
ShmTuple_length(ShmTupleObject *self)
{
	ShmValueHeader *header = LOCAL(self->data);
	if (!header)
		return -1;

	int size = shm_value_get_length(header);
	shmassert(size % sizeof(ShmPointer) == 0);
	return size / sizeof(ShmPointer);
}

static Py_ssize_t
ShmTupleValue_length(ShmValueHeader *header)
{
	int size = shm_value_get_length(header);
	return size / sizeof(ShmPointer);
}

static PyObject *
ShmTupleValue_GetItem(ShmValueHeader *header, Py_ssize_t i, Py_ssize_t size)
{
	if (i < 0 || i >= size)
	{
		PyErr_SetString(PyExc_IndexError, "tuple index out of range");
		return NULL;
	}
	ShmPointer *data = shm_value_get_data(header);
	ShmPointer value = data[i];
	shm_pointer_acq(thread, value);
	return shm_pointer_to_object_consume(value);
}

static PyObject *
ShmTuple_item(ShmTupleObject *self, Py_ssize_t i)
{
	ShmValueHeader *header = LOCAL(self->data);
	if (!header)
		return NULL;
	return ShmTupleValue_GetItem(header, i, ShmTupleValue_length(header));
}

static PyObject*
ShmTuple_subscript(ShmTupleObject* self, PyObject* item)
{
	ShmValueHeader *header = LOCAL(self->data);
	if (!header)
		return NULL;
	if (PyIndex_Check(item)) {
		Py_ssize_t i = PyNumber_AsSsize_t(item, PyExc_IndexError);
		if (i == -1 && PyErr_Occurred())
			return NULL;
		Py_ssize_t size = ShmTupleValue_length(header);
		if (i < 0)
			i += size;
		return ShmTupleValue_GetItem(header, i, size);
	}
	else if (PySlice_Check(item))
	{
		PyErr_SetString(PyExc_IndexError, "slices are not supproted for ShmTuple");
		return NULL;
	}
	else
	{
		PyErr_Format(PyExc_TypeError,
		             "tuple indices must be integers or slices, not %.200s",
		             Py_TYPE(item)->tp_name);
		return NULL;
	}
}

static PyObject *
ShmTuple_iter(PyObject *obj)
{
	if (!PyObject_TypeCheck(obj, &ShmTuple_Type))
	{
		PyErr_SetString(PyExc_ValueError, "tuple must be ShmTuple");
		return NULL;
	}

	TupleRef tuple;
	tuple.shared = ((ShmTupleObject *)obj)->data;
	tuple.local = LOCAL(tuple.shared);
	if (tuple.local == NULL)
	{
		PyErr_SetString(PyExc_ValueError, "no tuple");
		return NULL;
	}

	ShmTupleIterObject *it;
	// PyObject_Init() ?
	it = PyObject_New(ShmTupleIterObject, &ShmTupleIter_Type);
	if (it == NULL)
		return NULL;

	shm_pointer_acq(thread, tuple.shared);
	it->tuple = tuple.local;
	it->tuple_shm = tuple.shared;
	it->itemindex = 0;
	return (PyObject *)it;
}

static PySequenceMethods ShmTuple_as_sequence = {
	.sq_length = (lenfunc)ShmTuple_length,
	.sq_item = (ssizeargfunc)ShmTuple_item,
};

static PyMappingMethods ShmTuple_as_mapping = {
	(lenfunc)ShmTuple_length,
	(binaryfunc)ShmTuple_subscript,
	0
};

static PyTypeObject ShmTuple_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pso.ShmTuple",
	.tp_doc = "Shared mutable list",
	.tp_basicsize = sizeof(ShmTupleObject),
	// .tp_repr = (reprfunc) ShmTuple_repr,
	.tp_dealloc = (destructor) ShmBase_dealloc,
	.tp_getattro = PyObject_GenericGetAttr,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_as_sequence = &ShmTuple_as_sequence,
	.tp_as_mapping = &ShmTuple_as_mapping,
	.tp_iter = ShmTuple_iter,
	// .tp_methods = ShmTuple_methods,
	// .tp_members = ShmTuple_members,
	.tp_init = (initproc) ShmTuple_init,
	.tp_new = PyType_GenericNew,
};

static PyObject *
ShmTupleIter_next(ShmTupleIterObject *it)
{
	ShmValueHeader *header = LOCAL(it->tuple_shm);
	if (!header)
		return NULL;

	int size = ShmTupleValue_length(header);
	if (it->itemindex < 0)
	{
		PyErr_SetString(PyExc_IndexError, "tuple index out of range");
		return NULL;
	}
	if (it->itemindex >= size)
		return NULL; // end
	ShmPointer *data = shm_value_get_data(header);
	ShmPointer value = data[it->itemindex];
	it->itemindex++;
	shmassert(SBOOL(value));
	shm_pointer_acq(thread, value);
	return shm_pointer_to_object_consume(value);
}

static void
ShmTupleIter_dealloc(ShmBase *self)
{
	if (shm_pointer_is_valid(self->data))
		shm_pointer_release(thread, self->data);
	else
		debug_print("Nothing to release\n");

	Py_TYPE(self)->tp_free((PyObject *)self);
}

// copied from listobject.h
static PyTypeObject ShmTupleIter_Type = {
	// PyVarObject_HEAD_INIT(&PyType_Type, 0)
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "ShmTupleIter",
	.tp_basicsize = sizeof(ShmTupleIterObject),
	.tp_itemsize = 0,
	.tp_dealloc = (destructor)ShmTupleIter_dealloc,
	.tp_getattro = PyObject_GenericGetAttr,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_iter = PyObject_SelfIter,
	.tp_iternext = (iternextfunc)ShmTupleIter_next,
};

// //////////////////////
//       ShmList
// //////////////////////

int
list_to_shm_list(PyObject *obj, __ShmPointer *rslt)
{
	PyObject *seq = PySequence_Fast(obj, "");
	if (seq == NULL) return -1;
	Py_ssize_t arglen = PySequence_Length(seq);
	*rslt = EMPTY_SHM;

	ShmList *list = new_shm_list_with_capacity(thread, rslt, arglen);
	// all elements in the list are EMPTY_SHM now
	for (int i = 0; i < arglen; i++)
	{
		PyObject *item = PySequence_Fast_GET_ITEM(seq, i);
		shmassert(item);
		ShmPointer newval = EMPTY_SHM;
		if (prepare_item_for_shm_container(item, &newval) == -1 || newval == EMPTY_SHM)
		{
			if (!PyErr_Occurred())
				PyErr_Format(Shm_Exception,
							 "could not marshall sequence item at index %d of type %.200s",
							 i, item->ob_type->tp_name);
			return -1;
		}

		int status = shm_list_set_item_raw(thread, (ListRef){.local = list, .shared = *rslt}, i, newval, true);
		if (status == RESULT_INVALID)
		{
			PyErr_Format(Shm_Exception, "internal failure in list_to_shm_list");
			return -1;
		}
		if (status == RESULT_FAILURE)
		{
			PyErr_Format(Shm_Exception, "internal failure in shm_list_set_item_raw");
			return -1;
		}
		shmassert(status == RESULT_OK);
	}

	return 0;
}

static int
ShmList_init(ShmListObject *self, PyObject *args, PyObject *kwds)
{
	self->data = EMPTY_SHM;
	if (!check_thread_inited())
		return -1;

	PyObject *obj = NULL;
	if (!PyArg_ParseTuple(args, "|O", &obj))
		return -1;
	if (obj == NULL)
	{
		new_shm_list(thread, &self->data);
		return 0;
	}
	if (PySequence_Check(obj) == false)
	{
		PyErr_Format(PyExc_TypeError,
					 "cannot initialize ShmList from %.200s, list or other sequence object is required",
					 obj->ob_type->tp_name);
	}

	return list_to_shm_list(obj, &self->data);
}

static PyObject *
ShmList_repr(ShmListObject *self)
{
	ListRef list;
	list.shared = self->data;
	list.local = LOCAL(self->data);
	if ( ! list.local )
		return PyUnicode_FromFormat("<empty %s object at %p>", self->ob_base.ob_type->tp_name, self);
	else
	{
		// shmassert(thread->transaction_mode != TRANSACTION_TRANSIENT);

		/*if_failure(
			transaction_lock_read(thread, &list.local->base.lock, list.shared, CONTAINER_LIST, NULL),
			{
				transient_abort(thread);
				PyErr_SetString(PyExc_ValueError, "Failed to acquire reading lock.");
				return NULL;
			}
		);
		shm_cell_check_read_write_lock(thread, &list.local->base.lock);

		ShmInt count;
		ShmInt new_count;
		shm_list_get_fast_count(thread, list.local, &count, &new_count);
		if (shm_cell_have_write_lock(thread, &list.local->base.lock))
			count = new_count;

		for (int idx = 0; idx < count; ++idx)
		{
			ShmPointer value;
			shm_list_get_item(thread, list, idx, &value);
		}*/

		/* Fast counts check is not valid anymore due to deletion support
		ShmListCounts count = SHM_LIST_INVALID_COUNTS;
		count = shm_list_get_fast_count(thread, list.local, shm_cell_have_write_lock(thread, &list.local->base.lock));

		ShmPointer value_shm = EMPTY_SHM;
		if (count.count > 0)
			shm_list_get_item(thread, list, 0, &value_shm);
		*/
		ShmPointer value_shm = EMPTY_SHM;
		bool out_of_range = false;
		RETRY_LOOP(shm_list_acq_item(thread, list, 0, &value_shm),
			{
				shmassert(value_shm == EMPTY_SHM);
				if (_rslt == RESULT_INVALID)
				{
					out_of_range = true;
					break;
				}
			},
			{
				shm_pointer_release(thread, value_shm);
				return NULL;
			},
			{
				PyErr_SetString(Shm_Exception, "Internal failure in ShmList_repr");
				shm_pointer_release(thread, value_shm);
				return NULL;
			});

		shmassert(value_shm != EMPTY_SHM || out_of_range == true);
		ShmValueHeader *first_value = LOCAL(value_shm);
		value_shm = EMPTY_SHM;
		// char *astr = "None";
		if (first_value)
		{
			// astr = (char *)shm_value_get_data(first_value);
			// fprintf(stderr, "first_value %#08x\n", (unsigned int)first_value);
			PyObject *first_uni = abstract_block_to_repr(first_value, "temporary", NULL);
			shm_pointer_release(thread, value_shm);
			return PyUnicode_FromFormat("ShmList: %d elements <%s object at %p>, first element: %U", list.local->count, self->ob_base.ob_type->tp_name, self, first_uni);
		}
		shm_pointer_release(thread, value_shm);
		return PyUnicode_FromFormat("ShmList: %d elements <%s object at %p>, empty", list.local->count, self->ob_base.ob_type->tp_name, self);
	}
	// "%s", _PyType_Name(Py_TYPE(ro))
}

// Iterator is meaningless outside transaction it was created in.
// Due to the locks always being held for the whole duration of transaction
// we can consider a shortcut: just take the read lock and forget about it.
// For transient transactions we just hope the list is unchanged.
static PyObject *
ShmList_iter(PyObject *obj)
{
	if (!PyObject_TypeCheck(obj, &ShmList_Type))
	{
		PyErr_SetString(PyExc_ValueError, "list must be ShmList");
		return NULL;
	}

	ListRef list;
	list.shared = ((ShmListObject *)obj)->data;
	list.local = LOCAL(list.shared);
	if ( ! list.local )
	{
		PyErr_SetString(PyExc_ValueError, "no list");
		return NULL;
	}

	// shmassert(thread->transaction_mode != TRANSACTION_TRANSIENT);

	// if_failure(
	// 	transaction_lock_read(thread, &list.local->base.lock, list.shared, CONTAINER_LIST, NULL),
	// 	{
	// 		transient_abort(thread);
	// 		PyErr_SetString(PyExc_ValueError, "Failed to acquire reading lock.");
	// 		return NULL;
	// 	}
	// );

	int mode = thread->transaction_mode;
	shmassert_msg(mode != TRANSACTION_TRANSIENT, "Unfinished transient transaction in ShmList_iter");
	shmassert_msg(mode != TRANSACTION_NONE, "Thread inactive in ShmList_iter");
	bool is_transient = mode == TRANSACTION_IDLE;
	shmassert(is_transient || mode == TRANSACTION_PERSISTENT);
	if (is_transient == false)
	{
		RETRY_LOOP(transaction_lock_read(thread, &list.local->base.lock, list.shared, CONTAINER_LIST, NULL),
			{},
			{ return NULL; },
			{
				PyErr_SetString(Shm_Exception, "Internal failure during shm_list_iter");
				return NULL;
			});

		shm_cell_check_read_write_lock(thread, &list.local->base.lock);
	}

	// ShmPointer shm_cell;
	// ShmCell *cell = shm_list_get_first(thread, list, &shm_cell);

	ShmListIterObject *it;
	it = PyObject_New(ShmListIterObject, &ShmListIter_Type);
	if (it == NULL)
		return NULL;

	// it->persistent = true;
	// it->cell = shm_cell;
	it->list = list.local;
	shm_pointer_acq(thread, list.shared);
	it->list_shm = list.shared;
	it->itemindex = 0;
	it->is_transient = is_transient;
	return (PyObject *)it;
}

static PyObject*
_shm_list_append(ShmListObject *self, PyObject *value) {
	ListRef list;
	list.shared = self->data;
	list.local = LOCAL(self->data);
	if ( ! list.local )
	{
		PyErr_SetString(PyExc_ValueError, "List object internal data is uninitialized.");
		return NULL;
	}

	/* if (!PyObject_TypeCheck(value, &ShmValue_Type)) {
		PyErr_SetString(PyExc_ValueError, "value must be ShmValue");
		return NULL;
	}
	ShmValueObject * _value = (ShmValueObject *) value;
	*/

	ShmPointer newval = EMPTY_SHM;
	if (prepare_item_for_shm_container(value, &newval) == -1 || newval == EMPTY_SHM)
	{
		if (!PyErr_Occurred())
			PyErr_SetString(Shm_Exception, "Invalid pso object when appending to ShmList");
		return NULL;
	}

	// CellRef cell;
	ShmInt index = -1;
		// RETRY_LOOP(shm_list_append(thread, list, _value->data, &index),
		RETRY_LOOP(shm_list_append(thread, list, newval, &index),
		{ shmassert(index == -1); },
		{ return NULL; },
		{
			PyErr_SetString(Shm_Exception, "Internal failure");
			return NULL;
		});

	// acquires the shm_list_append result
	// PyObject *obj =  PyObject_CallFunction((PyObject *)&ShmObject_Type, "");
	// ShmObjectObject *cellobj = (ShmObjectObject *) obj;
	// if ( ! cellobj )
	// {
	//	PyErr_SetString(PyExc_ValueError, "_shm_list_append: Failed to pack the result cell.");
	// 	return NULL;
	// }
	// shm_pointer_acq(cell.shared);
	// cellobj->data = cell.shared;
	return PyLong_FromLong(index);
}

static Py_ssize_t
shm_list_length(ShmListObject *self)
{
	ListRef list;
	if (!init_list_ref(self->data, &list))
	{
		PyErr_SetString(Shm_Exception, "Invalid list object");
		return -1;
	}
	ShmListCounts count = shm_list_get_fast_count(thread, list.local, shm_cell_have_write_lock(thread, &list.local->base.lock));
	return count.count;
}

static PyObject *
shm_list_item(ShmListObject* self, Py_ssize_t i)
{
	ListRef list;
	if (!init_list_ref(self->data, &list))
	{
		PyErr_SetString(Shm_Exception, "Invalid list object");
		return NULL;
	}
	if (i < 0)
	{
		PyErr_SetString(PyExc_IndexError, "list index out of range");
		return NULL;
	}
	ShmPointer value = EMPTY_SHM;
	bool out_of_range = false;
	RETRY_LOOP(shm_list_acq_item(thread, list, i, &value),
		{
			shmassert(value == EMPTY_SHM);
			if (_rslt == RESULT_INVALID)
			{
				out_of_range = true;
				break;
			}
		},
		{ return NULL; },
		{
			PyErr_SetString(Shm_Exception, "Internal failure");
			return NULL;
		});
	shmassert(value != EMPTY_SHM || out_of_range == true);
	if (out_of_range)
	{
		PyErr_SetString(PyExc_IndexError, "list index out of range");
		return NULL;
	}
	PyObject *result_obj = shm_pointer_to_object_consume(value);
	return result_obj;
}

static PyObject *
shm_list_subscript(ShmListObject* self, PyObject* item)
{
	if (PyIndex_Check(item))
	{
		Py_ssize_t i;
		i = PyNumber_AsSsize_t(item, PyExc_IndexError);
		if (i == -1 && PyErr_Occurred())
			return NULL;
		if (i < 0)
			i += PyList_GET_SIZE(self);
		return shm_list_item(self, i);
	}
	else if (PySlice_Check(item))
	{
		return NULL;
	}
	else
	{
		PyErr_Format(PyExc_TypeError,
		             "list indices must be integers or slices, not %.200s",
		             item->ob_type->tp_name);
		return NULL;
	}
}

static int
shm_list_ass_item(ShmListObject *self, Py_ssize_t i, PyObject *value)
{
	ListRef list;
	if (i < 0) {
		PyErr_SetString(PyExc_IndexError,
		                "list assignment index out of range");
		return -1;
	}
	if (!init_list_ref(self->data, &list))
	{
		PyErr_SetString(Shm_Exception, "Invalid list object");
		return -1;
	}

	ShmPointer newval = EMPTY_SHM;
	if (prepare_item_for_shm_container(value, &newval) == -1 || newval == EMPTY_SHM)
	{
		if (!PyErr_Occurred())
			PyErr_SetString(Shm_Exception, "Invalid pso object when assigning ShmList's item");
		return -1;
	}

	bool out_of_range = false;
	RETRY_LOOP(shm_list_set_item(thread, list, i, newval),
	{
			if (_rslt == RESULT_INVALID)
			{
				out_of_range = true;
				break;
			}
		},
		{ return -1; },
		{
			PyErr_SetString(Shm_Exception, "Internal failure");
			return -1;
		});

	shmassert(newval != EMPTY_SHM || out_of_range == true);
	if (out_of_range)
	{
		PyErr_SetString(PyExc_IndexError,
		                "list assignment index out of range");
		return -1;
	}
	shm_pointer_release(thread, newval);
	return 0;
}

static int
shm_list_ass_subscript(ShmListObject* self, PyObject* item, PyObject* value)
{
	if (PyIndex_Check(item)) {
		Py_ssize_t i = PyNumber_AsSsize_t(item, PyExc_IndexError);
		if (i == -1 && PyErr_Occurred())
			return -1;
		if (i < 0)
			i += PyList_GET_SIZE(self);
		return shm_list_ass_item(self, i, value);
	}
	else if (PySlice_Check(item)) {
		return -1;
	}
	else {
		PyErr_Format(PyExc_TypeError,
		             "list indices must be integers or slices, not %.200s",
		             item->ob_type->tp_name);
		return -1;
	}
}

static PySequenceMethods shm_list_as_sequence = {
	.sq_length = (lenfunc)shm_list_length,
	.sq_item = (ssizeargfunc)shm_list_item,
	.sq_ass_item = (ssizeobjargproc)shm_list_ass_item,
};

static PyMappingMethods shm_list_as_mapping = {
	(lenfunc)shm_list_length,
	(binaryfunc)shm_list_subscript,
	(objobjargproc)shm_list_ass_subscript
};

static PyMethodDef ShmList_methods[] = {
	// {
	//     "new_value", (PyCFunction)_shm_list_new_value, METH_O,
	//     "Returns new value for use in the list."
	// },
	{
		"append", (PyCFunction)_shm_list_append, METH_O,
		"Appends a value to the list"
	},
	{NULL, NULL} // sentinel
};

static PyTypeObject ShmList_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pso.ShmList",
	.tp_doc = "Shared mutable list",
	.tp_basicsize = sizeof(ShmListObject),
	.tp_repr = (reprfunc) ShmList_repr,
	.tp_dealloc = (destructor) ShmBase_dealloc,
	.tp_getattro = PyObject_GenericGetAttr,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_as_sequence = &shm_list_as_sequence,
	.tp_as_mapping = &shm_list_as_mapping,
	.tp_iter = ShmList_iter,
	.tp_methods = ShmList_methods,
	// .tp_members = ShmList_members,
	.tp_init = (initproc) ShmList_init,
	.tp_new = PyType_GenericNew,
};


static PyObject *
ShmListIter_next(ShmListIterObject *it)
{
	if (thread->transaction_mode != TRANSACTION_PERSISTENT)
	{
		if (it->is_transient)
			shmassert(thread->transaction_mode == TRANSACTION_IDLE);
		else
		{
			PyErr_SetString(Shm_Exception, "Cannot use transacted iterator outside its transaction");
			return NULL;
		}
	}
	/*assert(it != NULL);
	ListCellRef cell;
	if ( ! init_listcell_ref(it->cell, &cell))
	{
		return NULL;
	}
	ShmPointer shm_next;
	shm_listcell_get_next(thread, cell.local, &shm_next);
	it->cell = shm_next;
	// debug_print("cell->next %p, cell->new_next %p\n", cell.local->next, cell.local->new_next);

	ShmPointer shm_value;
	if ( ! cell_get_value(thread, (ShmCell *)cell.local, &shm_value))
	{
		// debug_print("exit 2, cell is %p, value is %p\n", cell.shared, shm_value);
		// return NULL;
		shm_value = NONE_SHM;
	}*/

	ShmPointer value = EMPTY_SHM;
	ListRef list = { .local = it->list, .shared = it->list_shm };
	bool out_of_range = false;
	// Could optimize into shm_list_get_item for persistent transactions.
	// shm_list_acq_item() is doing locking for transient transactions too.
	RETRY_LOOP(shm_list_acq_item(thread, list, it->itemindex, &value),
		{
			shmassert(value == EMPTY_SHM);
			if (_rslt == RESULT_INVALID)
			{
				out_of_range = true;
				break;
			}
		},
		{
			shm_pointer_release(thread, value);
			return NULL;
		},
		{
			PyErr_Format(Shm_Exception, "Error (%d) getting item at index %d.", _rslt, it->itemindex);
			shm_pointer_release(thread, value);
			return NULL;
		});

	shmassert(value != EMPTY_SHM || out_of_range == true);
	if (out_of_range)
		return NULL;

	PyObject *result_obj = shm_pointer_to_object_consume(value);
	it->itemindex++;
	// result_obj->persistent = true;
	return result_obj;
}

static void
ShmListIter_dealloc(ShmBase *self)
{
	if (shm_pointer_is_valid(self->data))
		shm_pointer_release(thread, self->data);
	else
		debug_print("Nothing to release\n");

	Py_TYPE(self)->tp_free((PyObject *)self);
}

// copied from listobject.h
static PyTypeObject ShmListIter_Type = {
	// PyVarObject_HEAD_INIT(&PyType_Type, 0)
		PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "shm_list_iterator",
	.tp_basicsize = sizeof(ShmListIterObject),
	.tp_itemsize = 0,
	/* methods */
	.tp_dealloc = (destructor)ShmListIter_dealloc,
	.tp_getattro = PyObject_GenericGetAttr,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_iter = PyObject_SelfIter,
	.tp_iternext = (iternextfunc)ShmListIter_next,
	// .tp_methods = listiter_methods,
};

// ////////////////
// ShmDict
// /////////////////

int
dict_to_shm_dict(PyObject *obj, __ShmPointer *rslt)
{
	*rslt = EMPTY_SHM;
	ShmUnDict *dict = new_shm_undict(thread, rslt);

	// similar to dict_merge()
	PyObject *keys = PyMapping_Keys(obj);
	if (keys == NULL)
		return -1;
	PyObject *iter = PyObject_GetIter(keys);
	Py_DECREF(keys);
	if (iter == NULL)
		return -1;

	for (PyObject *key = PyIter_Next(iter); key; key = PyIter_Next(iter))
	{
		ShmUnDictKey dictkey = EMPTY_SHM_UNDICT_KEY;
		if (!object_to_dict_key(key, &dictkey))
		{
			Py_DECREF(iter);
			Py_DECREF(key);
			PyErr_Format(PyExc_TypeError,
			             "dict key must be a string, not %.200s",
			             key->ob_type->tp_name);
			return -1;
		}
		PyObject *value = PyObject_GetItem(obj, key);
		if (value == NULL)
		{
			Py_DECREF(iter);
			Py_DECREF(key);
			return -1;
		}
		ShmPointer newval = EMPTY_SHM;
		if (prepare_item_for_shm_container(value, &newval) < 0)
		{
			Py_DECREF(iter);
			Py_DECREF(key);
			Py_DECREF(value);
			if (!PyErr_Occurred())
				PyErr_SetString(Shm_Exception, "Invalid pso object when assigning ShmDict's item");
			return -1;
		}
		int status = shm_undict_set_item_raw(thread, dict, &dictkey, newval, true);
		shmassert(RESULT_OK == status);
		Py_DECREF(key);
		Py_DECREF(value);
	}
	Py_DECREF(iter);
	if (PyErr_Occurred())
		/* Iterator completed, via error */
		return -1;

	return 0;
}

static int
ShmDict_init(ShmDictObject *self, PyObject *args, PyObject *kwds)
{
	self->data = EMPTY_SHM;
	if (!check_thread_inited())
		return -1;
	PyObject *obj = NULL;
	if (!PyArg_ParseTuple(args, "|O", &obj))
		return -1;
	if (obj == NULL)
	{
		ShmUnDict *dict = new_shm_undict(thread, &self->data);
		shmassert(dict->type == SHM_TYPE_UNDICT);
		return 0;
	}
	if (PyDict_Check(obj) == false)
	{
		PyErr_Format(PyExc_TypeError,
		             "cannot initialize ShmDict from %.200s, dict required instead",
		             obj->ob_type->tp_name);
	}
	return dict_to_shm_dict(obj, &self->data);
}

static Py_ssize_t
ShmDictObject_length(ShmDictObject *mp)
{
	UnDictRef dict;
	if (!init_undict_ref(mp->data, &dict))
	{
		PyErr_SetString(Shm_Exception, "Invalid ShmDict object");
		return -1;
	}
	ShmInt count = -1;
	RETRY_LOOP(shm_undict_get_count(thread, dict, &count),
		{
			shmassert(count == -1);
		},
		{
			return -1;
		},
		{
			PyErr_SetString(Shm_Exception, "Internal failure in ShmDictObject_length");
			return -1;
		});
	return count;
}

// borrows the data from key, so the dictkey doesn't need to be freed
bool
object_to_dict_key(PyObject *key, ShmUnDictKey *dictkey)
{
	// hashing function for small strings from pyhash.c (DJBX33A) gives very poor dispersion.
	// Also see dictobject.c comments.
	PyTypeObject *type = Py_TYPE(key);
	if (type == &PyBytes_Type) {
		/*char* val;
		Py_ssize_t size;
		if (PyBytes_AsStringAndSize(key, &val, &size) == -1)
			return false;
		dictkey->key = val;
		dictkey->keysize = size;
		dictkey->hash = hash_string(val, size);
		return true;*/
		shmassert_msg(false, "not supported");
		return false;
	}
	else if (type == &PyUnicode_Type) {
		// from as_ucs4() and PyUnicode_GetLength()
		if (!PyUnicode_Check(key)) {
			return false;
		}
		if (PyUnicode_READY(key) == -1)
		{
			shmassert(false);
			return false;
		}
		void *data;
		Py_ssize_t size;
		enum PyUnicode_Kind kind;
		size  = PyUnicode_GET_LENGTH(key);
		if (size == -1)
			return false;
		kind = PyUnicode_KIND(key);
		data = PyUnicode_DATA(key);

		*dictkey = (ShmUnDictKey)EMPTY_SHM_UNDICT_KEY;
		dictkey->keysize = size;
		switch (kind)
		{
			case PyUnicode_1BYTE_KIND:
				dictkey->key1 = data;
				dictkey->hash = hash_string_ascii(data, size);
				break;
			case PyUnicode_2BYTE_KIND:
				shmassert(false);
				dictkey->key2 = data;
				shmassert_msg(false, "Unsupported 2-byte string");
				// dictkey->hash = hash_string(buffer, size);
				break;
			case PyUnicode_4BYTE_KIND:
				dictkey->key4 = data;
				dictkey->hash = hash_string(data, size);
				break;
			default:
				shmassert(false);
		}
		return true;
	}

	return false;
}

static PyObject *
ShmDictObject_subscript(ShmDictObject *mp, PyObject *key)
{
	// if (!PyUnicode_Check(key) ||

	ShmUnDictKey dictkey = EMPTY_SHM_UNDICT_KEY;
	if (!object_to_dict_key(key, &dictkey))
	{
		PyErr_Format(PyExc_TypeError,
		             "dict key must be a string, not %.200s",
		             key->ob_type->tp_name);
		return NULL;
	}

	UnDictRef dict;
	if (!init_undict_ref(mp->data, &dict))
	{
		PyErr_SetString(Shm_Exception, "init_undict_ref(mp->data, &dict)");
		return NULL;
	}

	ShmPointer result_value = EMPTY_SHM;
	RETRY_LOOP(shm_undict_acq(thread, dict, &dictkey, &result_value),
		{ shmassert(result_value == EMPTY_SHM); },
		{
			shm_pointer_release(thread, result_value);
			return NULL;
		},
		{
			PyErr_SetString(Shm_Exception, "Internal failure");
			shm_pointer_release(thread, result_value);
			return NULL;
		});

	if (result_value != EMPTY_SHM)
	{
		// ShmAbstractBlock *data = LOCAL(result_value);
		// PyObject *rslt = abstract_block_to_object(data);
		// if (rslt == NULL)
		//    PyErr_Format(Shm_Exception, "Unknown type in ShmDictObject_subscript: %d\n", data->type);
		// // transient_commit(thread);

		return shm_pointer_to_object_consume(result_value);
	}
	else
	{
		_PyErr_SetKeyError(key);
		// transient_abort(thread);
		return NULL;
	}
}

// similar to ShmDict_subscript but with errors supressed
static PyObject *
ShmDict_GetItem(UnDictRef dict, PyObject *key)
{
	ShmUnDictKey dictkey = EMPTY_SHM_UNDICT_KEY;
	if (!object_to_dict_key(key, &dictkey))
	{
		PyErr_Format(PyExc_TypeError,
		            "dict key must be a string, not %.200s",
		            key->ob_type->tp_name);
		return NULL;
		}

	ShmPointer result_value = EMPTY_SHM;
	RETRY_LOOP(shm_undict_acq(thread, dict, &dictkey, &result_value),
		{ shmassert(result_value == EMPTY_SHM); },
		{
			shm_pointer_release(thread, result_value);
			return NULL;
		},
		{
			PyErr_SetString(Shm_Exception, "Internal failure in ShmDict_GetItem");
			shm_pointer_release(thread, result_value);
			return NULL;
		});

	if (result_value != EMPTY_SHM)
	{
		return shm_pointer_to_object_consume(result_value);
	}
	else
	{
		return NULL;
	}
}

int
ShmDictObject_DelItem(ShmDictObject *mp, ShmUnDictKey *dictkey)
{
	UnDictRef dict;
	if (!init_undict_ref(mp->data, &dict))
		return -1;
	RETRY_LOOP(shm_undict_set_empty(thread, dict, dictkey, NULL),
		{},
		{ return -1; },
		{
			PyErr_SetString(Shm_Exception, "Internal failure in ShmDictObject_DelItem");
			return -1;
		});
	return 0;
}

int
ShmDict_SetItemInternal(UnDictRef dict, ShmUnDictKey *dictkey, PyObject *value)
{
	ShmPointer newval = EMPTY_SHM;
	// we might be in transient transaction here so always have references acquired.
	if (value != NULL)
	{
		if (prepare_item_for_shm_container(value, &newval) < 0)
		{
			if (!PyErr_Occurred())
				PyErr_SetString(Shm_Exception, "Invalid pso object when assigning ShmDict's item");
			return -1;
		}
	}

	// can't really use shm_undict_consume_item here because that would mean releasing the reference at each cycle.
	RETRY_LOOP(shm_undict_set_item(thread, dict, dictkey, newval, NULL),
		{},
		{
			shm_pointer_release(thread, newval);
			return -1;
		},
		{
			PyErr_SetString(Shm_Exception, "Internal failure in ShmDictObject_SetItem");
			shm_pointer_release(thread, newval);
			return -1;
		});

	shm_pointer_release(thread, newval);
	return 0;
}

int
ShmDict_SetItem(UnDictRef dict, PyObject *key, PyObject *value)
{
	ShmUnDictKey dictkey = EMPTY_SHM_UNDICT_KEY;
	if (!object_to_dict_key(key, &dictkey))
	{
		PyErr_Format(PyExc_TypeError,
		             "dict key must be a string, not %.200s",
		             key->ob_type->tp_name);
		return -1;
	}
	return ShmDict_SetItemInternal(dict, &dictkey, value);
}

int
ShmDictObject_SetItem(ShmDictObject *mp, ShmUnDictKey *dictkey, PyObject *value)
{
	UnDictRef dict;
	if (!init_undict_ref(mp->data, &dict))
	{
		return -1;
	}

	return ShmDict_SetItemInternal(dict, dictkey, value);
}

static int
ShmDictObject_ass_sub(ShmDictObject *mp, PyObject *v, PyObject *w)
{
	ShmUnDictKey dictkey = EMPTY_SHM_UNDICT_KEY;
	if (!object_to_dict_key(v, &dictkey))
	{
		PyErr_Format(PyExc_TypeError,
		             "dict key must be a string, not %.200s",
		             v->ob_type->tp_name);
		return -1;
	}
	int rslt = -1;
	if (w == NULL)
		rslt = ShmDictObject_DelItem(mp, &dictkey);
	else
		rslt = ShmDictObject_SetItem(mp, &dictkey, w);

	return rslt;
}

static PyMappingMethods ShmDict_as_mapping = {
	(lenfunc)ShmDictObject_length, /*mp_length*/
	(binaryfunc)ShmDictObject_subscript, /*mp_subscript*/
	(objobjargproc)ShmDictObject_ass_sub, /*mp_ass_subscript*/
};

// see ShmList_iter for comments on locks
static PyObject *
ShmDictObject_iter(PyObject *obj)
{
	if (!PyObject_TypeCheck(obj, &ShmDict_Type))
	{
		PyErr_SetString(PyExc_ValueError, "dictionary must be ShmDict");
		return NULL;
	}

	UnDictRef dict;
	dict.shared = ((ShmDictObject *)obj)->data;
	dict.local = LOCAL(dict.shared);
	if (!dict.local)
	{
		PyErr_SetString(PyExc_ValueError, "no dict");
		return NULL;
	}

	int mode = thread->transaction_mode;
	shmassert_msg(mode != TRANSACTION_TRANSIENT, "Unfinished transient transaction in ShmDictObject_iter");
	shmassert_msg(mode != TRANSACTION_NONE, "Thread inactive in ShmDictObject_iter");
	bool is_transient = mode == TRANSACTION_IDLE;
	shmassert(is_transient || mode == TRANSACTION_PERSISTENT);
	if (is_transient == false)
	{
		RETRY_LOOP(transaction_lock_read(thread, &dict.local->lock, dict.shared, CONTAINER_UNORDERED_DICT, NULL),
			{},
			{ return NULL; },
			{
				PyErr_SetString(Shm_Exception, "Internal failure during ShmDictObject_iter");
				return NULL;
			});

		shm_cell_check_read_write_lock(thread, &dict.local->lock);
	}


	// ShmPointer shm_cell;
	// ShmCell *cell = shm_list_get_first(thread, list, &shm_cell);

	ShmDictIterObject *it;
	it = PyObject_New(ShmDictIterObject, &ShmDictIter_Type);
	if (it == NULL)
		return NULL;

	// it->persistent = true;
	// it->cell = shm_cell;
	it->dict = dict.local;
	shm_pointer_acq(thread, dict.shared);
	it->dict_shm = dict.shared;
	it->itemindex = 0;
	it->is_transient = is_transient;
	return (PyObject *)it;
}

#define MODE_KEYS    0
#define MODE_VALUES  1
#define MODE_ITEMS   2

// ShmUnDict and particulary shm_undict_get_bucket_at_index() still don't support counting uncommitted items.
static PyObject *
ShmDict_keys_values(ShmDictObject *self, int mode)
{
	UnDictRef dict = { .shared = self->data, .local = LOCAL(self->data) };
	if (dict.local == NULL)
		Py_RETURN_NONE;

	int committed_count = -1;
	// shm_undict_get_count does the same as
	// transaction_lock_read(thread, &dict.local->lock, dict.shared, CONTAINER_UNORDERED_DICT, NULL)
	RETRY_LOOP(_shm_undict_get_count(thread, dict, &committed_count, false),
		{},
		{
			transient_abort(thread);
			return NULL;
		},
		{
			transient_abort(thread);
			PyErr_SetString(Shm_Exception, "Internal failure during ShmDict_keys");
			return NULL;
		});

	shm_cell_check_read_lock(thread, &dict.local->lock);
	shmassert(committed_count != -1);

	PyObject *list = PyList_New(committed_count);
	if (list == NULL)
	{
		transient_abort(thread);
		return NULL;
	}
	int result_count = 0;
	int table_itemindex = 0;
	while (true)
	{
		// TODO: We should really check for preemption here
		ShmPointer value_shm = EMPTY_SHM;
		ShmPointer key_shm = EMPTY_SHM;
		bool eot = shm_undict_get_bucket_at_index(thread, dict.local, table_itemindex, &key_shm, &value_shm);
		table_itemindex++;
		if (eot || value_shm != EMPTY_SHM)
		{
			if (value_shm != EMPTY_SHM)
			{
				shmassert(!eot);
				PyObject *marshalled_obj = NULL;
				if (mode == MODE_KEYS)
				{
					shm_pointer_acq(thread, key_shm);
					marshalled_obj = shm_pointer_to_object_consume(key_shm);
				}
				else if (mode == MODE_VALUES)
				{
					shm_pointer_acq(thread, value_shm);
					marshalled_obj = shm_pointer_to_object_consume(value_shm);
				}
				else
					shmassert(false);

				if (marshalled_obj == NULL)
				{
					Py_DECREF(list);
					transient_abort(thread);
					return NULL;
				}
				int idx = result_count;
				result_count++;
				if (idx < committed_count)
					PyList_SET_ITEM(list, idx, marshalled_obj);
				else
					PyList_Append(list, marshalled_obj);
			}
			if (eot)
			{
				if (result_count < committed_count)
				{
					// crop the list to its actual size
					PyObject *indices = PySlice_New(PyLong_FromLong(result_count), NULL, NULL);
					if (indices == NULL)
					{
						Py_DECREF(list);
						return NULL;
					}
					if (PyObject_DelItem(list, indices) < 0)
					{
						Py_DECREF(list);
						return NULL;
					}
				}
				transient_commit(thread);
				return list;
			}
		}
	}
	shmassert(false); // unreachable
}

static PyObject *
ShmDict_keys(ShmDictObject *self)
{
	return ShmDict_keys_values(self, MODE_KEYS);
}

static PyObject *
ShmDict_values(ShmDictObject *self)
{
	return ShmDict_keys_values(self, MODE_VALUES);
}

static PyMethodDef ShmDict_methods[] = {
	{"keys",            (PyCFunction)ShmDict_keys,      METH_NOARGS },
	{"values",           (PyCFunction)ShmDict_values,     METH_NOARGS },
	{NULL, NULL} // sentinel
};


static PyTypeObject ShmDict_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pso.ShmDict",
	.tp_doc = "Shared mutable dictionary",
	.tp_basicsize = sizeof(ShmDictObject),
		// .tp_repr = (reprfunc) ShmDict_repr,
	.tp_dealloc = (destructor) ShmBase_dealloc,
	.tp_as_mapping = &ShmDict_as_mapping,
	.tp_getattro = PyObject_GenericGetAttr,
	.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_iter = ShmDictObject_iter,
	.tp_methods = ShmDict_methods,
	// .tp_members = ShmDict_members,
	.tp_init = (initproc) ShmDict_init,
	.tp_new = PyType_GenericNew,
};

static PyObject *
ShmDictIter_next(ShmDictIterObject *it)
{
	// XXX: just place debugger break here and enjoy random line numbers in test.pso.py: run_parent()
	if (thread->transaction_mode != TRANSACTION_PERSISTENT)
	{
		if (it->is_transient)
			shmassert(thread->transaction_mode == TRANSACTION_IDLE);
		else
		{
			PyErr_SetString(Shm_Exception, "Cannot use transacted iterator outside its transaction");
			return NULL;
		}
	}

	UnDictRef dict = { .local = it->dict, .shared = it->dict_shm };
	if (it->is_transient)
	{
		RETRY_LOOP(transaction_lock_read(thread, &dict.local->lock, dict.shared, CONTAINER_UNORDERED_DICT, NULL),
			{},
			{
				transient_abort(thread);
				return NULL;
			},
			{
				transient_abort(thread);
				PyErr_SetString(Shm_Exception, "Internal failure during ShmDictIter_next");
				return NULL;
			});
	}
	shm_cell_check_read_lock(thread, &dict.local->lock);

	while (true)
	{
		// TODO: We should really check for preemption here
		ShmPointer value = EMPTY_SHM;
		bool eot = shm_undict_get_bucket_at_index(thread, dict.local, it->itemindex, NULL, &value);
		it->itemindex++;
		if (eot)
		{
			transient_commit(thread);
			return NULL;
		}
		if (value != EMPTY_SHM)
		{
			PyObject *result_obj = shm_pointer_to_object_consume(value);
			transient_commit(thread);
			return result_obj;
		}
	}
	shmassert(false); // unreachable
}

static PyTypeObject ShmDictIter_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pso.ShmDictIter",
	.tp_basicsize = sizeof(ShmDictIterObject),
	.tp_itemsize = 0,
	/* methods */
	.tp_dealloc = (destructor)ShmBase_dealloc,
	.tp_getattro = PyObject_GenericGetAttr,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_iter = PyObject_SelfIter,
	.tp_iternext = (iternextfunc)ShmDictIter_next,
	// .tp_methods = listiter_methods,
};

// //////////////////////
//       ShmObject
// //////////////////////

_Py_IDENTIFIER(__module__);
_Py_IDENTIFIER(__qualname__);
_Py_IDENTIFIER(__name__);

//def class_fullname(o):
//  klass = o.__class__
//  module = klass.__module__
//  if module == 'builtins':
//      return klass.__qualname__ # avoid outputs like 'builtins.str'
//  return module + '.' + klass.__qualname__
// Also see Modules/_pickle.c: save_global() and whichmodule()

// static int
// ShmObjectObject_init(ShmObjectObject *self, PyObject *args, PyObject *kwds)
PyObject *
ShmObjectObject_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
	ShmObjectObject *self = (ShmObjectObject *)PyType_GenericNew(type, args, kwds);
	if (self == NULL)
		return NULL;
	self->data = EMPTY_SHM;
	if (!check_thread_inited())
		return NULL;
	ShmUnDict *dict = new_shm_undict(thread, &self->data);
	shmassert(dict->type == SHM_TYPE_UNDICT);
	dict->type = SHM_TYPE_OBJECT;

	// PyObject *myclass = NULL;
	// if (_PyObject_LookupAttrId((PyObject*)self, &PyId___class__, &myclass) < 0)
	//     return -1;

	// PyTypeObject *myclass = Py_TYPE(self);
	// shmassert(myclass);
	PyTypeObject *myclass = type;
	PyObject *module = NULL;
	if (_PyObject_LookupAttrId((PyObject*)myclass, &PyId___module__, &module) < 0)
	{
		PyErr_SetString(Shm_Exception, "ShmObject's class has no __module__ attribute.");
		return NULL;
	}

	PyObject *qualname = NULL;
	if (_PyObject_LookupAttrId((PyObject*)myclass, &PyId___qualname__, &qualname) < 0)
		goto error; // Error already set by _PyObject_LookupAttrId
	qualname = _PyObject_GetAttrId((PyObject*)myclass, &PyId___name__);
	if (qualname == NULL) // Error already set by _PyObject_GetAttrId
		goto error;

	Py_ssize_t qualname_size = -1;
	const Py_UCS1 *qualname_utf8 = (const Py_UCS1 *)PyUnicode_AsUTF8AndSize(qualname, &qualname_size);
	if (qualname_utf8 == NULL)
		goto error;
	Py_ssize_t module_size = -1;
	const Py_UCS1 *module_utf8 = (const Py_UCS1 *)PyUnicode_AsUTF8AndSize(module, &module_size);
	if (module_utf8 == NULL)
		goto error;

	ShmPointer str_shm = shm_ref_unicode_new(thread, NULL, qualname_size + 1 + module_size + 1);
	RefUnicode str = shm_ref_unicode_get(str_shm);
	UCS4_from_ASCII(CAST_VL(str.data), module_utf8, module_size);
	str.data[module_size] = '\t';
	UCS4_from_ASCII(CAST_VL(&str.data[module_size + 1]), qualname_utf8, qualname_size);
	str.data[qualname_size + 1 + module_size] = 0;
	dict->class_name = str_shm;

	return (PyObject *)self;
error:
	Py_XDECREF(module);
	Py_XDECREF(qualname);
	return NULL;
}

PyObject *
ShmObjectObject_getattro(PyObject *obj, PyObject *name)
{
	// mostly a copy of Objects/object.c: _PyObject_GenericGetAttrWithDict
	PyTypeObject *tp = Py_TYPE(obj);
	PyObject *descr = NULL;
	PyObject *res = NULL;
	descrgetfunc f;
	ShmObjectObject *self = (ShmObjectObject *)obj;

	if (!PyUnicode_Check(name)){
		PyErr_Format(PyExc_TypeError,
		            "attribute name must be string, not '%.200s'",
		             name->ob_type->tp_name);
		return NULL;
	}
	Py_INCREF(name);

	if (tp->tp_dict == NULL) {
		if (PyType_Ready(tp) < 0)
			goto done;
	}

	descr = _PyType_Lookup(tp, name);

	f = NULL;
	if (descr != NULL) {
		Py_INCREF(descr);
		f = descr->ob_type->tp_descr_get;
		if (f != NULL && PyDescr_IsData(descr)) {
			res = f(descr, obj, (PyObject *)obj->ob_type);
			// if (res == NULL && suppress &&
			//         PyErr_ExceptionMatches(PyExc_AttributeError)) {
			//     PyErr_Clear();
			// }
			goto done;
		}
	}

	UnDictRef dict = { .shared = self->data, .local = LOCAL(self->data) };
	if (dict.local)
	{
		res = ShmDict_GetItem(dict, name);
		if (res != NULL) {
			Py_INCREF(res);
			goto done;
		}
		else if (PyErr_Occurred())
			goto done;
	}

	if (f != NULL) {
		res = f(descr, obj, (PyObject *)Py_TYPE(obj));
		// if (res == NULL && suppress &&
		//         PyErr_ExceptionMatches(PyExc_AttributeError)) {
		//     PyErr_Clear();
		// }
		goto done;
	}

	if (descr != NULL) {
		res = descr;
		descr = NULL;
		goto done;
	}

	PyErr_Format(PyExc_AttributeError,
	             "'%.50s' object has no attribute '%U'",
	             tp->tp_name, name);
done:
	Py_XDECREF(descr);
	Py_DECREF(name);
	return res;
}

int
ShmObjectObject_setattro(PyObject *obj, PyObject *name, PyObject *value)
{
	// mostly copy of _PyObject_GenericSetAttrWithDict
	PyTypeObject *tp = Py_TYPE(obj);
	PyObject *descr;
	descrsetfunc f;
	int res = -1;
	ShmObjectObject *self = (ShmObjectObject *)obj;

	if (!PyUnicode_Check(name)){
		PyErr_Format(PyExc_TypeError,
		             "attribute name must be string, not '%.200s'",
		             name->ob_type->tp_name);
	return -1;
	}

	if (tp->tp_dict == NULL && PyType_Ready(tp) < 0)
	return -1;

	Py_INCREF(name);

	descr = _PyType_Lookup(tp, name);

	if (descr != NULL) {
	Py_INCREF(descr);
		f = descr->ob_type->tp_descr_set;
		if (f != NULL) {
				res = f(descr, obj, value);
			goto done;
		}
	}

	UnDictRef dict = { .shared = self->data, .local = LOCAL(self->data) };
	if (dict.local)
	{
		res = ShmDict_SetItem(dict, name, value);
		goto done;
	}
	if (res < 0 && PyErr_ExceptionMatches(PyExc_KeyError))
		PyErr_SetObject(PyExc_AttributeError, name);

done:
	Py_XDECREF(descr);
	Py_DECREF(name);
	return res;
}

PyObject *
ShmObjectObject_GetDict(PyObject *obj, void *context)
{
	ShmObjectObject *self = (ShmObjectObject *)obj;
	if (SBOOL(self->data) == false)
		Py_RETURN_NONE;
	ShmDictObject *dict = PyObject_New(ShmDictObject, &ShmDict_Type);
	dict->data = self->data;
	return (PyObject *)dict;
}

int
ShmObjectObject_SetDict(PyObject *obj, PyObject *value, void *context)
{
	PyErr_SetString(PyExc_AttributeError,
	                "Changing __dict__ for ShmObject is not supported.");
	return -1;
}

// static PyMethodDef ShmObjectObject_methods[] = {
// 	{NULL, NULL} // sentinel
// };

// static PyMemberDef ShmObjectObject_members[] = {
// 	{NULL, NULL} // sentinel
// };

static PyGetSetDef ShmObjectObject_getset[] = {
	{"__dict__", ShmObjectObject_GetDict, ShmObjectObject_SetDict},
	{NULL, NULL} // sentinel
};

static PyTypeObject ShmObject_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pso.ShmObject",
	.tp_doc = "Custom object",
	.tp_basicsize = sizeof(ShmObjectObject),
	.tp_dealloc = (destructor) ShmBase_dealloc,
	.tp_getattro = ShmObjectObject_getattro,
	.tp_setattro = ShmObjectObject_setattro,
	.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	// .tp_methods = ShmObjectObject_methods,
	// .tp_members = ShmObjectObject_members,
	.tp_getset = ShmObjectObject_getset,
	// .tp_init = (initproc) ShmObjectObject_init,
	.tp_new = ShmObjectObject_new,
};

// ShmPromise
static int
ShmPromiseObject_init(ShmObjectObject *self, PyObject *args, PyObject *kwds)
{
	self->data = EMPTY_SHM;
	if (!check_thread_inited())
		return -1;
	ShmPromise *promise = new_shm_promise(thread, &self->data);
	shmassert(promise->type == SHM_TYPE_PROMISE);
	return 0;
}

static PyObject*
ShmPromiseObject_wait(ShmListObject *self, PyObject *value)
{
	ShmPromise *promise = LOCAL(self->data);
	shm_promise_wait(thread, promise);
	Py_RETURN_NONE;
}

static PyObject*
ShmPromiseObject_signal(ShmListObject *self, PyObject *value)
{
	if (value != Py_True && value != Py_False)
	{
		PyErr_SetString(PyExc_TypeError, "ShmPromiseObject.signal() requires boolean argument");
		return NULL;
	}
	ShmPromise *promise = LOCAL(self->data);

	shm_promise_signal(thread, promise, self->data, value == Py_True ? PROMISE_STATE_FULFILLED : PROMISE_STATE_REJECTED, NONE_SHM);

	Py_RETURN_NONE;
}

static PyMethodDef ShmPromise_methods[] = {
	// {
	//     "new_value", (PyCFunction)_shm_list_new_value, METH_O,
	//     "Returns new value for use in the list."
	// },
	{
		"wait", (PyCFunction)ShmPromiseObject_wait, METH_O,
		"Wait for the promise to be resolved"
	},
	{
		"signal", (PyCFunction)ShmPromiseObject_signal, METH_O,
		"Fulfill or reject the promise"
	},
	{NULL, NULL} // sentinel
};

static PyTypeObject ShmPromise_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pso.ShmPromise",
	.tp_doc = "Simple one time event for multiple waiters",
	.tp_basicsize = sizeof(ShmPromiseObject),
	.tp_dealloc = (destructor) ShmBase_dealloc,
	.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_methods = ShmPromise_methods,
	// .tp_members = ShmObjectObject_members,
	.tp_init = (initproc) ShmPromiseObject_init,
	.tp_new = PyType_GenericNew,
};

// Method definition object for this extension, these argumens mean:
// ml_name: The name of the method
// ml_meth: Function pointer to the method implementation
// ml_flags: Flags indicating special features of this method, such as
//          accepting arguments, accepting keyword arguments, being a
//          class method, or being a static method of a class.
// ml_doc:  Contents of this method's docstring
static PyMethodDef pso_methods[] = {
	{
		"init", init, METH_NOARGS,
			"Initialize superblock and create coordinator structures"
	},
	{
		"connect", connect_to_coordinator, METH_VARARGS,
			"Connect to existing coordinator and use its superblock. Single argument is the name of superblock shared memory object"
	},
	{
		"transient_start", transient_start, METH_NOARGS,
		"Engage the \"commit after every operation\" mode"
	},
	{
		"transient_end", transient_end, METH_NOARGS,
		"Disengage the \"commit after every operation\" mode"
	},
	{
		// We cannot actually commit the transient transaction manually,
		// because commit-rollback happens automatically after each operator.
		// I still tend to write transient_commit though.
		"transient_commit", transient_end, METH_NOARGS,
		"Disengage the \"commit after every operation\" mode (same as transient_end)"
	},
	{
		"transient_active", transient_active, METH_NOARGS,
		"True when at least transient transaction mode is active i.e. also True when in persistent transaction"
	},
	{
		"transaction_start", transaction_start, METH_NOARGS,
		"Start transaction so you can operate the shared memory data"
	},
	{
		"transaction_commit", transaction_commit, METH_NOARGS,
		"Save transaction changes"
	},
	{
		"transaction_rollback", transaction_rollback, METH_NOARGS,
		"Cancel transaction changes"
	},
	{
		"transaction_active", transaction_active, METH_NOARGS,
		"True when in transaction"
	},

	{
		"transaction_rollback_retaining", transaction_rollback_retaining, METH_NOARGS,
		"Cancel transaction changes, but keep the current transaction mode active"
	},
	{
		"root", get_root, METH_NOARGS,
		"Get a superblock-bound shared container"
	},
	{
		"set_random_flinch", set_random_flinch, METH_O,
		"Enable random transaction retries for debugging purpose"
	},
	{
		"set_debug_reclaimer", set_debug_reclaimer, METH_O,
		"Enable reclaimer's statistic print for debugging purpose"
	},
	{
		"set_debug_print", set_debug_print, METH_O,
		"Enable pso module debug printf-s"
	},
	{
		"print_thread_counters", print_thread_counters, METH_NOARGS,
		"Print transaction profiling data"
	},
	{
		"global_debug_stop_on_contention", global_debug_stop_on_contention, METH_NOARGS,
		"Pause all processes when contention for resource is encountered"
	},
	{
		"object_debug_stop_on_contention", object_debug_stop_on_contention, METH_O,
		"Pause all processes when contention for resource is encountered"
	},
	{
		"get_contention_count", get_contention_count, METH_O,
		"Get contention counter for specific container (read, write)"
	},
	{
		"try_object_to_shm_value", try_object_to_shm_value, METH_O,
		"Try to convert python simple value in to shared memory value object. Returns None if object is not a simple value"
	},
	{NULL, NULL, 0, NULL} // sentinel
};

// Module definition
// The arguments of this structure tell Python what to call your extension,
// what it's methods are and where to look for it's method definitions
static struct PyModuleDef pso_definition = {
	PyModuleDef_HEAD_INIT,
	"pso",
	"Objects in shared memory with transaction support.",
	-1,
	pso_methods
};

#define return_null_on_failure(rslt)  if (rslt < 0) \
	return NULL;

// Module initialization
// Python calls this function when importing your extension. It is important
// that this function is named PyInit_[[your_module_name]] exactly, and matches
// the name keyword argument in setup.py's setup() call.
PyMODINIT_FUNC PyInit__pso(void) {
	PyObject *m;
	return_null_on_failure(PyType_Ready(&ShmObject_Type));
	return_null_on_failure(PyType_Ready(&ShmValue_Type));
	return_null_on_failure(PyType_Ready(&ShmTuple_Type));
	return_null_on_failure(PyType_Ready(&ShmTupleIter_Type));
	return_null_on_failure(PyType_Ready(&ShmList_Type));
	return_null_on_failure(PyType_Ready(&ShmListIter_Type));
	return_null_on_failure(PyType_Ready(&ShmDict_Type));
	return_null_on_failure(PyType_Ready(&ShmDictIter_Type));
	return_null_on_failure(PyType_Ready(&ShmPromise_Type));

	m = PyModule_Create(&pso_definition);
	if (m == NULL)
		return NULL;
	
	// shall I do it through static PyTypeObject definition and PyType_Ready?
	Shm_Exception = PyErr_NewException("pso.ShmException", NULL, NULL);
	Py_INCREF(Shm_Exception);
	return_null_on_failure(PyModule_AddObject(m, "ShmException", (PyObject *)Shm_Exception));

	Shm_Abort = PyErr_NewException("pso.ShmAbort", NULL, NULL);
	Py_INCREF(Shm_Abort);
	return_null_on_failure(PyModule_AddObject(m, "ShmAbort", (PyObject *)Shm_Abort));

	Py_INCREF(&ShmObject_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmObject", (PyObject *)&ShmObject_Type));
	Py_INCREF(&ShmValue_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmValue", (PyObject *)&ShmValue_Type));
	Py_INCREF(&ShmTuple_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmTuple", (PyObject *)&ShmTuple_Type));
	Py_INCREF(&ShmTupleIter_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmTupleIter", (PyObject *)&ShmTupleIter_Type));
	Py_INCREF(&ShmList_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmList", (PyObject *)&ShmList_Type));
	Py_INCREF(&ShmListIter_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmListIter", (PyObject *)&ShmListIter_Type));
	Py_INCREF(&ShmDict_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmDict", (PyObject *)&ShmDict_Type));
	Py_INCREF(&ShmDictIter_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmDictIter", (PyObject *)&ShmDictIter_Type));
	Py_INCREF(&ShmPromise_Type);
	return_null_on_failure(PyModule_AddObject(m, "ShmPromise", (PyObject *)&ShmPromise_Type));

	return m;
}

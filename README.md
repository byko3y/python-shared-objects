# python-shared-objects

## Rationale

For a long time Python was known as a strictly one-threaded language, with threads playing role of I/O threading but not the computation threading. [PEP 554 -- Multiple Interpreters in the Stdlib](https://www.python.org/dev/peps/pep-0554/) and https://github.com/ericsnowcurrently/multi-core-python gives some hope to have some kind of multithreading, but currently there are a lot of bugs and global mutable structures in the interpreter, so a lot of work needs to be done until we finally see the multiple interpreters in work. Erlang, being similar to Python in its architecture of highly isolated processes, managed to become a highly concurrent runtime, which the Python is yet to become. While the progress is being made I'd be glad to contribute by creating a project which allows to handle mutable objects with multiple interpreters within and across the process boundaries.<br/>
The final goal is to allow creation of multiprocess servers with minimal reliance on DB/memcached for coordination, responsive GUI with logic separated from GUI rendering while having access to both with python code. Computer's work is much cheaper than a programmer's work, so low performance of such a dynamically typed language as Python can be compensated by CPU cores, while usually you have a hard limit on amount of programmers in a project.<br/>

## Known alternatives

* **multiprocessing** itself - allows one process to use another process object transparently. The module is know to have many bugs and the transparent access is in fact a serialization of accessing function sent via socket/pipe, which may become incredibly slow and glitchy even for a level of some mediocre python project
* **multiprocessing.shared_memory** - very primitive implementation of OS-level shared memory
* **multiprocessing.sharedctypes** - uses multiprocessing.shared_memory to implement a simple memory manager which is used for sharing of ctypes' structures
* https://sourceforge.net/projects/poshmodule/ - really nice try to implement effective object sharing, but native python objects have inherent resistance to sharing, thus sharing objects is a dead end - you need an object born for being shared. Also the implementation is based on fork and absolute addressing, thus inherently flawed.
* https://github.com/dRoje/pipe-proxy - small modification a regular multiprocessing object proxy.
* https://mpi4py.readthedocs.io/en/stable/tutorial.html - pickle-based messaging and raw sending of data between processes. Either low level or low performance tool. Could be utilized for sophisticated message passing between processes.
* https://arrow.apache.org/docs/python/plasma.html - by far the closest attempt to complete the same task. So far the most significant shortcoming is immutability of most of the objects. This also implies the data has no methods - it's pretty much dead for a living program. Despite the fact it provides great interoperability between languages, the living program in any language in fact studies the dead parts of the data body. Yes, it can load/unload a part of data, which is usefull for big data processing - not much useful for a mutable not-so-big-data.

## Goals

Make perfectly sharable objects that look very similar to native python objects (like list, dict, object with methods and descriptors) whose data and methods reside in shared memory, thus being readable and modifiable from multiple interpreters and processes without a need for pickle/unpickle or complicated interaction with IPC. Shared objects won't have access to regular objects (because you can't see those outside the process), thus the only way of communication is from a regular object to a shared object.<br/>

Main facilities to implement, in order of importance:
* Message passing for initializing and coordinating the interpreters. Could be implemented with the same shared objects, but we just need to have some solid ground during development
* Basic native-looking objects that use shared memory to store their data (int, string, tuple, list, dict, object). Unfortunately, CPython's implementation of those objects is inherently single-threaded, but probably can be partially reused with the help of object-level locks, until a real lock-free implementation is made. It's preferable to store basic values like int/float/bool as in-place data instead of a separate object. Methods can be stored as a function text, which can be compiled and executed in the context of calling thread.
* Transaction mechanism for lock-free access to mutable shared objects. Deadlocks and racing conditions are unacceptable complications for such a high-level language as Python. That's why lock and modification contention shall be resolved by a well-debugged algorithm, while user will just provide a pure function (without implicit side effects) to be executed as atomic transaction, which briefly locks every modified object and applies the chances, verifying the read-only values remain the same. We can help those read-only values to remain the same by using more immutable values. Mutable objects might also be locked for the whole duration of transaction, but the problem of deadlocks isu resolved by using all-or-none locking approach, failing the transaction in case of contention.<br/>
Snapshots (RDBMS-style MVCC) are not viable due to intense garbage generation with every modification of every shared object. So only actually accessed objects are recorded.<br/>
Unsynchronized access is still an option. Obviously, shared objects cannot be too complex, otherwise it will be really hard to implement the transaction feature.
* Memory manager for managing allocation of small objects like a string or an element of list. Saves a lot of resources by using one piece of shared memory for multiple parts of data.<br/>
* Parallel garbage collector. Python generational reference-count-based collector is a stop-the-world kind of collector. Initial implementations might lack the garbage collector overall, managing the references and release of unused objects by virtue of reference counting with atomic increment/decrement.

## Prototyping results

After implementation of a prototype (unpublished), the key problem of the CPython was quickly (re)discovered: reference counting requires intensive writing and locking of shared memory even for read-only access and garbage collection. Most moderns CPUs show a huge loss of performance when writing to a shared location due to caching mechanisms: every modification on shared location later causes cache miss for every new core reading this location and even surrounding ones. This performance drop can easily overshadow the gain of multiprocessing. There are sereval ways to resolve the problem:
1. Abandon the reference counting, entrusting the GC with all the memory management. That's how PyPy, Java, .NET, V8, and others work. Nice route, but I really think it can be done without any GC;
2. Keep the reference counting, but only modify the counter during actual modification of data. Tracking the read-only references becomes a pain in the ass.

I had many ideas for the second route, I'll try to not mention failed ones here. Of course, stop-the-world deallocation mechanism could be used in both cases to ensure a consistent state of data during release. As well it might destroy the whole idea of non-interruptable python workers executing a code written in different languages e.g. Python, C, C++ - fairly common python program.

Now, for a perfectly concurrent memory release, the trickiest part of tracking read-only references is the moment when a thread just read the reference value into register, but haven't stored it anywhere.  
```
  shared data: shared_cell -> shared_data  
   local data: thread_cell  
```
Here a thread wants to access the shared_cell's value (shared_data) by placing a reference to shared_data into thread_cell. The thread algorythm (x86 pseudocode):  
```
  eax := [shared_cell]  
  [thread_cell] := eax  
```
Suppose this thread switches the context right in between those two commands. Suppose another thread (releasing) at this time clears the reference and decides to free the shared_data which got zero refcount now.  
```
  shared data: shared_cell -> null
```
How can possibly another thread track the another's thread reference in eax to retain from releasing an object in use? For this reason, if we want to be able to make the release-or-postpone decision at this moment, the referencing thread has to inform the releasing thread about the new reference. For example, V8 concurrent collector does that via write barrier: https://v8.dev/blog/concurrent-marking

However, unlike V8, we don't know the shared_data address until we read it, so we either need to block any release operations during reference read-traversal or block any read-traversal during memory release. In heavily concurrent environment blocking the releaser-thread could cause it to never complete the operation. On the other hand, single threaded V8 has short stop-the-world moments when the releaser can safely perform the actions. Considering the eclectic nature of python's world we can barely afford stop-the-world: single threaded python makes stop at particular spots, while concurrent interpreters have to wait for each other to stop at the spot, which can take an excessively long period of time. 

Therefore, we can't let any of those threads to wait. Possible solution could be an asynchrounous freeing queue, which releases the shared_data when the referencing thread is clearly not referencing shared_data and not trying to read the reference to shared_data. And we might just find this moments, thanks to transaction mechanism: **when the shared_data is not referenced by any shared object (refcount = 0); and every transaction, which could reference the shared_data when it was visible in shared memory, have finished**. Not only it is a moment but also an infinite period of time starting when there are no active transactions exist which were started with shared_data being visible. We can check this condition by flagging the running transactions and waiting until the thread clears the flag on commit/retry/failure.

Pros: transaction becoming a black box.  
Cons: long running transaction will disable any memory release for duration of transaction.

The transaction might actually stay inifinite amount of time with address of shared_data in register. Possible solution? Avoid long running transactions - would be too general and impossible for an inxperienced python developer. We could store the local references in some shared storage (dedicated to one thread for locality), so when an external thread  detects a non-progressing thread (e.g. IO wait), it could examine the references. Of course, this means the mechanisms of local reference acquisition/release should obey the lock and the mechanism prohibits the locking when acquisition/release is already in progress. This also implies that acquisition/release shall be performed by short known-to-never-block functions.
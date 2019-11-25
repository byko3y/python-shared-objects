# python-shared-objects

## Rationale

For a long time Python was known as a strictly one-threaded language, with threads playing role of I/O threading but not the computation threading. [PEP 554 -- Multiple Interpreters in the Stdlib](https://www.python.org/dev/peps/pep-0554/) and https://github.com/ericsnowcurrently/multi-core-python gives some hope to have some kind of multithreading, but currently there are a lot of bugs and global mutable structures in the interpreter, so a lot of work needs to be done until we finally see the multiple interpreters in work. Erlang, being similar to Python in its architecture of highly isolated processes, managed to become a highly concurrent runtime, which the Python is yet to become. While the progress is being made I'd be glad to contribute by creating a project which allows to handle mutable objects with multiple interpreters within and across the process boundaries.<br/>
The final goal is to allow creation of multiprocess servers with minimal reliance on DB/memcached for coordination, responsive GUI with logic separated from GUI rendering while having access to both with python code. Computer's work is much cheaper than a programmer's work, so low performance of such a dynamically typed language as Python can be compensated by CPU cores, while usually you have a hard limit on amount of programmers in a project.<br/>

## Known alternatives

* PyPy STM - good attempt to make the language multithreaded by serializing the parallel thread. Nice attempt, but I believe it's a dead end because they try to bring concurrency into too many places, while lacking the most important safe concurrency in manipulating the shared data.
* **multiprocessing** itself - allows one process to use another process object transparently. The module is know to have many bugs and the transparent access is in fact a serialization of accessing function sent via socket/pipe, which may become incredibly slow and glitchy even for a level of some mediocre python project
* **multiprocessing.shared_memory** - very primitive implementation of OS-level shared memory
* **multiprocessing.sharedctypes** - uses multiprocessing.shared_memory to implement a simple memory manager which is used for sharing of ctypes' structures
* https://sourceforge.net/projects/poshmodule/ - really nice try to implement effective object sharing, but native python objects have inherent resistance to sharing, thus sharing objects is a dead end - you need an object born for being shared. Also the implementation is based on fork and absolute addressing, thus inherently flawed.
* https://github.com/dRoje/pipe-proxy - small modification a regular multiprocessing object proxy.
* https://mpi4py.readthedocs.io/en/stable/tutorial.html - pickle-based messaging and raw sending of data between processes. Either low level or low performance tool. Could be utilized for sophisticated message passing between processes.
* https://arrow.apache.org/docs/python/plasma.html - by far the closest attempt to complete the same task. So far the most significant shortcoming is immutability of most of the objects. This also implies the data has no methods - it's pretty much dead for a living program. Despite the fact it provides great interoperability between languages, the living program in any language in fact studies the dead parts of the data body. Yes, it can load/unload a part of data, which is usefull for big data processing - not much useful for a mutable not-so-big-data.
* https://en.wikipedia.org/wiki/Zope_Object_Database - pickle-based client-server.
* lmdb - shared memory storage with MVCC that makes a smart use of unified buffer cache of most modern OS-es to protect the shared storage: mmaped read-only data is read with zero-copying, while writes are performed with pwrite/writev in special routines, that's why no random writes would corrupt the persistent storage. Disadvantages: serialized-only writes, single key-value model of storage (hence no dict, no list, no object, etc), it really loves to eat memory for its large history of append/removes.

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

Therefore, we can't let any of those threads to wait. Possible solution could be an asynchrounous freeing queue, which releases the shared_data when the referencing thread is clearly not referencing shared_data and not trying to read the reference to shared_data. And we might just find this moments, thanks to transaction mechanism: **when the shared_data is not referenced by any shared object (refcount = 0); and every transaction, which could reference the shared_data when it was visible in shared memory, have finished**. We can call it a **contemporary transaction** relative to the data being released. Not only it is a moment but also an infinite period of time starting when there are no active transactions exist which were started with shared_data being visible. We can check this condition by flagging the running transactions and waiting until the thread clears the flag on commit/retry/failure.

Pros: transaction becoming a black box.  
Cons: long running transaction will disable any memory release for duration of transaction.

The transaction might actually stay inifinite amount of time with address of shared_data in register. Possible solution? Avoid long running transactions - would be too general and impossible for an inxperienced python developer. We could store the local references in some shared storage (dedicated to one thread for locality), so when an external thread  detects a non-progressing thread (e.g. IO wait), it could examine the references. Of course, this means the mechanisms of local reference acquisition/release should obey the lock and the mechanism prohibits the locking when acquisition/release is already in progress. This also implies that acquisition/release shall be performed by short known-to-never-block functions.


# Transactional memory

Explicit locks are tricky to write and can cause deadlocks or performance issues. Lock-free algorythms are even more tricky to write, although ready-made well-debugged lock-free functions are great for some tasks (not nearly always they offer any advantage though), but lock-free algorithms have an inherent flaw - they are not composable. For example, a value can be appended to a list or removed from list in a lock-free fashion, but we cannot easily append the value with condition of list length being less than, say, 10. That's where comes into play the idea of transactional memory: try to perform changes one by one, then commit with validation of consistency. However, lock-free operations might still come in handy for some simple and fundamental tasks.

Naive implementations of transactional memory suffer from multiple problems and thus haven't gained a significant popularity. Those problems were known as early as 1992 ( [Performance issues in non-blocking synchronization on shared-memory multiprocessors](https://dl.acm.org/citation.cfm?id=135446) ), yet still there's no generally acccepted solution. I have a belief those problems can be solved - it's just not enough effort being put into the problem.  
So, the problems are:
* Performance in mid-high contention scenario. That's what killed GHC's STM. Performance can drop hunderd times (compared to a single thread) with intense abort/retry cycles doing no work and just hindering each other, making it pointless to implement those algorithms concurrently in Haskell. That's why concurrent modifications of shared memory should be limited as much as possible by sequential execution instead of endless retries;
* Many implementations of transactional memory are blocking, although the whole idea of transactional memory is to make a non-blocking concurrent operation which could allow to perform another non-blocking operation at the same time i.e. compose function;
* OS-es are still in early stages of SMP/NUMA/multiprocessor support. For example, the use of basic spinlocks in linux kernel kills the performance of application trying to use kernel structures in heavily concurrent fashion: [Spin lock killed the performance star](https://dx.doi.org/10.1109/SCCC.2015.7416588). Modern SMP OS can demonstrate a good performance for tasks with minimal sharing of data, thus promoting the popularity of shared-none models, Go language and JavaScript workers being recent examples. For this reason the most performant web-servers are single threaded event-driven ones e.g. nginx. Thus we should avoid high concurrency in kernel primitives;
* Absence of efficient mechanism for cross-thread/cross-process synchronous call in OS is another culprit. For that reason many runtimes employ a green threads model (haskell, erlang, go) which can make a cross-thread call cost 100-200 CPU cycles instead of 5000-20000 cycles with a regular model of "wake another thread up and go to sleep". The reason for that is because kernel awakes kernel on another CPU core, schedules the awaken thread onto it, then removes the calling thread from schedule of the first kernel's core. Not only it destroys the CPU cache, but it also requires quite expensive processing in scheduler. Pinning the threads to a core can improve the performance a lot, but still the cost of context switch remains. Python can perform significant amount of work in 5-20k cycles, like many dozens of low-level function calls, thus synchronous function calls should be avoided when possible, limiting the possibilities for coordination between transactions in different threads/processes;
* Prioritization and scheduling. As already mentioned, concurrent unrestricted conflicting modifications hurt the overall performance. Thus conflicting operations should be performed sequentially. There are no established algorithms for efficient scheduling of transactions with complex dependencies. Existing highly concurrent locking algorithms and existing schedulers are not suitable for systems with non-linear dependencies of executing units and intense volatility of those dependencies. Without priority and schedule, "spinlocking" threads would succeed faster and may completely stall slower transactions.

So we need to minimize writing to shared locations and don't read the frequently modified locations; we need to enforce some order on modifications, at the same time not forcing the executing unit to be locked onto thread; locking/scheduling routines should not use OS primitives in concurrent manner and should not call other threads/processes via OS, unless waiting time is so long it can be exchanged for the serveral thousand cycles in OS. Implicit consequence of these restrictions is that we can't easily transfer (e.g. preempt) the lock/waiting status from one thread to another - those need to be transfered in a two step procedure "lock given - lock accepted".

Keeping multiple versions of data could become handy, but the problem of data consistency might overweight the advantage of taking snapshots, while the complexity and overhead of multiversioning is significant.

Now, the idea of the new transactional memory looks like this: the most desirable data type is a single immutable piece of memory, while mutable cells contain a single pointer to the immutable value, thus greatly simplifying the amount of changes committed into shared location; threads can read the mutable cells without locking (and without consistency guarantee); threads also can lock the cells in a non-blocking manner (strangely enough), detecting the conflict early and executing the conflicting transactions sequentially with a guaranteed right to execute a single transaction after every other conflicting transaction succeeded.

Importantly, the whole system must be stable enough for one or even several executing units to crash or hang, which becomes even more probable as the amount of those units increase. Thus the transactional routines and regular python-C functions, as well as data should be separated from each other so memory corruption won't propogate into shared data. Read-only view of value might be acceptable, but writing to the shared memory should be stricktly limited.

Basis for fair scheduling is a low-overhead monotonic timer, like clock_gettime(CLOCK_MONOTONIC_COARSE...), GetTickCount, or maybe some dedicated timer thread. This way we can detect the starving thread by difference between its last success time and last success of the current thread/transaction. If the thread is starving and some conflicting transaction is in progress, than the transaction should be aborted and starving transaction receives the lock via "given-accepted" two step mechanism. Of course, amount of preemption and changes of order in general should be minimized.

Some update: after analyzing the scenario of complex contentions on several lock, I realized that good queueing with minimal amount of unnecessary locking can be implemented using separate queues for every lock. Every thread can wait for a single lock, so the thread descriptor can be used as a queue item. It's important to ensure the thread belongs to a single queue and no thread from other queue waits for him.  
This model might leads to a situation when a thread requiring many locks will have to queue itself many times, leading to a significant delay for threads waiting for the first locks that the heavy-locking thread have acquired. BTW, because the heavy-locking thread started earlier, it will not be preempted. For this reason it might be viable to give a heavy-locking thread preference in queue position.  
Preemption can be implemented either by a regular queue with a small modification of a preemption flag in the thread descriptor, or by avoiding the queue using a separate preempting_thread pointer in the thread. Of course, the latter model makes it hard to assign two threads as preempting simultaneously.  
Outline of data structures for this model:

**mutable_cell: struct**  
&nbsp;&nbsp;&nbsp;&nbsp;**{**  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**lock:** *pointer to the thread_descriptor holding the lock to the cell, or null when the cell is unlocked.*;  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**lock_queue:** *single-linked list of threads that wait for the lock*;  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**value:** *pointer to the value or current value itself for small things like boolean or integer*;  
(maybe) **new_value:** *similar to "value", but hold the uncommitted data*  
&nbsp;&nbsp;&nbsp;&nbsp;**};**

**thread_descriptor:** *semi-private thread structure*  
&nbsp;&nbsp;&nbsp;&nbsp;**struct {**  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**first_attempt_after_success_tick:** *when the first time the thread attempted some transaction and haven't succeeded yet with it*;  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**preempted:** *this one will force the current thread to abort transaction in progress during the next call of its transaction routines (the rest of transaction's code is a black box, remember?);*  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**state:** *normal operation, waiting in queue (for quick abort), quickly aborted*;  
**};**

Just as a regular cell, memory of thread_descriptor shall not be freed until all **contemporary transactions** finished plus all pending transfers of lock to that thread are finished (either accepted or dropped).

First thread reads the mutable_cell.lock - it's free now, so it CAS-es his thread_descriptor into mutable_cell.lock. Second thread comes and sees the mutable_cell.lock taken. Next the second thread checks the thread_descriptor to read the thread_descriptor.queue_to_thread and thread_descriptor.preempt_to_thread. If both are empty, then it can queue itself for this lock. Single step queue - that's how far we can go with planning because of how non-linear dependencies are.

Now if comes third thread and sees queue_to_thread or preempt_to_thread filled, than the third thread can decide to abort its transaction, queue itself instead of the current queued thread (preempt the queued thread), or change the preempt_to_thread to self due to higher priority of self.

There must be some threshold for modification of queue_to_thread and preempt_to_thread so rescheduling will be minimized.

This model need to work well for 3 scenarios of workload:
* No contention. Threads simply CAS themself into lock
* Low contention. Threads don't CAS the lock, but simply checks the lock value and proceedes into thread_descriptor to CAS the queue request (or preemption)
* High contention. Threads don't CAS the lock neither they CAS the thread_descriptor queue field - they just wait with unknown mechanism until the lock or queue becomes available

It's really desirable to never cause the contention to suddenly become higher due to transactions' internal behaviour.

# Scheduling

Although the only possible strict scheduling is single step long, there is a possibility for waiting threads to form an ordered queue with high priority threads in front and low priority at the back. However, due to heterogenous nature of the system (synchronous/asynchronous, fast/slow requests) it is desirable to allow inclusion of fast requests in between the slow ones. Failing to do so might cause huge drop of performance and increased contention frequency, leading to sudden drops in performance which might eventually destroy the reason to use our system. For example, this problem was encountered by Windows developers: [Anti-convoy locks in Windows Server 2003 SP1 and Windows Vista](http://joeduffyblog.com/2006/12/14/anticonvoy-locks-in-windows-server-2003-sp1-and-windows-vista/)

The queue might be a double-linked list made of the thread_descriptor-s themselfs. To allow for fast requests to happen in between transaction, we can use a flag in the current owner of the lock, saying "I finished my transaction and don't need the locks, but they haven't been taken from me yet". Clearing the lock mioght be another option, but some random transaction might come along at random moment and take our lock without consulting the waiting queue.

To avoid deadlocks it is necessary to make a clean distinction between running and waiting transactions. That's kinda position of the thread in the one step queue: you are either first or you belong to the whole bunch of the "second" waiting transactions in the volatile queue. If the running thread fails to acquire the lock, he moves into the second row of waiting threads.

The problem of mutual locks remains, and taking them via wake up and two step "give-accept" mechanism acutally leads to a three step mechanism request-give-accept which might introduce an unacceptable delay, up to hunderds of milliseconds. The problem might be resolved by allowing the running thread to hijack any lock from waiting threads. To do that, some kind of a **flag** is required in the thread_descriptor indicating the **waiting status**, so the running thread can take the lock instantly and signal the waiting thread to **abort due to lost lock**. This also implies all the **transaction data shall be releasable from outside**.

My first suggestion for an action after lock release/transfer is to wake up everybody. Obviously, this could lead to sudden high contention, similarly to a simple "retry transaction endlessly" tactic. Thus it's reasonable to wake up a limited amount of waiters, possibly some fixed amount of them, so the load will ramain the same no matter how many waiters are on the queue.

# Data protection

Python's low level libraries are prone to memory corruption. This is the price of performance (suddenly). Lmdb's attempt to protect its data could have been usefull, however, the need for fast synchronization mechanism on shared memory seems to be mutually exclusive with lmdb mechanisms. (cost of mprotect()( syscall is approx 0.5 us, write() syscall - 1+ us)
That's why we will have to use a simple writable memory at least for mutable data. To achieve some kind of protection, functions from outside shall use some kind of handle instead of a direct memory pointer. At the same time, some kind of translation is required to make references between objects immutable and transferable between processes. These two task can be performed by the same mechanism.

We can borrow the Hoard's structure of memory with several superblocks consisting of fixed-sized block, where one superblock can hold only blocks of same size and sizes are 2^n, thus on getmem() the smallest block is picked with size being larger than requested amount of bytes. We could make use of the blocks being of fixed size by forcing outside functions to point to blocks intead of bytes. For example, by using a pointer like "16 bits region number, 16 bits offset" on 32 processes - where "region number" is an index in the table of shared memory region, and "offset" can be converted into real pointer by something like "region_pointer + offset * 8" - no need to let the user address individual bytes. Handle-to-pointer functions will verify the region index, possibly mmap it if it's not mapped yet, find superblock header from "offset" value (superblocks all have the same predefined size being multiple of page size), verify the block position inside the superblock and verify the status of the block itself - that's like 4 additional reads from unique locations: 2 of them being shared (superblocks), 2 being private (local region mappings).

Still there remains a problem of some crazy code trying to write into completely random pointer e.g. the one left on the stack or in the global variable. Some things might be done:
* Always clear local direct references in local variables i.e. pointers in the stack
* Try to move the shared mappings as far as possible from the other application data. Much easier to implement in 64-bit space
* Direct reading/writing of large values with outside code might be benificial, although has to be reduced as much as possible

# Asynchronous and synchronous calls
 
Obviously, not all the operations are performed independently on shared memory. Some algorithms will require one thread to coordinate with another thread, and even wait for completion of the processing. Some external library could have been used for that purpose, but that implies some additional mean of data transfer through that library. We already have our own means of data transfer, so we want to reuse them as closely as possible. Also, anyway we need to implement the thread scheduling with waiting which requires some kind of signaling. Moreover, it could be advantageous to make the scheduler aware of custom signals for asynchronous processing.

The common implementation is a mix of waiting in userspace and kernel (similar to futex and windows critical section), with flag in a shared memory as a fast fundamental mechanism and an auxiliary kernel object for long waits. The last one can be shared for several condition flags, so we won't need to create multiple kernel objects and wait on all of them.
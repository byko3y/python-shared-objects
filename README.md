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

## Goals

Make perfectly sharable objects that look very similar to native python objects (like list, dict, object with methods and descriptors) whose data and methods reside in shared memory, thus being readable and modifiable from multiple interpreters and processes without a need for pickle/unpickle or complicated interaction with IPC. Shared objects won't have access to regular objects (because you can't see those outside the process), thus the only way of communication is from a regular object to a shared object.<br/>

Main facilities to implement, in order of importance:
* Message passing for initializing and coordinating the interpreters. Could be implemented with the same shared objects, but we just need to have some solid ground during development
* Basic native-looking objects that use shared memory to store their data (int, string, tuple, list, dict, object). Unfortunately, CPython's implementation of those objects is inherently single-threaded, but probably can be partially reused with the help of object-level locks, until a real lock-free implementation is made. It's preferable to store basic values like int/float/bool as in-place data instead of a separate object. Methods can be stored as a function text, which can be compiled and executed in the context of calling thread.
* Memory manager for managing allocation of small objects like a string or an element of list. Saves a lot of resources by using one piece of shared memory for multiple parts of data.<br/>
* Parallel garbage collector. Python generational reference-count-based collector is a stop-the-world kind of collector. Initial implementations might lack the garbage collector overall, managing the references and release of unused objects by virtue of reference counting with atomic increment/decrement.
CPython extension implementing Shared Transactional Memory with native-looking interface

This is my alternative to channels and pickle for cross-interpreter communication for the ongoing [PEP 554 -- Multiple Interpreters in the Stdlib](https://www.python.org/dev/peps/pep-0554/) and [multi-core-python](https://github.com/ericsnowcurrently/multi-core-python). Treat this project as a deep rework of a standard multiprocessing.sharedctypes, intended to implement a support for complex dynamic data and complex atomic operations.

See [INSTALL.txt](INSTALL.txt) for instructions on setup. Only 32-bit Linux or Windows are supported right now (64-bit support is on its way), and probably CPython 3.7+. It's still kinda proof-of-concept, so don't expect too much from it.

https://habr.com/en/post/585320/ - Detailed article with examples, benchmarks, and review of implementation details.

#### Brief tutorial on usage

1. Import the library using ```import pso```;
2. Primary process calls ```pso.init()``` and notes the returned unique ID of the session;
3. Secondary processes join the session with ```pso.connect(ID)```;
4. Now all the processes can access a common ```pso.root()``` storage via attributes of this object.

Currently the ```pso.root()``` shared storage can be used for such types as: str, bytes, bool, int, float, dict, list, tuple, instances of pso.ShmObject and its subclasses, and any combination of mentioned data types.

For example, you can run this code side by side in two terminals:

| Primary | Secondary |
|--|--|
| >>> import pso  | >>> import pso  |
| >>> pso.init()<br>'pso_FHu0a4idD'  |  |
|  | >>> pso.connect('pso_FHu0a4idD')  |
| >>> r = pso.root() <br> >>> r.list = [] | r = pso.root() |
|  | >>> r.list<br>ShmList: 0 elements <pso.ShmList object at 0xb7403030>, empty  |
|  | >>> r.list.append('stringy')  |
| >>> r.list<br>ShmList: 1 elements <pso.ShmList object at 0xb73f5030>, first element: stringy <string temporary at 0x(nil)> | >>> r.list<br>ShmList: 1 elements <pso.ShmList object at 0xb7403040>, first element: stringy <string temporary at 0x(nil)> |

Those are basics you can do with multiprocessing.ShareableList too... except you cannot grow the list after creation and you cannot put other mutable containers into it. But we are just getting started.

#### Transactions

Extending the two terminals example:

| Primary | Secondary |
|--|--|
| >>> pso.transaction_start()<br> >>> r.list.append('another string') |  |
|  | >>> len(r.list)<br>1 |
| >>> pso.transaction_commit() |  |
|  | >>> len(r.list)<br>2 |
| >>> pso.transaction_start()<br> >>> r.list.append('third string') |  |
|  | >>> r.list <br>*execution pauses* |
| >>> pso.transaction_commit() | ShmList: 3 elements <pso.ShmList object at 0xb7403050>, first element: stringy <string temporary at 0x(nil)> |

As you can see, the changes made within transaction are invisible to other processes until committed. Moreover, modified uncommitted objects cannot be read by other processes. By making multiple changes withing a single transaction, you can commit them atomically and be sure nobody sees half-committed inconsistent data.

At first it might look like a simple fine-grained locking, but actually it magically resolves deadlocks and resource starvation. Let's repeat the last operation again but do the reading part with a longer running transaction:

| Primary | Secondary |
|--|--|
|  | >>> pso.transaction_start() <br> >>> r.list <br> ShmList: 3 elements <pso.ShmList object at 0xb7403080>, first element: stringy <string temporary at 0x(nil)> |
| >>> pso.transaction_start()<br> >>> r.list.append('fourth string') <br> Traceback (most recent call last):<br>  File "<stdin>", line 1, in <module><br>pso.ShmAbort: Transaction aborted

Oops, what happenned? The two transactions contended for the same resource again, but the result is different. Obviously, one transaction should be terminated and the other should keep running. PSO knows which one of them is running longer and allows it to proceed, while a recently started transaction is aborted.

In this very example the primary process is left in a limbo state where you are required to handle the contention somehow e.g. do ```pso.transaction_abort()```. However, there already exist functions that can handle everything for you. Those are: ```pso.transaction()``` which accepts a function to be executed atomically, and a special ```with transaction:``` syntactic sugar which can run an unnamed block atomically e.g.:

```python
with transaction:
    r.list.append('Fifth element')
```

This syntactic sugar requires a special module loader, activated either by running ```python3 -m pso modulename.py``` or by naming a loaded module with a ```.pso``` suffix, like ```modulename.pso.py```. The latter option is only available once the "pso" module is loaded, so it won't work for the main module of your project.

You can find example programs in the ```examples/``` folder, those are launched from the project's root using the following commands:  
python3 [examples/simple_workers.py](examples/simple_workers.py)  
python3 -m pso [examples/accounts.pso.py](examples/accounts.pso.py)  
python3 [examples/producer_consumer.py](examples/producer_consumer.py)  
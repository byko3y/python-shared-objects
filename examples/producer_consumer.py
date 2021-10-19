import sys
import pso
import subprocess
import time

class Queue(pso.ShmObject):
    termination_mark = False
    def __init__(this):
        this.deque = pso.ShmList()
        this.promise = pso.ShmPromise()

    def put(this, value):
        with transaction:
            this.deque.append(value)
            this.promise.signal(True)

    def pop(this):
        rslt = this.deque.popleft()
        while rslt is None:
            this.promise.wait(0) # cannot be waited inside transaction
            this.promise = pso.ShmPromise()
            rslt = this.deque.popleft()

        if rslt is this.termination_mark:
            return (None, True)
        else:
            return (rslt, False)

    def terminate(this):
        this.terminated = True
        this.promise.signal(True)

number_of_accounts = 200

print('sys.argv: ', sys.argv)

if len(sys.argv) != 3 or sys.argv[1] != 'worker':
    # main process
    coord_name = pso.init()

    pso.root().queue = queue = Queue()

    worker_count = 1
    if len(sys.argv) >= 2:
        worker_count = int(sys.argv[1])
    workers = [subprocess.Popen([sys.executable, '-m', 'pso', sys.argv[0], 'worker', coord_name])
        for i in range(worker_count)]

    items = []
    ends = 0
    while ends < worker_count:
        #items.append(queue.pop())
        (item, eoq) = queue.pop()
        if eoq:
            ends += 1
        print(item)
    #print('Got items: ', items)

    print(f'Queue object contention (r, w): {pso.get_contention_count(queue)}')
    print(f'ShmList contention (r, w): {pso.get_contention_count(queue.deque)}')
    print(f'ShmList final state: {list(queue.deque)}')
    statuses = [w.wait(None) for w in workers]
    input("Press Enter to continue...")
else:
    # worker process
    pso.connect(sys.argv[2])
    queue = pso.root().queue
    for i in range(100):
        queue.put(i)
    queue.put(queue.termination_mark)

import sys
import random
from multiprocessing import Process, Manager, Lock

class Account():
    def __init__(this, id, starting_value):
        this.id = id
        this.balance = starting_value

def worker(accounts, locks, worker_count):
    local_locks = dict(locks)
    for i in range(100*1000 // worker_count):
        source_index = random.randrange(number_of_accounts)
        target_index = random.randrange(number_of_accounts)
        source = f'client{source_index}'
        target = f'client{target_index}'
        amount = random.randrange(1, 50)
        if source != target:
            if worker_count == 1:
                if amount <= accounts[source].balance:
                    accounts[source].balance -= amount
                    accounts[target].balance += amount
            else:
                # acquire locks from lowest index to highest to avoid deadlocks
                if source_index <= target_index:
                    lock1, lock2 = local_locks[source], local_locks[target]
                else:
                    lock1, lock2 = local_locks[target], local_locks[source]

                if not lock1.acquire():
                    raise Exception(f'failed to get lock {source}-{target}')
                try:
                    if not lock2.acquire():
                        raise Exception(f'failed to get lock {source}-{target}')
                    try:
                        if amount <= accounts[source].balance:
                            accounts[source].balance -= amount
                            accounts[target].balance += amount
                    finally:
                        lock2.release()
                finally:
                    lock1.release()

worker_count = 3

number_of_accounts = 200

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        worker_count = int(sys.argv[1])

    manager = Manager()
    accounts = manager.dict({f'client{i}': Account(i, random.randrange(1000))
        for i in range(number_of_accounts)})
    locks = manager.dict({f'client{i}': manager.Lock()
        for i in range(number_of_accounts)})

    original_values = [accounts[f'client{i}'].balance
        for i in range(number_of_accounts)]
    original_sum = sum(original_values)
    print(f'Original sum: {original_sum}')
    print(f'{original_values}')

    print(f'Launching account processing with {worker_count} workers')
    workers = [Process(target=worker, args=(accounts, locks, worker_count))
        for i in range(worker_count)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    new_values = [accounts[f'client{i}'].balance
        for i in range(number_of_accounts)]
    new_sum = sum(new_values)
    print(f'New sum: {new_sum}')
    print(f'{new_values}')

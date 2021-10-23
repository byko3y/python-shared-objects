import sys
import random
import pso
import subprocess

class Account(pso.ShmObject):
    def __init__(this, id, starting_value):
        this.id = id
        this.balance = starting_value

number_of_accounts = 200

if len(sys.argv) != 3 or sys.argv[1] != 'worker':
    # main process
    coord_name = pso.init()

    pso.root().worker_count = 3
    if len(sys.argv) >= 2:
        pso.root().worker_count = int(sys.argv[1])

    pso.root().accounts = {f'client{i}': Account(i, random.randrange(1000))
        for i in range(number_of_accounts)}
    original_values = [pso.root().accounts[f'client{i}'].balance
        for i in range(number_of_accounts)]
    original_sum = sum(original_values)
    print(f'Original sum: {original_sum}')
    print(f'{original_values}')

    print(f'Launching account processing with {pso.root().worker_count} workers')
    workers = [subprocess.Popen([sys.executable, '-m', 'pso', sys.argv[0], 'worker', coord_name])
        for i in range(pso.root().worker_count)]
    statuses = [w.wait(None) for w in workers]

    new_values = [pso.root().accounts[f'client{i}'].balance
        for i in range(number_of_accounts)]
    new_sum = sum(new_values)
    print(f'New sum: {new_sum}')
    print(f'{new_values}')
    if new_sum != original_sum:
        raise Exception('Sums do not match')
else:
    # worker process
    pso.connect(sys.argv[2])
    accounts = pso.root().accounts
    for i in range(100*1000 // pso.root().worker_count):
        source_index = random.randrange(number_of_accounts)
        target_index = random.randrange(number_of_accounts)
        source = f'client{source_index}'
        target = f'client{target_index}'

        amount = random.randrange(1, 50)
        if source != target:
            with transaction:
                if amount <= accounts[source].balance:
                    accounts[source].balance -= amount
                    accounts[target].balance += amount

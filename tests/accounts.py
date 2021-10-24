import sys
import os # debug
import random
import pso
import subprocess

class Account(pso.ShmObject):
    def __init__(this, id, starting_value):
        this.id = id
        this.balance = starting_value

number_of_accounts = 200

print('sys.argv: ', sys.argv)

if len(sys.argv) != 3 or sys.argv[1] != 'worker':
    # main process
    coord_name = pso.init()

    pso.root().worker_count = 3
    if len(sys.argv) >= 2:
        pso.root().worker_count = int(sys.argv[1])

    pso.root().accounts = {f'client{i}': Account(i, random.randrange(1000))
        for i in range(number_of_accounts)}
    #original_values = [client.balance for client in pso.root().accounts.values()]
    original_values = [pso.root().accounts[f'client{i}'].balance
        for i in range(number_of_accounts)]
    original_sum = sum(original_values)
    print(f'Original sum: {original_sum}')
    print(f'{original_values}')
    pso.object_debug_stop_on_contention(pso.root().accounts)

    print(f'Launching account processing with {pso.root().worker_count} workers')
    workers = [subprocess.Popen([sys.executable, sys.argv[0], 'worker', coord_name])
        for i in range(pso.root().worker_count)]
    try:
        statuses = [w.wait(None) for w in workers]
    except:
        pass

    # new_values = [client.balance for client in pso.root().accounts.values()]
    new_values = [pso.root().accounts[f'client{i}'].balance
        for i in range(number_of_accounts)]
    new_sum = sum(new_values)
    print(f'New sum: {new_sum}')
    print(f'{new_values}')
    if new_sum != original_sum:
        raise Exception('Sums do not match')

    accounts = pso.root().accounts
    print(f'Root contention (r, w): {pso.get_contention_count(accounts)}')
    contentions = [pso.get_contention_count(accounts[f'client{i}'])
        for i in range(number_of_accounts)]
    print(f'Items contentions (r, w): {contentions}')
    del accounts
    pso.root().accounts = None
    #input("Press Enter to continue...")
else:
    # worker process
    pso.connect(sys.argv[2])
    accounts = pso.root().accounts
    attempts = 0
    commits = 0
    for i in range(100*1000 // pso.root().worker_count):
        source_index = random.randrange(number_of_accounts)
        target_index = random.randrange(number_of_accounts)
        source = f'client{source_index}'
        target = f'client{target_index}'

        amount = random.randrange(1, 50)
        if source != target:
            @pso.transaction
            def transaction():
                global attempts
                attempts += 1
                #if attempts >= 10000:
                #    pso.enable_debug_stop_on_contention()
                if amount <= accounts[source].balance:
                    accounts[source].balance -= amount
                    accounts[target].balance += amount
                    #for i in range(1000):
                    #    a = amount * 2
                # Cool way to break the library:
                # if accounts[source].balance <= 0:
                #     raise Exception(f'Invalid balance {accounts[source].balance}')
                global commits
                commits += 1

            transaction()

    print(f'{os.getpid()}. {attempts} attempts, {commits} commits')
    pso.print_thread_counters()

# accounts.py example implemented in a regular single-threaded python without shared objects.
import random

class Account:
    def __init__(this, id, starting_value):
        this.id = id
        this.balance = starting_value

number_of_accounts = 200

accounts = {f'client{i}': Account(i, random.randrange(1000))
    for i in range(number_of_accounts)}
original_values = [client.balance for client in accounts.values()]
original_sum = sum(original_values)
print(f'Original sum: {original_sum}')
print(f'{original_values}')

for i in range(100*1000):
    source_index = random.randrange(number_of_accounts)
    target_index = random.randrange(number_of_accounts)
    source = f'client{source_index}'
    target = f'client{target_index}'

    amount = random.randrange(1, 50)
    if source != target:
        if amount <= accounts[source].balance:
            accounts[source].balance -= amount
            accounts[target].balance += amount

new_values = [client.balance for client in accounts.values()]
new_sum = sum(original_values)
print(f'New sum: {original_sum}')
print(f'{new_values}')

if new_sum != original_sum:
    raise Exception('Sums do not match')

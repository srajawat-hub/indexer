from math import sqrt

import pandas as pd
import numpy as np
import random
import string
from datetime import datetime, timedelta

# Helper functions to generate random hex addresses and hashes
def random_hex_string(length):
    return '0x' + ''.join(random.choice('0123456789abcdef') for _ in range(length - 2))

def random_timestamp(start, end):
    """Generate a random timestamp between two datetime objects."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# Define token list
tokens = ['TOKENA', 'TOKENB', 'TOKENC', 'TOKEND']

# Generate 100 mock records
num_records = 1000
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 6, 1)

data = []
def_price = 100.0
prices = {}
pools = {}
current_timestamp = datetime(2025, 1, 1, 0, 0, 0)
block_number = 100

for _ in range(num_records):
    token_in = random.choice(tokens)
    token_out = random.choice([t for t in tokens if t != token_in])
    if token_in < token_out:
        pair = f"{token_in}/{token_out}"
    else:
        pair = f"{token_out}/{token_in}"

    amount_in = round(random.uniform(1, 1000), 6)
    old_price = prices.get(pair, random.uniform(10, 1000))
    price = round(old_price + random.uniform(-8, 10), 6)
    prices[pair] = price
    amount_out = round(amount_in * (1 / price), 6)
    amount_in_usd = round(amount_in * price, 2)
    amount_out_usd = round(amount_out * price, 2)
    delta_seconds = random.randint(1, 30 * 60)  # 1 to 1800 seconds
    current_timestamp += timedelta(seconds=delta_seconds)
    pool_address = pools.get(pair, random_hex_string(42))
    pools[pair] = pool_address
    block_number += random.randint(0, 3)

    record = {
        'pool_address': pool_address,
        'token_in': token_in,
        'token_out': token_out,
        'amount_in': amount_in,
        'amount_out': amount_out,
        'amount_in_usd': amount_in_usd,
        'amount_out_usd': amount_out_usd,
        'initiator_user_address': random_hex_string(42),
        'price': price,
        'transaction_hash': random_hex_string(66),
        'block_number': block_number,
        'timestamp': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'chain_id': 1,
        'is_vault_initiated': random.choice([True, False]),
        'sqrt_price': sqrt(price),
        'liquidity': 0,
        'tick': 0
    }
    data.append(record)

# Create DataFrame
df = pd.DataFrame(data)

# Generate SQL INSERT statements and save to file
sql_statements = [
    "DELETE FROM public.ammswap;"
]
for pair, pool_address in pools.items():
    tok_a, tok_b = pair.split('/')
    sql_statements.append(
        f"insert into public.pools (pool_address, chain_id, token_0_address, token_1_address, fee, tick_spacing, pool_type, project_manager, block_number, created_at, min_trade_amt, max_trade_amt, metadata, etp_start_time, etp_end_time, launch_type, initial_sqrt_price, initial_tick, token_supply)"
        f"values  ('{pool_address}', 1, '{tok_a}', '{tok_b}', 1, 30, 'EVM', '0x', 1, '2025-06-02 15:54:42.000000', '0', '0', '{{}}', '2025-06-02 15:54:46.000000', '2025-06-02 15:54:48.000000', 'FAIR', 1, 1, 10);"
    )

for _, row in df.iterrows():
    sql_statements.append(
        f"INSERT INTO public.ammswap ("
        f"pool_address, token_in, token_out, amount_in, amount_out, amount_in_usd, amount_out_usd, "
        f"initiator_user_address, price, transaction_hash, block_number, timestamp, chain_id, "
        f"is_vault_initiated, sqrt_price, liquidity, tick"
        f") VALUES ("
        f"'{row['pool_address']}', '{row['token_in']}', '{row['token_out']}', {row['amount_in']}, {row['amount_out']}, "
        f"{row['amount_in_usd']}, {row['amount_out_usd']}, "
        f"'{row['initiator_user_address']}', {row['price']}, '{row['transaction_hash']}', {row['block_number']}, "
        f"'{row['timestamp']}', {row['chain_id']}, {str(row['is_vault_initiated']).upper()}, "
        f"{row['sqrt_price']}, {row['liquidity']}, {row['tick']}"
        f");"
    )

# Save SQL to a file
file_path = 'mock_swap_amm_inserts.sql'
with open(file_path, 'w') as f:
    f.write('\n'.join(sql_statements))

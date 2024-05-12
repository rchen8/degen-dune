from datetime import datetime
from dotenv import load_dotenv
import json
import os
import pathlib
import requests
import subprocess
import time

load_dotenv()

def cryo(table, start_block, end_block, chunk_size):
  columns = {
    'blocks': ['timestamp', 'block_number', 'gas_limit', 'gas_used', 'difficulty', 'total_difficulty', 'size', 'base_fee_per_gas', 'state_root', 'transactions_root', 'receipts_root', 'block_hash', 'parent_hash', 'author', 'nonce', 'extra_data'],
    'transactions': ['timestamp', 'value', 'block_number', 'gas_limit', 'gas_price', 'gas_used', 'max_fee_per_gas', 'max_priority_fee_per_gas', 'nonce', 'transaction_index', 'success', 'transaction_type', 'chain_id', 'from_address', 'to_address', 'block_hash', 'input', 'transaction_hash'],
    'logs': ['block_number', 'block_hash', 'address', 'topic0', 'topic1', 'topic2', 'topic3', 'data', 'transaction_hash', 'log_index', 'transaction_index']
  }

  subprocess.run(['cryo',
    '--rpc', 'https://rpc.degen.tips/http',
    '--chunk-size', str(chunk_size),
    '--output-dir', 'output', '--overwrite', '--json', '--no-report',
    table, '--blocks', '%d:%d' % (start_block, end_block),
    '--include-columns', *columns[table]
  ])

def create_blocks_schema(start_block, end_block):
  schema = ''
  filename = 'output/network_666666666__blocks__%08d_to_%08d.json' % (start_block, end_block - 1)
  with open(filename) as file:
    for block in json.load(file):
      block['date'] = datetime.utcfromtimestamp(block['timestamp']).strftime('%Y-%m-%d')
      block['time'] = datetime.utcfromtimestamp(block['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
      block['number'] = block['block_number']
      block['total_difficulty'] = block['total_difficulty_string']
      block['hash'] = block['block_hash']
      block['miner'] = block['author']

      del block['timestamp']
      del block['block_number']
      del block['total_difficulty_binary']
      del block['total_difficulty_string']
      del block['total_difficulty_f64']
      del block['block_hash']
      del block['author']
      del block['chain_id']

      schema += json.dumps(block) + '\n'
  return schema

def create_transactions_schema(start_block, end_block):
  schema = ''
  filename = 'output/network_666666666__transactions__%08d_to_%08d.json' % (start_block, end_block - 1)
  with open(filename) as file:
    for txn in json.load(file):
      txn['block_date'] = datetime.utcfromtimestamp(txn['timestamp']).strftime('%Y-%m-%d')
      txn['block_time'] = datetime.utcfromtimestamp(txn['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
      txn['value'] = txn['value_string']
      txn['index'] = txn['transaction_index']
      txn['type'] = txn['transaction_type']
      txn['from'] = txn['from_address']
      txn['to'] = txn['to_address']
      txn['data'] = txn['input']
      txn['hash'] = txn['transaction_hash']

      del txn['timestamp']
      del txn['value_binary']
      del txn['value_string']
      del txn['value_f64']
      del txn['transaction_index']
      del txn['transaction_type']
      del txn['from_address']
      del txn['to_address']
      del txn['input']
      del txn['transaction_hash']
      del txn['n_input_bytes']
      del txn['n_input_zero_bytes']
      del txn['n_input_nonzero_bytes']

      schema += json.dumps(txn) + '\n'
  return schema

def create_logs_schema(start_block, end_block):
  filename = 'output/network_666666666__transactions__%08d_to_%08d.json' % (start_block, end_block - 1)
  timestamp = {}
  addresses = {}
  with open(filename) as file:
    for txn in json.load(file):
      timestamp[txn['block_number']] = txn['timestamp']
      addresses[txn['transaction_hash']] = (txn['from_address'], txn['to_address'])

  schema = ''
  filename = 'output/network_666666666__logs__%08d_to_%08d.json' % (start_block, end_block - 1)
  with open(filename) as file:
    for log in json.load(file):
      log['block_date'] = datetime.utcfromtimestamp(timestamp[log['block_number']]).strftime('%Y-%m-%d')
      log['block_time'] = datetime.utcfromtimestamp(timestamp[log['block_number']]).strftime('%Y-%m-%d %H:%M:%S')
      log['contract_address'] = log['address']
      log['tx_hash'] = log['transaction_hash']
      log['index'] = log['log_index']
      log['tx_index'] = log['transaction_index']
      log['tx_from'] = addresses[log['transaction_hash']][0]
      log['tx_to'] = addresses[log['transaction_hash']][1]

      del log['address']
      del log['transaction_hash']
      del log['log_index']
      del log['transaction_index']
      del log['n_data_bytes']
      del log['chain_id']

      schema += json.dumps(log) + '\n'
  return schema

def insert_data(table, schema):
  start_time = time.time()
  url = 'https://api.dune.com/api/v1/blockchain/degen/%s/insert' % table
  headers = {
    'X-DUNE-API-KEY': os.environ['X-DUNE-API-KEY'],
    'Content-Type': 'application/x-ndjson'
  }
  response = requests.post(url, data=schema, headers=headers)
  print(response.text, time.time() - start_time)

def get_start_block():
  url = 'https://api.dune.com/api/v1/query/3716015/execute'
  headers = {
    'X-DUNE-API-KEY': os.environ['X-DUNE-API-KEY']
  }
  response = requests.post(url, headers=headers).json()

  start_time = time.time()
  while time.time() - start_time < 60:
    url = "https://api.dune.com/api/v1/execution/%s/results" % response['execution_id']
    response = requests.get(url, headers=headers).json()
    if not response['is_execution_finished']:
      continue
    return response['result']['rows'][0]['_col0'] + 1

def get_end_block():
  url = 'https://explorer.degen.tips/api/v2/stats'
  return int(requests.get(url).json()['total_blocks'])

####################################################################################################

chunk_size = 10000
start_block = get_start_block()
end_block = get_end_block()
print(start_block, end_block)

for start_block in range(start_block, end_block, chunk_size):
  if start_block + chunk_size > end_block:
    break

  cryo('blocks', start_block, start_block + chunk_size, chunk_size)
  cryo('transactions', start_block, start_block + chunk_size, chunk_size)
  cryo('logs', start_block, start_block + chunk_size, chunk_size)

  blocks_schema = create_blocks_schema(start_block, start_block + chunk_size)
  txns_schema = create_transactions_schema(start_block, start_block + chunk_size)
  logs_schema = create_logs_schema(start_block, start_block + chunk_size)

  insert_data('blocks', blocks_schema)
  insert_data('transactions', txns_schema)
  insert_data('logs', logs_schema)

  pathlib.Path.unlink('output/network_666666666__blocks__%08d_to_%08d.json' % (start_block, start_block + chunk_size - 1))
  pathlib.Path.unlink('output/network_666666666__transactions__%08d_to_%08d.json' % (start_block, start_block + chunk_size - 1))
  pathlib.Path.unlink('output/network_666666666__logs__%08d_to_%08d.json' % (start_block, start_block + chunk_size - 1))

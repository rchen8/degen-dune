import cryo

CHAIN_RPC = "https://rpc.sepolia.org/"

## Removed gas_limit, difficulty, nonce for Sepolia example
BLOCKS_COLUMNS = [
    "timestamp",
    "block_number",
    "gas_used",
    "total_difficulty",
    "size",
    "base_fee_per_gas",
    "state_root",
    "transactions_root",
    "receipts_root",
    "block_hash",
    "parent_hash",
    "author",
    "extra_data",
]
LOGS_COLUMNS = [
    "block_number",
    "block_hash",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
    "transaction_hash",
    "log_index",
    "transaction_index",
]
TRANSACTIONS_COLUMNS = [
    "timestamp",
    "value",
    "block_number",
    "gas_limit",
    "gas_price",
    "gas_used",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "nonce",
    "transaction_index",
    "success",
    "transaction_type",
    "chain_id",
    "from_address",
    "to_address",
    "block_hash",
    "input",
    "transaction_hash",
]


def get_blocks(start_block, end_block, chunk_size):
    """
    Get blocks data for blocks table.

    Args:
        start_block: start block for the queried data.
        end_block: end block for the queried data.
        chunk_size: chunk size for the exported data. (Note: function will error if chunk_size is not within start_block and end_block)

    Returns:
        blocks: Pandas dataframe with blocks.
    """
    blocks = cryo.collect(
        "blocks",
        blocks=[f"{start_block}:{end_block}"],
        rpc=CHAIN_RPC,
        output_format="pandas",
        hex=True,
        include_columns=BLOCKS_COLUMNS,
        no_report=True,
    )
    return blocks


def get_transactions(start_block, end_block, chunk_size):
    """
    Get transactions data for transactions table.

    Args:
        start_block: start block for the queried data.
        end_block: end block for the queried data.
        chunk_size: chunk size for the exported data. (Note: function will error if chunk_size is not within start_block and end_block)

    Returns:
        transactions: Pandas dataframe with transactions.
    """
    transactions = cryo.collect(
        "transactions",
        blocks=[f"{start_block}:{end_block}"],
        rpc=CHAIN_RPC,
        output_format="pandas",
        hex=True,
        include_columns=TRANSACTIONS_COLUMNS,
        no_report=True,
    )
    return transactions


def get_logs(start_block, end_block, chunk_size):
    """
    Get logs for logs table.

    Args:
        start_block: start block for the queried data.
        end_block: end block for the queried data.
        chunk_size: chunk size for the exported data. (Note: function will error if chunk_size is not within start_block and end_block)

    Returns:
        logs: Pandas dataframe with logs.
    """
    logs = cryo.collect(
        "logs",
        blocks=[f"{start_block}:{end_block}"],
        rpc=CHAIN_RPC,
        output_format="pandas",
        hex=True,
        include_columns=LOGS_COLUMNS,
        no_report=True,
    )
    return logs

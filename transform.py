import json
import pandas as pd
from datetime import datetime


def transform_blocks(blocks_df):
    """
    Transform blocks data from cryo into ndjson accepted by Dune blockchain blocks insert endpoint.
    The transformation was adopted from Richard Chen's Degen indexing script

    Args:
        blocks_df: Pandas dataframe with blocks data from cryo.

    Returns:
        blocks_data: ndjson string with transformed blocks.
    """

    # Convert timestamp to date and time
    blocks_df["date"] = pd.to_datetime(blocks_df["timestamp"], unit="s").dt.strftime(
        "%Y-%m-%d"
    )
    blocks_df["time"] = pd.to_datetime(blocks_df["timestamp"], unit="s").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Rename columns
    blocks_df.rename(
        columns={
            "block_number": "number",
            "total_difficulty_string": "total_difficulty",
            "block_hash": "hash",
            "author": "miner",
        },
        inplace=True,
    )

    # Drop unnecessary columns
    columns_to_drop = [
        "timestamp",
        "total_difficulty_binary",
        "total_difficulty_f64",
        "chain_id",
    ]
    blocks_df.drop(columns_to_drop, axis=1, inplace=True)

    # Iterate over DataFrame rows to create ndjson string
    blocks_data = ""
    for _, row in blocks_df.iterrows():
        blocks_data += json.dumps(row.to_dict()) + "\n"
    return blocks_data


def transform_transactions(transactions_df):
    """
    Transform transactions data from cryo into ndjson accepted by Dune blockchain transactions insert endpoint.
    The transformation was adopted from Richard Chen's Degen indexing script

    Args:
        transactions_df: Pandas dataframe with transactions data from cryo.

    Returns:
        transactions_data: ndjson string with transformed transactions.
    """

    # Convert timestamp to block_date and block_time
    transactions_df["block_date"] = pd.to_datetime(
        transactions_df["timestamp"], unit="s"
    ).dt.strftime("%Y-%m-%d")
    transactions_df["block_time"] = pd.to_datetime(
        transactions_df["timestamp"], unit="s"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Rename columns
    transactions_df = transactions_df.rename(
        columns={
            "value_string": "value",
            "transaction_index": "index",
            "transaction_type": "type",
            "from_address": "from",
            "to_address": "to",
            "input": "data",
            "transaction_hash": "hash",
        }
    )

    # Drop unnecessary columns
    columns_to_drop = [
        "timestamp",
        "value_f64",
        "value_binary",
        "n_input_bytes",
        "n_input_zero_bytes",
        "n_input_nonzero_bytes",
    ]
    transactions_df.drop(columns=columns_to_drop, inplace=True)
    transactions_df.fillna(-1, inplace=True)

    # Iterate over DataFrame rows to create ndjson string
    transactions_data = ""
    for _, row in transactions_df.iterrows():
        transactions_data += json.dumps(row.to_dict()) + "\n"
    return transactions_data


def transform_logs(transactions_df, logs_df):
    """
    Transform logs data from cryo into ndjson accepted by Dune blockchain logs insert endpoint.
    The transformation was adopted from Richard Chen's Degen indexing script

    Args:
        transactions_df: Pandas dataframe with transactions data from cryo.
        logs_df: Pandas dataframe with logs data from cryo.

    Returns:
        logs_data: ndjson string with transformed logs.
    """

    # Create timestamp and addresses dictionaries
    timestamp = {}
    addresses = {}
    for index, txn in transactions_df.iterrows():
        timestamp[txn["block_number"]] = txn["timestamp"]
        addresses[txn["transaction_hash"]] = (txn["from_address"], txn["to_address"])

    # Apply transformations to logs_df
    logs_df["block_date"] = logs_df["block_number"].map(
        lambda x: datetime.utcfromtimestamp(timestamp[x]).strftime("%Y-%m-%d")
    )
    logs_df["block_time"] = logs_df["block_number"].map(
        lambda x: datetime.utcfromtimestamp(timestamp[x]).strftime("%Y-%m-%d %H:%M:%S")
    )

    logs_df["tx_from"] = logs_df["transaction_hash"].map(lambda x: addresses[x][0])
    logs_df["tx_to"] = logs_df["transaction_hash"].map(lambda x: addresses[x][1])

    # Rename columns
    logs_df = logs_df.rename(
        columns={
            "address": "contract_address",
            "transaction_hash": "tx_hash",
            "log_index": "index",
            "transaction_index": "tx_index",
        }
    )

    # Drop unnecessary columns
    # Removed `n_data_bytes` for Sepolia
    logs_df.drop(columns=["chain_id"], inplace=True)

    # Iterate over DataFrame rows to create ndjson string
    logs_data = ""
    for _, row in logs_df.iterrows():
        logs_data += json.dumps(row.to_dict()) + "\n"
    return logs_data

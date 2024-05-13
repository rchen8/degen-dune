from dotenv import load_dotenv
import logging

from rpc import get_end_block
from dune import get_start_block
from dune import insert_data

from cryo_utils import get_blocks
from cryo_utils import get_transactions
from cryo_utils import get_logs

from transform import transform_blocks
from transform import transform_transactions
from transform import transform_logs


load_dotenv()
logger = logging.getLogger(__name__)

chunk_size = 1
start_block = 5895130  # get_start_block()
end_block = 5895136  # get_end_block()
blockchain = "sepolia"
logger.info("Started harvesting blocks from %s to %s", start_block, end_block)

for start_block in range(start_block, end_block, chunk_size):
    if start_block + chunk_size > end_block:
        break

    blocks = get_blocks(start_block, start_block + chunk_size, chunk_size)
    transactions = get_transactions(start_block, start_block + chunk_size, chunk_size)
    logs = get_logs(start_block, start_block + chunk_size, chunk_size)

    blocks_data = transform_blocks(blocks)
    transactions_data = transform_transactions(transactions)
    logs_data = transform_logs(transactions, logs)

    insert_data(blockchain, "blocks", blocks_data)
    insert_data(blockchain, "transactions", transactions_data)
    insert_data(blockchain, "logs", logs_data)

from dotenv import load_dotenv
import requests
import time
import os

load_dotenv()


def insert_data(blockchain, table, schema):
    start_time = time.time()
    url = "https://api.dune.com/api/v1/blockchain/%s/%s/insert" % (blockchain, table)
    headers = {
        "X-DUNE-API-KEY": os.environ["DUNE_API_KEY"],
        "Content-Type": "application/x-ndjson",
    }
    response = requests.post(url, data=schema, headers=headers)
    print(response.text, time.time() - start_time)


# Degen specific function
# The Dune query needs to be updated for a different chain
def get_start_block():
    url = "https://api.dune.com/api/v1/query/3716015/execute"
    headers = {"X-DUNE-API-KEY": os.environ["DUNE_API_KEY"]}
    response = requests.post(url, headers=headers).json()

    start_time = time.time()
    while time.time() - start_time < 60:
        url = (
            "https://api.dune.com/api/v1/execution/%s/results"
            % response["execution_id"]
        )
        response = requests.get(url, headers=headers).json()
        if not response["is_execution_finished"]:
            continue
        return response["result"]["rows"][0]["_col0"] + 1

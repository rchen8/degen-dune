import requests


def get_end_block():
    url = "https://explorer.degen.tips/api/v2/stats"
    return int(requests.get(url).json()["total_blocks"])

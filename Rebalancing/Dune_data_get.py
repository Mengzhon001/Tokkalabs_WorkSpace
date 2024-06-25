import requests
import pandas as pd
import io
import json


def Dune_dataGet(QueryNo):
    url = "https://api.dune.com/api/v1/query/" + str(QueryNo) + "/results/csv"
    headers = {"X-DUNE-API-KEY": "r9bAPfbJIm4gZcCdu1VCvH7uRBa9sXQC"}
    response = requests.request("GET", url, headers=headers)
    r = response.content
    return pd.read_csv(io.StringIO(r.decode("utf-8")))


if __name__ == "__main__":
    QueryNo = 3803744
    DuneData=Dune_dataGet(QueryNo)
    DuneData.to_csv('Rebalancing/CEX_address.csv')

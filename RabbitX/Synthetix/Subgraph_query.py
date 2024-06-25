import requests
import pandas as pd


### using the subgraph for synthetix, find the supported
# Define the GraphQL endpoint
url = "https://api.thegraph.com/subgraphs/name/synthetix-perps/perps"

# Define the GraphQL query
query = """
{
  futuresMarkets {
    id
    asset
    marketKey
    isActive
  }
}
"""

# Send the request
response = requests.post(url, json={'query': query})

data = response.json()

markets = data['data']['futuresMarkets']

def bytes32_to_string(bytes32_value):
    # Remove trailing null bytes and decode to string
    return bytes32_value.rstrip(b'\x00').decode('utf-8')

for market in markets:
    market['asset_str'] = bytes32_to_string(bytes.fromhex(market['asset'][2:]))
    market['marketKey_str'] = bytes32_to_string(bytes.fromhex(market['marketKey'][2:]))

markets_pd = pd.DataFrame(markets)

markets_pd.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/Synthetix_markets.csv')

###### fetch funding rate ( the fetched data are for 24h rate (daily funding rate)

Market_list=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/Synthetix_markets.csv',index_col=0)

# Define the GraphQL endpoint
url = "https://api.thegraph.com/subgraphs/name/synthetix-perps/perps"

asset='DOGE'
market_id = "0x98ccbc721cc05e28a125943d69039b39be6a21e9"

# Define the GraphQL query
def get_query(time_start, market_id):
    time_gap = 60*60*12
    query_template = f"""
    {{
      fundingRateUpdates(
        orderBy: timestamp
        orderDirection: asc
        first: 1000
        skip: 0
        where: {{market: "{market_id}", timestamp_lt: "{time_start+time_gap}", timestamp_gte: "{time_start}"}}
      ) {{
        timestamp
        fundingRate
        index
        id
      }}
    }}
    """
    return query_template



# Function to fetch data
def fetch_data(time_start, market_id):
    query = get_query(time_start, market_id)
    response = requests.post(url, json={'query': query})
    if response.status_code == 200:
        return response.json()['data']['fundingRateUpdates']
    else:
        raise Exception(f"Query failed with status code {response.status_code}")

# Fetch all updates and adjust funding rates
all_updates = []
time_start = 1714449600

while True:
    updates = fetch_data(time_start, market_id)
    if time_start >= 1717992000:
        break
    all_updates.extend(updates)
    time_start += 60*60*12
    print(time_start)

fundingRate_raw=pd.DataFrame(all_updates)
fundingRate_raw.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/Synthetix_fundingRate_graph_raw'+asset+'.csv')

## prepare for the candel chart

import plotly.graph_objs as go
from datetime import datetime
from collections import defaultdict
import numpy as np


fundingRate_raw=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/Synthetix_fundingRate_graph_raw'+asset+'.csv',index_col=0)
data =  fundingRate_raw.to_dict(orient='records')

# Convert timestamp to datetime and fundingRate to float
for entry in data:
    entry['timestamp'] = datetime.fromtimestamp(int(entry['timestamp']))
    entry['fundingRate'] = float(entry['fundingRate'])

# Group by hour and aggregate fundingRate
hourly_data = defaultdict(list)
for entry in data:
    hour_key = entry['timestamp'].replace(minute=0, second=0, microsecond=0)
    hourly_data[hour_key].append(entry['fundingRate'])

# Create candlestick data
candlestick_data = []
for hour_key, rates in hourly_data.items():
    transformed_rates = [(1 + rate / 1e18) ** 365 - 1 for rate in rates]  # adjust funding Rate to apy
    print(transformed_rates)
    open_rate = transformed_rates[0]  # First rate in the hour
    close_rate = transformed_rates[-1]  # Last rate in the hour
    high_rate = max(transformed_rates)  # Highest rate in the hour
    low_rate = min(transformed_rates)

    candlestick_data.append(
        dict(
            x=hour_key,
            open=open_rate,
            high=high_rate,
            low=low_rate,
            close=close_rate
        )
    )

# Convert to DataFrame
df = pd.DataFrame(candlestick_data)

# Ensure datetime format
df['x'] = pd.to_datetime(df['x'])
df.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/Synthetix_fundingRate_graph_candel_'+asset+'.csv')
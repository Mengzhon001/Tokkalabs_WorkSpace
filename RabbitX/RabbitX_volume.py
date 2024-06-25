from datetime import datetime
import pandas as pd
import io

symbol_base='BTC'
symbol_quote='USD'
pair=symbol_base+'-'+symbol_quote

def datetime_to_timestamp(dt):
    # Convert the datetime object to a timestamp in seconds
    timestamp = dt.timestamp()
    # Convert seconds to milliseconds
    timestamp = int(timestamp)
    return timestamp

# time range to egt data
dt_start = datetime(2023, 12, 1, 12, 0, 0)
dt_end = datetime(2024, 6, 10, 12, 0, 0)
timeRange_micro = [datetime_to_timestamp(dt_start),datetime_to_timestamp(dt_end)]

######################################### get volume on rabbitX
import requests
import json

def getVolume_rabbitx(timeRange,pair):
    url = "https://api.prod.rabbitx.io/candles"
    params = {
        'market_id': pair,
        'timestamp_from': timeRange[0],
        'timestamp_to': timeRange[1],
        'period': 1440,
    }
    headers = {
        'api_key': 'OAoFn1m6IFw94HxCysYT77gKZRrBhDOiYz3UBkk39qo=',
        'api_secret': '0x85990a46d81000d35de10bd57a5ad8ca003dc4450009f32f6381b4de512414f7',
        'public_jwt': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI0MDAwMDAwMDAwIiwiZXhwIjo2NTQ4NDg3NTY5fQ.o_qBZltZdDHBH3zHPQkcRhVBQCtejIuyq8V1yj5kYq8',
        'private_jwt': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2MzA4NSIsImV4cCI6MTcyNTc5OTExNH0.rryUwNY1BPS8qb5nthCapOSKEoKpVHojNPeQN3IceIc',
        "Accept": "application/json"
    }
    response = requests.get(url, params=params, headers=headers)
    data=response.text
    data_dict=json.loads(data)
    return data_dict

def seconds_to_hour(seconds):
    dt = pd.to_datetime(seconds, unit='s')
    return dt.strftime('%Y-%m-%d')

def Dune_dataGet(QueryNo):
    url = "https://api.dune.com/api/v1/query/" + str(QueryNo) + "/results/csv"
    headers = {"X-DUNE-API-KEY": "QRgJQX7PTo7pOh3V1u0Fjsawcs4IRrGw"}
    response = requests.request("GET", url, headers=headers)
    r = response.content
    return pd.read_csv(io.StringIO(r.decode("utf-8")))
## RabbitX volume
data_dict = getVolume_rabbitx(timeRange_micro,pair)
RabbitX_volume=pd.DataFrame(data_dict['result'])
RabbitX_volume['time_hour'] = RabbitX_volume['time'].apply(seconds_to_hour)
RabbitX_volume=RabbitX_volume[['time_hour','time','volume']]
RabbitX_volume.rename(columns={'volume':'RabbitX','time_hour':'time','time':'timestamp'},inplace=True)

## get GMX data
QueryNo = 2638030
GMX_Arbitrum_WBTC_volume=Dune_dataGet(QueryNo)
GMX_Arbitrum_WBTC_volume.rename(columns={'TradingVolume_usd':'GMX_Arbitrum'},inplace=True)
RabbitX_volume=pd.merge(RabbitX_volume,GMX_Arbitrum_WBTC_volume,how='left',on='time')

## get dydx data
url = "https://api.coinalyze.net/v1/ohlcv-history"
headers = {

    "Accept": "application/json",
    'api_key':'62dcd7fc-85a0-4209-b654-8509a1509a06'
}

parameters = {
    'symbols':'BTC-USD.8',
    'interval':'daily',
    'from': timeRange_micro[0],
    'to': timeRange_micro[1],
}

response = requests.get(url, headers=headers,params=parameters)

data=response.text
data_dict=json.loads(data)

DYDX_BTC_volume=pd.DataFrame.from_dict(data_dict[0]['history'])
DYDX_BTC_volume=DYDX_BTC_volume[['t','v']]
DYDX_BTC_volume.rename(columns={'t':'timestamp','v':'dYdX'},inplace=True)
DYDX_BTC_volume['dYdX']=DYDX_BTC_volume['dYdX']*1000_000

RabbitX_volume=pd.merge(RabbitX_volume,DYDX_BTC_volume,how='left',on='timestamp')
RabbitX_volume.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/Rabbit_dex_volume.csv')


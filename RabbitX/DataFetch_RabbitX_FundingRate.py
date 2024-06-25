from datetime import datetime
import pandas as pd

###### pair to detect
symbol_base='ETH'
symbol_quote='USD'
pair=symbol_base+'-'+symbol_quote



def datetime_to_timestamp_milliseconds(dt):
    # Convert the datetime object to a timestamp in seconds
    timestamp_seconds = dt.timestamp()
    # Convert seconds to milliseconds
    timestamp_milliseconds = int(timestamp_seconds * 1_000_000)
    return timestamp_milliseconds

# time range to egt data
dt_start = datetime(2024, 4, 30, 12, 0, 0)
dt_end = datetime(2024, 6, 10, 12, 0, 0)
timeRange_micro = [datetime_to_timestamp_milliseconds(dt_start),datetime_to_timestamp_milliseconds(dt_end)]


######################################### get funding rate on rabbitX
import requests
import json

def getFundingRate_rabbitx(timeRange,pair):
    url = "https://api.prod.rabbitx.io/markets/fundingrate"
    params = {
        'market_id': pair,
        'p_limit': 1000,
        'start_time': timeRange[0],
        'end_time': timeRange[1],
        'p_page': 0,
        'p_order': "DESC"
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

def microseconds_to_hour(microseconds):
    dt = pd.to_datetime(microseconds, unit='us')
    return dt.strftime('%Y-%m-%d %H:00')

def hourly_to_apy(hourly_return):
    hours_per_year = 24 * 365
    if pd.isna(hourly_return):
        return None
    return (1 + hourly_return) ** hours_per_year - 1

data_dict = getFundingRate_rabbitx(timeRange_micro,pair)
RabbitX_FR=pd.DataFrame(data_dict['result'])
RabbitX_FR['time_hour'] = RabbitX_FR['timestamp'].apply(microseconds_to_hour)
RabbitX_FR['funding_rate'] = RabbitX_FR['funding_rate'].astype(float)
RabbitX_FR['funding_rate_apy'] = RabbitX_FR['funding_rate'].apply(hourly_to_apy)

#################################### get the perp list & exchange list on coinalyze
## perp list
url = "https://api.coinalyze.net/v1/future-markets"
headers = {

    "Accept": "application/json",
    'api_key':'62dcd7fc-85a0-4209-b654-8509a1509a06'
}

parameters = {
}
response = requests.get(url, headers=headers,params=parameters)
data=response.text
data_dict=json.loads(data)
perp_list_coinalyze = pd.DataFrame(data_dict)

## exchange list
url = "https://api.coinalyze.net/v1/exchanges"
headers = {

    "Accept": "application/json",
    'api_key':'62dcd7fc-85a0-4209-b654-8509a1509a06'
}

parameters = {
}
response = requests.get(url, headers=headers,params=parameters)
data=response.text
data_dict=json.loads(data)
exchange_list_coinalyze = pd.DataFrame(data_dict)

exchange_list_coinalyze.rename(columns={'code':"exchange"}, inplace=True)
perp_list_coinalyze = pd.merge(perp_list_coinalyze,exchange_list_coinalyze, how="left", on="exchange")
a=perp_list_coinalyze[perp_list_coinalyze['is_perpetual']=='True']
perp_list_coinalyze = perp_list_coinalyze.loc[perp_list_coinalyze['is_perpetual']==True,['symbol','base_asset','quote_asset','name']]
perp_list_coinalyze.rename(columns={'name':"exchange"}, inplace=True)


#################################### get funding rate on other cex
def datetime_to_unix_timestamp(dt):
    # Convert the datetime object to a timestamp in seconds
    unix_timestamp = int(dt.timestamp())
    return unix_timestamp

def hour8_to_apy(hourly_return):
    hours_per_year = 3 * 365
    if pd.isna(hourly_return):
        return None
    return (1 + hourly_return) ** hours_per_year - 1

timeRange_s = [datetime_to_unix_timestamp(dt_start),datetime_to_unix_timestamp(dt_end)]

def getFundingRate_coinAnlyze(symbol,timeRange):
    url = "https://api.coinalyze.net/v1/funding-rate-history"
    headers = {

        "Accept": "application/json",
        'api_key':'62dcd7fc-85a0-4209-b654-8509a1509a06'
    }

    parameters = {
        'symbols':symbol,
        'interval':'1hour',
        'from':timeRange[0],
        'to':timeRange[1],
    }

    response = requests.get(url, headers=headers,params=parameters)

    data=response.text
    data_dict=json.loads(data)
    FundingRate_his = (pd.DataFrame(data_dict[0]['history']))[['t', 'o']]
    FundingRate_his.rename(columns={'t': 'timestamp', 'o': 'funding_rate'}, inplace=True)
    FundingRate_his['funding_rate'] = FundingRate_his['funding_rate'] / (10 ** (2))
    FundingRate_his['funding_rate_apy'] = FundingRate_his['funding_rate'].apply(hour8_to_apy)
    return FundingRate_his



## find and combine perp of certain symbol from all supported exchanges

symbol_base='BTC'
symbol_quote='USD'
symbol_base_i=perp_list_coinalyze[(perp_list_coinalyze['base_asset']==symbol_base)&(perp_list_coinalyze['quote_asset']==symbol_quote)&(perp_list_coinalyze['exchange'].isin(['Binance','Bybit','OKX']))]
symbol_base_i.reset_index(drop=True, inplace=True)
symbol_error=[]
for index, row in symbol_base_i.iterrows():
    print(index)
    if index == 0:
        results_df=getFundingRate_coinAnlyze(row['symbol'],timeRange_s)
        results_df=results_df[['timestamp','funding_rate_apy']]
        results_df.rename(columns={'funding_rate_apy':row['exchange']},inplace=True)
    else:
        try:
            results_i=getFundingRate_coinAnlyze(row['symbol'],timeRange_s)
            results_i=results_i[['timestamp','funding_rate_apy']]
            results_i.rename(columns={'funding_rate_apy': row['exchange']},inplace=True)
            results_df=pd.merge(results_df,results_i,how='left',on='timestamp')
        except Exception as e:
            print(f"An error occurred with item {row['symbol']}: {e}")
            # Optionally, you can log the error or perform other actions
            symbol_error.append(row['symbol'])
            continue


## attach funding rates from Binance, Bybit okx to rabbitX
RabbitX_FR['timestamp']=RabbitX_FR['timestamp']/1_000_000
RabbitX_FR['timestamp']=RabbitX_FR['timestamp'].astype(int)
RabbitX_FR_otherCEX=pd.merge(RabbitX_FR,results_df,how='left',on='timestamp')

RabbitX_FR_otherCEX.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/RabbitX_FundingRate_'+pair+'.csv')


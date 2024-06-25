import requests
import pandas as pd
import io
import json


def Dune_dataGet(QueryNo):
    url = "https://api.dune.com/api/v1/query/" + str(QueryNo) + "/results/csv"
    headers = {"X-DUNE-API-KEY": "QRgJQX7PTo7pOh3V1u0Fjsawcs4IRrGw"}
    response = requests.request("GET", url, headers=headers)
    r = response.content
    return pd.read_csv(io.StringIO(r.decode("utf-8")))

def hour8_to_apy(hourly_return):
    hours_per_year = 3 * 365
    if pd.isna(hourly_return):
        return None
    return (1 + hourly_return/100) ** hours_per_year - 1

## get GMX data
QueryNo = 3112382
GMX_ArbitrumShort_FundingRates=Dune_dataGet(QueryNo)
# DuneData.to_csv('Rebalancing/CEX_address.csv')
GMX_ArbitrumShort_FundingRates.iloc[:,1:]=GMX_ArbitrumShort_FundingRates.iloc[:,1:].applymap(hour8_to_apy)
GMX_ArbitrumShort_FundingRates['time'] = pd.to_datetime(GMX_ArbitrumShort_FundingRates['time'])
GMX_ArbitrumShort_FundingRates['time'] = GMX_ArbitrumShort_FundingRates['time'].dt.strftime('%Y-%m-%d %H:%M')
GMX_ArbitrumShort_FundingRates.rename(columns={'time':'time_hour'},inplace=True)

GMX_ArbitrumShort_FundingRates.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/GMX_ArbitrumShort_FundingRates.csv')


## get Synthetix data
QueryNo = 3817770
Synthetix_FundingRates=Dune_dataGet(QueryNo)

Synthetix_FundingRates['time_hour'] = pd.to_datetime(Synthetix_FundingRates['time_hour'])
Synthetix_FundingRates['time_hour'] = Synthetix_FundingRates['time_hour'].dt.strftime('%Y-%m-%d %H:%M')

Synthetix_FundingRates.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/Synthetix_FundingRates.csv')

symbol_list=Synthetix_FundingRates['underlying_asset'].unique().tolist()


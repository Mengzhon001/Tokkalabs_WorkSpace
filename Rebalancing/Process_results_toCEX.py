import pandas as pd
from alive_progress import alive_bar
import numpy as np


tx_ERC20_CEX=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Tx_wintermute_ToCEX.csv', index_col=0)
tx_ERC20_CEX_detailed=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Tx_wintermute_ToCEX_detailed.csv', index_col=0)

tx_ERC20_CEX_detailed=tx_ERC20_CEX_detailed[['blockNumber','timeStamp','hash','from','to','contractAddress','tokenName','tokenSymbol','value','tokenDecimal','gas','gasPrice','gasUsed']]
tx_ERC20_CEX=tx_ERC20_CEX[['evt_tx_hash','evt_block_time','cex_name','distinct_name']]
tx_ERC20_CEX.rename(columns={'evt_tx_hash':'hash'},inplace=True)
tx_ERC20_CEX_detailed_cexIndicated=pd.merge(tx_ERC20_CEX_detailed,tx_ERC20_CEX,how='inner',on='hash')

# calculate gas fee
tx_ERC20_CEX_detailed_cexIndicated['gasFee']=tx_ERC20_CEX_detailed_cexIndicated['gasPrice']*tx_ERC20_CEX_detailed_cexIndicated['gasUsed']*10**(-18)

import requests
import json

def getSymbol(contractAddress):
    # contractAddress='0xdac17f958d2ee523a2206206994597c13d831ec7'
    url_1='https://data-api.cryptocompare.com/onchain/v1/data/by/address?address='
    url_2='&chain_symbol=ETH&api_key=0b640570752da036f31a0d7ea31a1ba6cf9ecf6a06ec5cdd1d003f554bd55caa'
    url=url_1+str(contractAddress)+url_2
    headers = {
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    data = response.text
    data_dict = json.loads(data)
    symbol=data_dict['Data']['SYMBOL']
    return symbol

def TradingHistory(symbol):
    url_1='https://min-api.cryptocompare.com/data/v2/histoday?fsym='
    url_2='&tsym=USD&allData=1&api_key=0b640570752da036f31a0d7ea31a1ba6cf9ecf6a06ec5cdd1d003f554bd55caa'
    url=url_1+str(symbol)+url_2
    headers = {
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    data = response.text
    data_dict = json.loads(data)
    return data_dict
pass

# contractAddress='0xdac17f958d2ee523a2206206994597c13d831ec7'
# symbol=getSymbol(contractAddress)
# PricingData=TradingHistory(symbol)
# Data_OHLC=pd.DataFrame.from_dict(PricingData['Data']['Data'])

import datetime

# Get the range of date to retrieve
def timestampMidnight(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
    midnight_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight_dt.timestamp())

tx_ERC20_CEX_detailed_cexIndicated['timeStamp_midnight']=tx_ERC20_CEX_detailed_cexIndicated['timeStamp'].apply(timestampMidnight)
timestamp_min = tx_ERC20_CEX_detailed_cexIndicated['timeStamp_midnight'].min()

PricingData_ETH=TradingHistory('ETH')
Data_OHLC_ETH=pd.DataFrame.from_dict(PricingData_ETH['Data']['Data'])
Data_OHLC_ETH=Data_OHLC_ETH.loc[Data_OHLC_ETH['time']>=timestamp_min,['time','open']]
Data_OHLC_ETH.rename(columns={'open':'ETH'},inplace=True)
PricingTable=Data_OHLC_ETH

# loop through the ERC20 contract involved in the tx
contract_list = tx_ERC20_CEX_detailed_cexIndicated['contractAddress'].unique().tolist()
contract_pricing_error=[]

with alive_bar(len(contract_list), force_tty=True) as bar:
    for contract_i in contract_list:
        bar()
        try:
            symbol_i = getSymbol(contract_i)
            PricingData_i = TradingHistory(symbol_i)
            Data_OHLC_i = pd.DataFrame.from_dict(PricingData_i['Data']['Data'])
            Data_OHLC_i = Data_OHLC_i.loc[Data_OHLC_i['time'] >= timestamp_min, ['time', 'open']]
            Data_OHLC_i.rename(columns={'open': contract_i}, inplace=True)
            PricingTable = pd.merge(PricingTable, Data_OHLC_i, how='left', on='time')
        except Exception as e:
            # Handle the exception (e.g., print an error message)
            print(f"An error occurred with item {contract_i}: {e}")
            # Optionally, you can log the error or perform other actions
            contract_pricing_error.append(contract_i)
            continue


PricingTable.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/PricingTable.csv')
# '0xbe9375c6a420d2eeb258962efb95551a5b722803' migrate to '0xa62cc35625B0C8dc1fAEA39d33625Bb4C15bD71C'
# contract addresses with error ['0xa3ee21c306a700e682abcdfe9baa6a08f3820419', '0x15d4c048f83bd7e37d49ea4c83a07267ec4203da', '0xbe9375c6a420d2eeb258962efb95551a5b722803', '0x4fe83213d56308330ec302a8bd641f1d0113a4cc']
'''
for toCEX tx
'0xa3ee21c306a700e682abcdfe9baa6a08f3820419' traded as CTC
'0xbe9375c6a420d2eeb258962efb95551a5b722803' migrate to '0xa62cc35625B0C8dc1fAEA39d33625Bb4C15bD71C' (STMX)
'0x15d4c048f83bd7e37d49ea4c83a07267ec4203da' migrate to GALA
'0x4fe83213d56308330ec302a8bd641f1d0113a4cc' is proccessed manually (nu-usd_pricing.csv)
'''
def ManualPricing_attach(contract_i,symbol_i,PricingTable):
    PricingData_i = TradingHistory(symbol_i)
    Data_OHLC_i = pd.DataFrame.from_dict(PricingData_i['Data']['Data'])
    Data_OHLC_i = Data_OHLC_i.loc[Data_OHLC_i['time'] >= timestamp_min, ['time', 'open']]
    Data_OHLC_i.rename(columns={'open': contract_i}, inplace=True)
    PricingTable = pd.merge(PricingTable, Data_OHLC_i, how='left', on='time')
    return PricingTable
PricingTable=ManualPricing_attach('0xa3ee21c306a700e682abcdfe9baa6a08f3820419','CTC',PricingTable)
PricingTable=ManualPricing_attach('0xbe9375c6a420d2eeb258962efb95551a5b722803','STMX',PricingTable)
PricingTable=ManualPricing_attach('0x15d4c048f83bd7e37d49ea4c83a07267ec4203da','GALA',PricingTable)

PricingData_nu=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/nu-usd_pricing.csv',index_col=0)
PricingData_nu['time_UTC']=PricingData_nu.index
PricingData_nu['time_UTC'] = pd.to_datetime(PricingData_nu['time_UTC'], utc=True)
PricingData_nu['time'] = PricingData_nu['time_UTC'].astype(int) // 10**9
PricingData_nu=PricingData_nu[['time','price']]
PricingData_nu.reset_index(drop=True, inplace=True)
PricingData_nu.rename(columns={'price':'0x4fe83213d56308330ec302a8bd641f1d0113a4cc'},inplace=True)
PricingTable=pd.merge(PricingTable,PricingData_nu,how='left',on='time')
PricingTable.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/PricingTable_toCEX.csv')

## attach token price to each transaction
tx_ERC20_CEX_detailed_cexIndicated['TokenPrice_traded']=np.nan
tx_ERC20_CEX_detailed_cexIndicated['ETH-USD']=np.nan
tx_ERC20_CEX_detailed_cexIndicated['Volume']=np.nan
for index, row in tx_ERC20_CEX_detailed_cexIndicated.iterrows():
    print(str(index))
    tx_ERC20_CEX_detailed_cexIndicated.at[index,'TokenPrice_traded']=PricingTable.loc[PricingTable['time']==row['timeStamp_midnight'],row['contractAddress']]
    tx_ERC20_CEX_detailed_cexIndicated.at[index,'ETH-USD']=PricingTable.loc[PricingTable['time']==row['timeStamp_midnight'],'ETH']
    tx_ERC20_CEX_detailed_cexIndicated.at[index,'Volume']=(float(row['value'])/float((10**row['tokenDecimal'])))*tx_ERC20_CEX_detailed_cexIndicated.at[index,'TokenPrice_traded']

tx_ERC20_CEX_detailed_cexIndicated['gasFee_inUSD']=tx_ERC20_CEX_detailed_cexIndicated['gasFee']*tx_ERC20_CEX_detailed_cexIndicated['ETH-USD']
tx_ERC20_CEX_detailed_cexIndicated['gasFee_percentage_bp']=(tx_ERC20_CEX_detailed_cexIndicated['gasFee_inUSD']/tx_ERC20_CEX_detailed_cexIndicated['Volume'])/0.0001
tx_ERC20_CEX_detailed_cexIndicated.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Wintermute_toCEX_results.csv.csv')
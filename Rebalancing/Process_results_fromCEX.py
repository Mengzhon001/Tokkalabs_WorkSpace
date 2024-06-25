import pandas as pd
from alive_progress import alive_bar
import numpy as np

## concat the data (original data in splited files have already been deleted)
# Tx_df_fromCEX_list=['0_5000','5000_10000','10000_15000','15000_20000','20000_end']
# results=[]
# for chunk_i in Tx_df_fromCEX_list:
#     data_i=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Tx_wintermute_FromCEX_detailed_'+chunk_i+'.csv',index_col=0)
#     results.append(data_i)
#
# Tx_wintermute_fromCEX_detailed=pd.concat(results, ignore_index=True)
# Tx_wintermute_fromCEX_detailed.to_csv('Rebalancing/Tx_wintermute_FromCEX_detailed.csv')

tx_ERC20_CEX=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Tx_wintermute_FromCEX.csv', index_col=0)
tx_ERC20_CEX_detailed=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Tx_wintermute_FromCEX_detailed.csv', index_col=0)

tx_ERC20_CEX_detailed=tx_ERC20_CEX_detailed[['blockNumber','timeStamp','hash','from','to','contractAddress','tokenName','tokenSymbol','value','tokenDecimal','gas','gasPrice','gasUsed']]
tx_ERC20_CEX=tx_ERC20_CEX[['evt_tx_hash','evt_block_time','cex_name','distinct_name']]
tx_ERC20_CEX.rename(columns={'evt_tx_hash':'hash'},inplace=True)
tx_ERC20_CEX_detailed_cexIndicated=pd.merge(tx_ERC20_CEX_detailed,tx_ERC20_CEX,how='inner',on='hash')

# calculate gas fee
tx_ERC20_CEX_detailed_cexIndicated['gasFee']=tx_ERC20_CEX_detailed_cexIndicated['gasPrice']*tx_ERC20_CEX_detailed_cexIndicated['gasUsed']*10**(-18)

# fn need for pricing
import datetime
import requests
import json
def timestampMidnight(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
    midnight_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight_dt.timestamp())

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



tx_ERC20_CEX_detailed_cexIndicated['timeStamp_midnight']=tx_ERC20_CEX_detailed_cexIndicated['timeStamp'].apply(timestampMidnight)
timestamp_min = tx_ERC20_CEX_detailed_cexIndicated['timeStamp_midnight'].min()

# for pricing table
contract_list = tx_ERC20_CEX_detailed_cexIndicated['contractAddress'].unique().tolist()

PricingData_ETH=TradingHistory('ETH')
Data_OHLC_ETH=pd.DataFrame.from_dict(PricingData_ETH['Data']['Data'])
Data_OHLC_ETH=Data_OHLC_ETH.loc[Data_OHLC_ETH['time']>=timestamp_min,['time','open']]
Data_OHLC_ETH.rename(columns={'open':'ETH'},inplace=True)
PricingTable=Data_OHLC_ETH

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

'''
part1:
on 48: An error occurred with item 0xbef26bd568e421d6708cca55ad6e35f8bfa0c406: bcut-usd_pricing.csv
on 72: An error occurred with item 0x09395a2a58db45db0da254c7eaa5ac469d8bdc85: sqt-usd_pricing.csv
on 84: An error occurred with item 0xc71b5f631354be6853efe9c3ab6b9590f8302e81: zkj-usd_pricing.csv
on 125: An error occurred with item 0x5de597849cf72c72f073e9085bdd0dadd8e6c199: fbx-usd_pricing.csv
on 129: An error occurred with item 0x6468e79a80c0eab0f9a2b574c8d5bc374af59414: exrd-usd_pricing.csv
on 134: An error occurred with item 0x4fe83213d56308330ec302a8bd641f1d0113a4cc: nu-usd_pricing.csv
on 160: An error occurred with item 0x0493bd231513003122ac861e335367bf78e553f0: indi-usd_pricing.csv
on 164: An error occurred with item 0x43f11c02439e2736800433b4594994bd43cd066d: floki-usd_pricing.csv
part2:
on 85: An error occurred with item 0xa3ee21c306a700e682abcdfe9baa6a08f3820419: traded as CTC
on 126: An error occurred with item 0x15d4c048f83bd7e37d49ea4c83a07267ec4203da: migrate to GALA
on 154: An error occurred with item 0xbe9375c6a420d2eeb258962efb95551a5b722803: migrate to '0xa62cc35625B0C8dc1fAEA39d33625Bb4C15bD71C' (STMX)
part3:
on 116: An error occurred with item 0x866ec9652fa462f17f89684f8cc5297e0e438065: paxos.gift (Airdrop) - exclude
on 139: An error occurred with item 0x0d3716e3e411af431a6e87e715d4b05bbcd67000: Token $ NFTGiftX.com (Airdrop) - exclude
on 142: An error occurred with item 0xab382babca94392736915278f30b181b4e51bd53: BUSDBonus.com (Airdrop) - exclude
'''
## for part2
mapping_address_to_dataFile={
    '0xbef26bd568e421d6708cca55ad6e35f8bfa0c406': 'bcut-usd_pricing.csv',
    '0x09395a2a58db45db0da254c7eaa5ac469d8bdc85': 'sqt-usd_pricing.csv',
    '0xc71b5f631354be6853efe9c3ab6b9590f8302e81': 'zkj-usd_pricing.csv',
    '0x5de597849cf72c72f073e9085bdd0dadd8e6c199': 'fbx-usd_pricing.csv',
    '0x6468e79a80c0eab0f9a2b574c8d5bc374af59414': 'exrd-usd_pricing.csv',
    '0x4fe83213d56308330ec302a8bd641f1d0113a4cc': 'nu-usd_pricing.csv',
    '0x0493bd231513003122ac861e335367bf78e553f0': 'indi-usd_pricing.csv',
    '0x43f11c02439e2736800433b4594994bd43cd066d': 'floki-usd_pricing.csv'
}

def PricingAttach_file(contractAddress, file_name,PricingTable):

    PricingData_nu = pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/'+file_name,
                                 index_col=0)
    PricingData_nu['time_UTC'] = PricingData_nu.index
    PricingData_nu['time_UTC'] = pd.to_datetime(PricingData_nu['time_UTC'], utc=True)
    PricingData_nu['time'] = PricingData_nu['time_UTC'].astype(int) // 10 ** 9
    PricingData_nu = PricingData_nu[['time', 'price']]
    PricingData_nu.reset_index(drop=True, inplace=True)
    PricingData_nu.rename(columns={'price': contractAddress}, inplace=True)
    PricingTable = pd.merge(PricingTable, PricingData_nu, how='left', on='time')
    return PricingTable

for key, value in mapping_address_to_dataFile.items():
    PricingTable=PricingAttach_file(key,value,PricingTable)

## for part2
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


## for part3
PricingTable['0x866ec9652fa462f17f89684f8cc5297e0e438065']=np.nan
PricingTable['0x0d3716e3e411af431a6e87e715d4b05bbcd67000']=np.nan
PricingTable['0xab382babca94392736915278f30b181b4e51bd53']=np.nan

PricingTable.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/PricingTable_fromCEX.csv')

##
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
tx_ERC20_CEX_detailed_cexIndicated.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Wintermute_fromCEX_results.csv.csv')



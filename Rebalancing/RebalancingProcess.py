from Rebalancing.Dune_data_get import Dune_dataGet
from Rebalancing.Etherscan_data_get import ERC20TransferTx
import pandas as pd
import time
# from alive_progress import alive_bar

# fetch tx by Wintermute address
AddressToDetect = '0xdbf5e9c5206d0db70a90108bf936da60221dc080'
Tx_wintermute=Dune_dataGet(3803627)
Tx_wintermute=Tx_wintermute[Tx_wintermute['blockchain']=='ethereum']
Tx_wintermute.to_csv('Rebalancing/WintermuteTx.csv')
Tx_wintermute=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/WintermuteTx.csv',index_col=0)

# fetch CEX address from Dune
DuneData=Dune_dataGet(3803744)
DuneData.to_csv('Rebalancing/CEX_address.csv')
CEX_address=pd.read_csv('Rebalancing/CEX_address.csv',index_col=0)
CEX_address=CEX_address[CEX_address['blockchain']=='ethereum']

CEX_address_forFrom=CEX_address[['address','cex_name','distinct_name']]
CEX_address_forFrom.rename(columns={'address':'from'},inplace=True)

CEX_address_forTo=CEX_address[['address','cex_name','distinct_name']]
CEX_address_forTo.rename(columns={'address':'to'},inplace=True)

# merge with tx with cex address list
Tx_wintermute_FromCEX=pd.merge(Tx_wintermute,CEX_address_forFrom,how='inner',on='from')
Tx_wintermute_ToCEX=pd.merge(Tx_wintermute,CEX_address_forTo,how='inner',on='to')

Tx_wintermute_FromCEX.to_csv('Rebalancing/Tx_wintermute_FromCEX.csv')
Tx_wintermute_ToCEX.to_csv('Rebalancing/Tx_wintermute_ToCEX.csv')




# # get the unique value of contract and the range of blocks
# contract_list_FromCEX=Tx_wintermute_FromCEX.contract_address.unique().tolist()
#
#
# Block_number_max=Tx_wintermute.evt_block_number.max()
# Block_number_min=Tx_wintermute.evt_block_number.min()
#
# # loop through the contract list for thr token transfer
# Tx_toProccess = Tx_wintermute_ToCEX.iloc[0:100,:]
#
# # def FetchERC20Tx_details(Tx_toProcess_chunk)
# Tx_total=pd.DataFrame()
# # Number of times to execute the function per second (rate limit)
# max_calls_per_second = 5
# # Minimum time interval between calls in seconds
# interval = 1 / max_calls_per_second
#
# # with alive_bar(len(Tx_toProccess), force_tty=True) as bar:
# for index,row in Tx_toProccess.iterrows():
#     # bar()
#     start_time = time.time() # count time
#
#     APIkey= 'BJ3446K621AECIYKDPFU1HXJ629NUU6Z7N'
#     walletAddress= AddressToDetect
#     contractAddress= row['contract_address']
#     startblock = int(row['evt_block_number'])
#     endblock = int(row['evt_block_number'])
#
#     data_contract_i=ERC20TransferTx(APIkey,
#                             walletAddress,
#                             contractAddress,
#                             startblock,
#                             endblock,
#                             sort='desc',
#                             page=1,
#                             offset=500
#                             )
#     ResultPd_contract_i=pd.DataFrame(data_contract_i['result'])
#     Tx_total=pd.concat([Tx_total,ResultPd_contract_i])
#
#     elapsed_time = time.time() - start_time
#     sleep_time = max(0, interval - elapsed_time)
#     time.sleep(sleep_time)
#     pass
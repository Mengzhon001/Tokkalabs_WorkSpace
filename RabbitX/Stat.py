import matplotlib.pyplot as plt
import pandas as pd
'''
ETH ARB BTC DOGE LINK SOL XRP
'''
symbol_base='DOGE'
symbol_quote='USD'
pair=symbol_base+'-'+symbol_quote

# Sample data
data=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/RabbitX_FundingRate_'+pair+'.csv',index_col=0)
data=data[['funding_rate_apy','Binance','Bybit','OKX','dYdX','Synthetix']]

mean_values = data.mean()


# Calculate the standard deviation of the four columns (excluding the date column)
std_values = data.std()

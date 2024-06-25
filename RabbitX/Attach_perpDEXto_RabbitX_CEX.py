import pandas as pd

symbol_base='ETH'
symbol_quote='USD'
pair=symbol_base+'-'+symbol_quote

## attach GMX
GMX_ArbitrumShort_FundingRates = pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/GMX_ArbitrumShort_FundingRates.csv',index_col=0)


RabbitX_CEX=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/RabbitX_FundingRate_'+pair+'.csv',index_col=0)

RabbitX_CEX=pd.merge(RabbitX_CEX,GMX_ArbitrumShort_FundingRates[['time_hour',symbol_base]],how='left',on='time_hour')
RabbitX_CEX.rename(columns={symbol_base:'dYdX'},inplace=True)
RabbitX_CEX.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/RabbitX_FundingRate_'+pair+'.csv')

## attach Synthstix
Synthetix_fundingRate = pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/Synthetix_FundingRates.csv',index_col=0)


RabbitX_CEX=pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/RabbitX_FundingRate_'+pair+'.csv',index_col=0)

Synthetix_fundingRate_i=Synthetix_fundingRate.loc[Synthetix_fundingRate['underlying_asset']==symbol_base,['time_hour','AnnualfundingRate']]
Synthetix_fundingRate_i.rename(columns={'AnnualfundingRate':'Synthetix'},inplace=True)
RabbitX_CEX=pd.merge(RabbitX_CEX,Synthetix_fundingRate_i,how='left',on='time_hour')
RabbitX_CEX.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/RabbitX/RabbitX_FundingRate_'+pair+'.csv')


import requests
import pandas as pd


def ERC20TransferTx(APIkey,
                    walletAddress,
                    contractAddress,
                    startblock,
                    endblock,
                    sort='desc',
                    page=1,
                    offset=1000,
                    ):
   url = 'https://api.etherscan.io/api' \
         '?module=account' \
         '&action=tokentx' \
         '&apikey='+APIkey

   # # Parameters for the request
   params = {"address": walletAddress,
             "contractaddress": contractAddress,
             'page': page,
             'offset': offset,
             'startblock': int(startblock),
             'endblock': int(endblock),
             'sort': sort
             }

   # Make the GET request
   response = requests.get(url, params=params)
   data = response.json()
   return data



# APIkey= 'BJ3446K621AECIYKDPFU1HXJ629NUU6Z7N'
# walletAddress= '0xdbf5e9c5206d0db70a90108bf936da60221dc080'
# contractAddress= '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
# startblock = int(11649124.0)
# endblock = int(20031436.0)
# sort='desc'
# page=1
# offset=1000
#
# # ResultPd=pd.DataFrame(data['result'])
#
# data_results=ERC20TransferTx(APIkey,
#                     walletAddress,
#                     contractAddress,
#                     startblock,
#                     endblock,
#                     sort='desc',
#                     page=1,
#                     offset=1000,
#                     )


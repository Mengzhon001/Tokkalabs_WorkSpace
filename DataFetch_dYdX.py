from dydx3 import Client
import requests

url = "https://api.dydx.exchange/v3/historical-funding/:market"
# headers = {
#     "Accept": "application/json"
# }

parameters = {
    'market': 'BTC-USD'
}

response = requests.get(url, params=parameters)

data = response.text

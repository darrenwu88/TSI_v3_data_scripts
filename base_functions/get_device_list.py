'''
Author: Darren Wu
Date: 12/5/2022 

Takes token as input and outputs list of TSI devices associated with the respective account

PARAMS:
token = oauth token
country = country that devices are associated with
'''

import requests
import os

### TO CHANGE
token = '3251LzQBmmZfE2dDrqL8wJGl3GHp'
country = 'Duke'

#Query PARAMS
#https://developers.tsilink.com/docs/tsi-external-api/1/routes/devices/legacy-format/get

def device_list(token, country):
  requestUrl = "https://api-prd.tsilink.com/api/v3/external/devices/legacy-format?include_shared=true"
  requestHeaders = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
  }

  response = requests.get(requestUrl, headers=requestHeaders)

  with open(os.path.join(r'./device_list_by_country', f'{country}_device_list.json'), "w") as outfile:
    outfile.write(response.text)

if __name__ == "__main__":
  device_list(token, country)
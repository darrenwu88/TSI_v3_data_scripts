'''
Author: Darren Wu
Date: 12/5/2022 

Takes token as input and outputs flat telemetry from the TSI server per requestUrl query

Note that this script file is extremely boilerplate, and not "automated". This is just for proof of concept. 
For the automated version, check out level0.py (the combined script which intergrates all the base functions together)

###IMPORTANT
Can only request telemetry of ONE device at a time, otherwise server throws error

PARAMS:
token_json_file: name of json file with token
start_date = data start point
end_date = data end point
'''

import requests
import os
import json

#USER PARAMS
token_json_file = '(mike.bergin@duke.edu)_token.json'

#Query PARAMS
device_id = 'c50fv8u7kn8vcc9cd6cg'
start_date = '2022-12-02T15:00:00Z'
end_date = '2022-12-22T15:00:00Z'

#More params here, keeping it simple for now
#https://developers.tsilink.com/docs/tsi-external-api/1/routes/telemetry/flat-format/get

def get_telemetry_flat(token_json_file, device_id, start_date, end_date) -> None:
  
  #initialize PATH where token for user is located
  token_PATH = fr'./client_tokens/{token_json_file}'
  json_file = open(token_PATH)
  #get data inside json file
  data = json.load(json_file)
    
  token = data['access_token']


  requestUrl = f"https://api-prd.tsilink.com/api/v3/external/telemetry/flat-format?device_id={device_id}&start_date={start_date}&end_date={end_date}&telem[]=location&telem[]=is_indoor&telem[]=mcpm1x0&telem[]=ncpm1x0&telem[]=tpsize&telem[]=temperature&telem[]=rh"
  requestHeaders = {
    "Accept": "application/json",
    "Authorization": f"Bearer {token}"
  }

  response = requests.get(requestUrl, headers=requestHeaders)

  #format of output file: device_id + start_date + end_date
  with open(os.path.join(r'./flat_telemetry_json', f'{device_id}_{start_date}_{end_date}.json'), "w") as outfile:
    outfile.write(response.text)

if __name__ == "__main__":
  get_telemetry_flat(token_json_file, device_id, start_date, end_date)
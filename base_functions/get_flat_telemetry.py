'''
Author: Darren Wu
Date: 12/5/2022 

Takes token as input and outputs flat telemetry from the TSI server per requestUrl query

PARAMS:
token = oauth token
start_date = data start point
end_date = data end point
'''

import requests
import os

### TO CHANGE
token = 'jOEiYd8iKpaDTmMrrN2OSpDdsfuD'

#Query PARAMS
device_id = 'c50fv8u7kn8vcc9cd6cg'
start_date = '2022-12-02T15:00:00Z'
end_date = '2022-12-03T15:00:00Z'

#More params here, keeping it simple for now
#https://developers.tsilink.com/docs/tsi-external-api/1/routes/telemetry/flat-format/get

def get_telemetry_flat(token, device_id, start_date, end_date) -> None:
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
  get_telemetry_flat(token, device_id, start_date, end_date)
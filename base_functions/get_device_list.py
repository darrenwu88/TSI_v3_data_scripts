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
import pandas as pd
import json

#library for finding country
import reverse_geocode

### TO CHANGE
token = 'jOEiYd8iKpaDTmMrrN2OSpDdsfuD'
country = 'Duke'

#Query PARAMS
#https://developers.tsilink.com/docs/tsi-external-api/1/routes/devices/legacy-format/get

def shorten_name(friendly_name) -> str:
    return ''
    
    #de Foy's code
    '''
        stid = tolower(stname)
        stid = str_replace_all(stid,'[.,\\\'() /-]+','_')
        stid = str_replace_all(stid,'(?i)_of_','_')
        stid = str_replace_all(stid,'(?i)university','u')
        stid = str_replace_all(stid,'(?i)univ','u')
        stid = str_replace_all(stid,'(?i)airport','apt')
        stid = str_replace_all(stid,'(?i)campus','')
        stid = str_replace_all(stid,'(?i)school','')
        stid = str_replace_all(stid,'_+$','')
        stid = str_replace_all(stid,'^_+','')
        stid = str_replace_all(stid,'_+','_')
        if ( str_detect(stid,sprintf('^(?i)%s',stctry)) ) {
        } else {
        stid = sprintf('%s_%s',tolower(stctry),stid)
    '''
    
def get_country(coords) -> str:
    #search requires list input
    location = reverse_geocode.search(coords)
    country = location[0]['country']

    return country

def append_device_list(response_json) -> None:
    #open master_device_list in subdirectory
    df = pd.read_csv(os.path.join(r'./master_device_list', 'master_device_list.csv'))
    
    for device in response_json:
        cloud_account_id = device['account_id']
        cloud_device_id = device['device_id']
        serial_number = device['serial']
        friendly_name = device['metadata']['friendlyName']

        #list input
        coords = [(float(device['metadata']['longitude']), float(device['metadata']['latitude']))]

        #get country based off coords
        country = get_country(coords)
        
        #shorten friendly_name using shorten_name() subroutine
        short_name = shorten_name(friendly_name)

        #new device to be inserted into csv
        insert_row = {
            'cloud_account_id': cloud_account_id,
            'cloud_device_id': cloud_device_id,
            'serial_number': serial_number,
            'country': country,
            'friendly_name': friendly_name,
            'short_name': short_name
        }

        #append new row for new device if it doesn't exist
        if cloud_device_id not in df['cloud_device_id'].values:
            df = pd.concat([df, pd.DataFrame([insert_row])])
    
    #overwrite old csv file
    df.to_csv(os.path.join(r'./master_device_list', 'master_device_list.csv'), index = False)


def device_list(token, country) -> None:
    requestUrl = "https://api-prd.tsilink.com/api/v3/external/devices/legacy-format?include_shared=true"
    requestHeaders = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
    }

    response = requests.get(requestUrl, headers=requestHeaders)

    append_device_list(response.json())

    with open(os.path.join(r'./device_list_by_country', f'{country}_device_list.json'), "w") as outfile:
        outfile.write(response.text)

if __name__ == "__main__":
    device_list(token, country)
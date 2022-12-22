'''
Author: Darren Wu
Date: 12/5/2022 

device_list()
Takes token as input and outputs list of TSI devices associated with the respective account

PARAMS/INPUTS:
token_json_file: name of json file with token
'''

import requests
import os
import pandas as pd
import json
#library for finding country
import reverse_geocode

#PARAMS
token_json_file = '(mike.bergin@duke.edu)_token.json'

#Query PARAMS
#https://developers.tsilink.com/docs/tsi-external-api/1/routes/devices/legacy-format/get

#shorten name based off de Foy's rules
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

#get country based of coords
def get_country(coords) -> str:
    #search requires list input
    location = reverse_geocode.search(coords)
    country = location[0]['country']

    return country

#append new countries to master device list
def append_device_list(response_json, dev_email) -> None:
    #open master_device_list in subdirectory
    df = pd.read_csv(os.path.join(r'./master_device_list', 'master_device_list.csv'))
    
    for device in response_json:
        cloud_account_id = device['account_id']
        cloud_device_id = device['device_id']
        serial_number = device['serial']
        friendly_name = device['metadata']['friendlyName']

        #list input
        coords = [(float(device['metadata']['latitude']), float(device['metadata']['longitude']))]

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


def device_list(token_json_file) -> None:

    #get token from json file
    token_PATH = fr'./client_tokens/{token_json_file}'
    json_file = open(token_PATH)
    #get data inside json file
    data = json.load(json_file)
    
    token = data['access_token']

    #request v3 data using token
    requestUrl = "https://api-prd.tsilink.com/api/v3/external/devices/legacy-format?include_shared=true"
    requestHeaders = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
    }

    response = requests.get(requestUrl, headers=requestHeaders)
    response_json = response.json()

    #get developer email for output file naming schema
    dev_email = data['developer.email']

    append_device_list(response_json, dev_email)

    #we want the output file to be a device list (identifiable by dev email), format = '(DEVELOPER EMAIL)_device_list.json'
    with open(os.path.join(r'./device_list_by_developer_user', f'{dev_email}_device_list.json'), "w") as outfile:
        outfile.write(response.text)

if __name__ == "__main__":
    device_list(token_json_file)
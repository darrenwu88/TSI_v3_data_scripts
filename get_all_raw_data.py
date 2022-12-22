'''
Author: Darren Wu
Date: 12/5/2022 

This script integrates all the base functions together, allowing the user to access all the raw data across all 
their devices in the accounts in the form of a csv file output.

Checkout main() function for the main body of the script

PARAMS:
data_start_date: data start range
data_end_date: data end range
database: dictionary of key/secret pairs (right now we only have one (mike's account))
'''

import requests
import os
import json
import glob 
import pandas as pd
import reverse_geocode


client_key = 'ZEMIbhqwCe7MIVfGeq1pNA9nqAGpvpcVuaw9XEEmRXtgGt1I'
client_secret = 'ayuIiRntou61plHwEtasfC4HnxBqG02svGGhaUSszGVBm9PRvn0yNWgUAq5UwpJN'

#If a user has multiple keys/secrets, I was thinking of keeping that data in a dictionary
#and feed the dictionary into the function so it can take in all the keys/secrets
#PARAMS
database = {client_key: client_secret}
start_date = '2022-12-02T15:00:00Z'
end_date = '2022-12-22T15:00:00Z'

#Base functions 
def client_token(client_key, client_secret) -> None:

    headers = {
        'Accept': 'application/json',
    }

    params = {
        'grant_type': 'client_credentials',
    }

    data = {
        'client_id': client_key,
        'client_secret': client_secret,
    }

    response = requests.post(
        'https://api-prd.tsilink.com/api/v3/external/oauth/client_credential/accesstoken',
        params=params,
        headers=headers,
        data=data,
    )

    data = response.json()
    #create output token filename, format = '(DEVELOPER_EMAIL)_token.json'
    dev_email = data['developer.email']
    output_token_filename = f'({dev_email})_token.json'

    with open(os.path.join(r'./client_tokens', output_token_filename), 'w') as f:
        json.dump(data, f)

def shorten_name(friendly_name) -> str:
    return ''
    
    #de Foy's code

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
            'short_name': short_name,
            'dev_email': dev_email
        }

        #append new row for new device if it doesn't exist
        if cloud_device_id not in df['cloud_device_id'].values:
            df = pd.concat([df, pd.DataFrame([insert_row])])
    
    #overwrite old csv file
    df.to_csv(os.path.join(r'./master_device_list', 'master_device_list.csv'), index = False)

def device_list(token_json_file) -> None:

    #get token from json file
    token_PATH = token_json_file
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

def flatten_json(json_file):
    raw_json = open(json_file, encoding = 'utf-8', errors = 'ignore')
    flat_json = json.load(raw_json)
    #normalize json
    raw_df = pd.json_normalize(flat_json)

    file_PATH = str(json_file)
    #remove directory from file_PATH to get file_name (./flat_telemetry_json/) and .json tag
    file_name = file_PATH[22:-5]

    #dump csv into directory
    raw_df.to_csv(os.path.join(r'./flat_telemetry_csv_RAW', f'{file_name}.csv'), index = False)

###MAIN BODY FUNCTION
def main(database, start_date, end_date) -> None:

    #iterate through each key/secret pair and get the respective token (if user has access to multiple TSI accounts)
    for client_key in database:
        
        client_key  = client_key
        client_secret = database[client_key]

        client_token(client_key, client_secret)

        #notification that specific client token has been successfully received
        print(f'Received token from client key {client_key}')

    #iterate through token jsons in /client_tokens to get devices from each token and update master_device_list
    
    #where all the token jsons are located
    token_jsons_PATHS = os.path.join(r'./client_tokens', '*.json')

    #returns list of all the files located in the /client_tokens directory so we can iterate through them
    joined_token_jsons = glob.glob(token_jsons_PATHS)
    
    #iterate
    for token_json in joined_token_jsons:
        #call device_list on each token json file
        device_list(token_json)
    
    #notification that master_device_list.csv is fully updated
    print('Device list is successfully updated with all devices.')

    #get flat telemetry from all devices using flat_telemetry()
    #only able to call flat_telemetry() one device at a time
    device_list_df = pd.read_csv(os.path.join(r'./master_device_list', 'master_device_list.csv'))
    
    #iterate through each row in device_list_df to get info for each device
    for index, row in device_list_df.iterrows():
        #get necessary query PARAMS to call telemetry_flat()
        dev_email = row['dev_email']

        device_id = row['cloud_device_id']
        token_json_file = f'({dev_email})_token.json'

        #dump json files for each device in /flat_telemetry_json
        get_telemetry_flat(token_json_file, device_id, start_date, end_date)
    
    print('Device telemetry jsons have been successfully dumped into /flat_telemetry_json')

    #convert all telemetry jsons to csv files using json decoder function
    telemetry_jsons_PATHS = os.path.join(r'./flat_telemetry_json', '*.json')
    
    #returns list of all the files located in the /client_tokens directory so we can iterate through them
    joined_telemetry_jsons = glob.glob(telemetry_jsons_PATHS)

    for telemetry_json in joined_telemetry_jsons:
        #use json decoder function to convert telemetry_json to csv file

        #flattens json and dumps output .csv file into /flat_telemetry_csv_RAW
        flatten_json(telemetry_json)

    print('All device telemetry jsons have been successfully converted to .csv files and dumped into /flat_telemetry_csv_RAW')

    #merge all the csv files in /flat_telemetry_csv_RAW together in one big raw .csv file
    telemetry_csvs_PATHS = os.path.join(r'./flat_telemetry_csv_RAW', '*.csv')

    joined_telemetry_csvs = glob.glob(telemetry_csvs_PATHS)
    
    #concatenate all telemetry_csv(s) together using map function across the indiv. csv directory
    combined_csv = pd.concat(map(lambda csv_file: pd.read_csv(csv_file), joined_telemetry_csvs), ignore_index = True)

    #output merged csv
    combined_csv.to_csv('output_raw_telemetry.csv', index = False)
    print('Merged raw csv successfully compiled')

if __name__ == "__main__":
    main(database, start_date, end_date)
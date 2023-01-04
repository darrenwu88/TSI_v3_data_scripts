'''
Author: Darren Wu
Date: 12/5/2022 

This script integrates all the base functions together, allowing the user to access all the raw data across all 
their devices in the accounts in the form of a csv file output.

Checkout main() function for the main body of the script

PARAMS:
data_start_date: data start range
data_end_date: data end range
secrets_PATH: PATH for secrets.csv (all developer key/secret pairs)

WORKFLOW DOC: (will be updated as we make edits to the script)

Script job: Get, clean, and merge sensor data using the TSI v3 external API from specified dates. 
Main inputs: developer key(s), secret(s), data date params (start_date & end_date)

### Most of the workflow can be explained through the main() function
1. Input developer key/secret pairs
2. Call client_token() function to return token .json file(s) by using developer key/secret pair
   until no key/secret pairs are left
3. Store token .json file(s) in ./client_tokens (file is named by developer email)
4. Open ./client_tokens folder and begin iterating through each token .json file in it. 
5. For each token file, call device_list() which takes in the token .json file PATH as an argument. 
   device_list() will extract the token string from the file input and GETS device list data associated with
   user token in json format. 
   The json of device list data is then dumped and stored in ./device_list_by_developer_user.
   The list of device data (e.g. device_cloud_id, device_location, friendly_name, etc.) is also appended to a 
   master .csv file using the append_device_list() function, which is found in ./master_device_list/master_device_list.csv. 
   Within append_device_list(), we utilize shorten_name() and get_country() to obtain and serialize valuable data associated
   with each device, most notably location and a shortened friendly device ID.
6. Now, with the master .csv file of all the devices, we can call get_telemetry_flat() to request data for each device from 
   the TSI server. The function (and GET method by extension) notably takes in the cloud ID of the device as an argument. 
   get_telemetry_flat() returns a .json file that is stored in ./flat_telemetry_json_RAW. This function call is repeated
   for all devices located in master_device_list.csv
7. All of the .json data files in ./flat_telemetry_json_RAW is converted to .csv files and stored in ./flat_telemetry_csv_RAW
   using the flatten_json() function. 
8. All of the .csv files in ./flat_telemetry_csv_RAW are merged together in one big .csv file. The merged file is outputted in 
   the main directory as output_raw_telemetry.csv


OBJECTIVES for this week

Keep updated/merged file for each device (run by run)
#talk with mike and prakash about this as well

add a flag in for suspect data during lvl 1 QA (device status is not support for v3 flat telemetry? not sure)

assuming we still have the 30 day rule, script continuously appends data week by week


'''

import requests
import os
import json
import glob 
import pandas as pd
from datetime import datetime
import numpy as np
import reverse_geocode

#PARAMS
secrets_PATH = r'./account_auth_info/secrets.csv'
start_date = '2022-12-05T15:00:00Z'
end_date = '2023-01-04T12:30:00Z'

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

#shorten name based off de Foy's rules
def shorten_name(friendly_name, country_code, is_indoor_flag) -> str:
    #de Foy's code

    #determine if friendly_name has outdoor/indoor in its name (TBD)

    #pattern guide for str replacement
    pg = {'of': '_',
          'university': 'u',
          'univ': 'u',
          'airport:': 'apt',
          'campus': '',
          'school': '',
          ' ': '_',
          ',': '_',
          '.': '_',
          '-': '_',
          '/': '_',
          '(': '_',
          ')': '_',
          '__': '_'
          }

    #method for replacing string using pattern guide dict
    def str_replace(string, pattern_guide) -> str:
        for pattern in pattern_guide:
            string = string.replace(pattern, pattern_guide[pattern])

        return string 

    #lowercase friendly name
    friendly_name = friendly_name.lower()
    #replace patterns in friendly_name according to pattern guide
    friendly_name = str_replace(friendly_name, pg)
    #add countrycode to beginning of short name
    country_code = country_code.lower()
    friendly_name = country_code + '_' + friendly_name
    #append is_indoors flag (type bool) to the short name
    friendly_name = friendly_name + '_' + str(is_indoor_flag)
    
    return friendly_name

#get country based of coords
def get_country(coords) -> list:
    #search requires list input
    location = reverse_geocode.search(coords)
    country_code = location[0]['country_code']
    country = location[0]['country']
    city = location[0]['city']

    return [city, country, country_code]

#append new countries to master device list
def append_device_list(response_json, dev_email) -> None:
    #open master_device_list in subdirectory
    df = pd.read_csv(os.path.join(r'./master_device_list', 'master_device_list.csv'))
    
    for device in response_json:
        cloud_account_id = device['account_id']
        cloud_device_id = device['device_id']
        serial_number = device['serial']
        friendly_name = device['metadata']['friendlyName']
        is_indoor = device['metadata']['is_indoor']
        longitude = device['metadata']['longitude']
        latitude = device['metadata']['latitude']

        #list inputs
        coords = [(float(device['metadata']['latitude']), float(device['metadata']['longitude']))]

        #get city/country/country_code based off coords
        city = get_country(coords)[0]
        country = get_country(coords)[1]
        country_code = get_country(coords)[2]

        #convert coords to tuple type
        #coords = (longitude, latitude)
        
        #shorten friendly_name using shorten_name() subroutine
        short_name = shorten_name(friendly_name, country_code, is_indoor)

        #new device to be inserted into csv
        insert_row = {
            'cloud_account_id': cloud_account_id,
            'cloud_device_id': cloud_device_id,
            'serial_number': serial_number,
            'city': city,
            'country': country,
            'friendly_name': friendly_name,
            'short_name': short_name,
            'dev_email': dev_email,
            'is_indoor': is_indoor,
            'coords': coords
        }

        #check to see if coord has changed for existing sensors and overwrite
        #df.loc[(df['cloud_device_id'] == cloud_device_id) & (df['coords'] != coords), ['coords']] = coords
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

def get_telemetry_flat(token_json_file, device_id, short_name, start_date, end_date) -> None:
  
  #initialize PATH where token for user is located
  token_PATH = fr'./client_tokens/{token_json_file}'
  json_file = open(token_PATH)
  #get data inside json file
  data = json.load(json_file)
    
  token = data['access_token']

  #use telem_query link to complete request Url
  telem_query = ['model', 'serial', 'location', 'is_public', 'is_indoor', 'mcpm1x0', 'mcpm2x5', 'mcpm4x0', 'mcpm10', 
                 'ncpm0x5', 'ncpm1x0', 'ncpm2x5', 'ncpm4x0', 'ncpm10', 'tpsize', 'mcpm2x5_aqi', 'mcpm10_aqi', 'co2_ppm', 
                 'co_ppm', 'baro_inhg', 'o3_ppb', 'no2_ppb', 'so2_ppb', 'ch2o_ppb', 'voc_mgm3', 'temperature', 'rh'
                ]

  requestUrl = f"https://api-prd.tsilink.com/api/v3/external/telemetry/flat-format?device_id={device_id}&start_date={start_date}&end_date={end_date}"
  
  #create full requestUrl
  for arg in telem_query:
    requestUrl += '&telem[]=' + arg
  
  requestHeaders = {
    "Accept": "application/json",
    "Authorization": f"Bearer {token}"
  }

  response = requests.get(requestUrl, headers = requestHeaders)
 
  #trim start_date and end_date for better filenaming
  start_date = start_date[:-4]
  end_date = end_date[:-4]

  #format of output file: short_name + start_date + end_date
  with open(os.path.join(r'./flat_telemetry_json_RAW', f'{short_name}_{start_date}_{end_date}.json'), "w") as outfile:
    outfile.write(response.text)

def flatten_json(json_file):
    raw_json = open(json_file, encoding = 'utf-8', errors = 'ignore')
    flat_json = json.load(raw_json)
    #normalize json
    raw_df = pd.json_normalize(flat_json)

    file_PATH = str(json_file)
    #remove directory from file_PATH to get file_name (./flat_telemetry_json_RAW/) and .json tag
    file_name = file_PATH[26:-5]

    #dump csv into directory
    raw_df.to_csv(os.path.join(r'./flat_telemetry_csv_RAW', f'{file_name}.csv'), index = False)

def level_zero_raw(raw_df):
    #set data type for serial numbers
    raw_df['serial'] = raw_df['serial'].astype(np.int64)
    #set data type for timestamps
    raw_df['timestamp'] = pd.to_datetime(raw_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    return raw_df

def level_zero_hourly(lvl0_raw_df):
    #group df by specific sensor and hour of data
    lvl0_raw_df['timestamp'] = pd.to_datetime(lvl0_raw_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    grouped_df = lvl0_raw_df.groupby(['serial', lvl0_raw_df['timestamp'].dt.year, lvl0_raw_df['timestamp'].dt.month, lvl0_raw_df['timestamp'].dt.day, lvl0_raw_df['timestamp'].dt.hour])

    grouped_df = grouped_df.filter(lambda x: len(x) * (x['timestamp'].diff().value_counts(dropna = True).idxmax().total_seconds() / 60) >= 45)  
    
    grouped_df.reset_index(drop = True, inplace = True)

    #all cols 
    '''
    cloud_account_id,cloud_device_id,is_indoor,is_public,latitude,longitude,
    mcpm10,mcpm10_aqi,mcpm1x0,mcpm2x5,mcpm2x5_aqi,mcpm4x0,model,ncpm0x5,ncpm10,
    ncpm1x0,ncpm2x5,ncpm4x0,rh,serial,temperature,timestamp,tpsize
    '''

    grouped_hourly_df = grouped_df.groupby(['serial', grouped_df['timestamp'].dt.year, grouped_df['timestamp'].dt.month, grouped_df['timestamp'].dt.day, grouped_df['timestamp'].dt.hour])
    grouped_hourly_df = grouped_hourly_df[['mcpm10','mcpm10_aqi','mcpm1x0','mcpm2x5','mcpm2x5_aqi','mcpm4x0','ncpm0x5','ncpm10',
                                          'ncpm1x0','ncpm2x5','ncpm4x0','rh','temperature','tpsize']].mean().round(2)     
    
    grouped_hourly_df.reset_index(level = ['serial'], inplace = True)
    grouped_hourly_df['timestamp'] = grouped_hourly_df.index.to_numpy()
    grouped_hourly_df['timestamp'] = grouped_hourly_df['timestamp'].apply(lambda x: datetime(*x))
    grouped_hourly_df.reset_index(drop = True, inplace = True)

    grouped_hourly_df = pd.merge(grouped_hourly_df, lvl0_raw_df[['serial', 'cloud_account_id', 'cloud_device_id',
                                                                 'is_indoor', 'is_public', 'latitude', 'longitude', 
                                                                ]], on = 'serial', how = 'left')

    grouped_hourly_df['timestamp'] = pd.to_datetime(grouped_hourly_df['timestamp'], format = '%Y-%m-%d %H:%M:%S')
    grouped_hourly_df.drop_duplicates(subset = ['serial', 'timestamp'], inplace = True)

    return grouped_hourly_df

def level_one_raw(lvl0_raw_df):
    #V2 had case error filtering for the data, but v3 doesn't have 'Device Status' query parameter // will followup

    #Data capping function for ug/m3 measurements
    # 1 min -> >5000 ug/m3
    # 5 min -> >2000 ug/m3
    # >10min -> >1000 ug/m3

    lvl0_raw_df['time_delta'] = lvl0_raw_df['timestamp'].diff().value_counts(dropna = True).idxmax().total_seconds() / 60

    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 10) & (lvl0_raw_df['mcpm1x0'] >= 1000), ['mcpm1x0']] = 1000
    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 5) & (lvl0_raw_df['mcpm1x0'] >= 2000), ['mcpm1x0']] = 2000
    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 1) & (lvl0_raw_df['mcpm1x0'] >= 5000), ['mcpm1x0']] = 5000

    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 10) & (lvl0_raw_df['mcpm2x5'] >= 1000), ['mcpm2x5']] = 1000
    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 5) & (lvl0_raw_df['mcpm2x5'] >= 2000), ['mcpm2x5']] = 2000
    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 1) & (lvl0_raw_df['mcpm2x5'] >= 5000), ['mcpm2x5']] = 5000

    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 10) & (lvl0_raw_df['mcpm4x0'] >= 1000), ['mcpm4x0']] = 1000
    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 5) & (lvl0_raw_df['mcpm4x0'] >= 2000), ['mcpm4x0']] = 2000
    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 1) & (lvl0_raw_df['mcpm4x0'] >= 5000), ['mcpm4x0']] = 5000

    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 10) & (lvl0_raw_df['mcpm10'] >= 1000), ['mcpm10']] = 1000
    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 5) & (lvl0_raw_df['mcpm10'] >= 2000), ['mcpm10']] = 2000
    lvl0_raw_df.loc[(lvl0_raw_df['time_delta'] >= 1) & (lvl0_raw_df['mcpm10'] >= 5000), ['mcpm10']] = 5000

    return lvl0_raw_df

def level_one_hourly(lvl1_raw_df):
    #group df by specific sensor and hour of data
    #lvl0_raw_df['timestamp'] = pd.to_datetime(lvl0_raw_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    grouped_df = lvl1_raw_df.groupby(['serial', lvl1_raw_df['timestamp'].dt.year, lvl1_raw_df['timestamp'].dt.month, lvl1_raw_df['timestamp'].dt.day, lvl1_raw_df['timestamp'].dt.hour])

    grouped_df = grouped_df.filter(lambda x: len(x) * (x['timestamp'].diff().value_counts(dropna = True).idxmax().total_seconds() / 60) >= 45)  
    
    grouped_df.reset_index(drop = True, inplace = True)

    grouped_hourly_df = grouped_df.groupby(['serial', grouped_df['timestamp'].dt.year, grouped_df['timestamp'].dt.month, grouped_df['timestamp'].dt.day, grouped_df['timestamp'].dt.hour])
    grouped_hourly_df = grouped_hourly_df[['mcpm10','mcpm10_aqi','mcpm1x0','mcpm2x5','mcpm2x5_aqi','mcpm4x0','ncpm0x5','ncpm10',
                                          'ncpm1x0','ncpm2x5','ncpm4x0','rh','temperature','tpsize']].mean().round(2)     
    
    grouped_hourly_df.reset_index(level = ['serial'], inplace = True)
    grouped_hourly_df['timestamp'] = grouped_hourly_df.index.to_numpy()
    grouped_hourly_df['timestamp'] = grouped_hourly_df['timestamp'].apply(lambda x: datetime(*x))
    grouped_hourly_df.reset_index(drop = True, inplace = True)

    grouped_hourly_df = pd.merge(grouped_hourly_df, lvl1_raw_df[['serial', 'cloud_account_id', 'cloud_device_id',
                                                                 'is_indoor', 'is_public', 'latitude', 'longitude', 
                                                                 'time_delta']], on = 'serial', how = 'left')

    grouped_hourly_df['timestamp'] = pd.to_datetime(grouped_hourly_df['timestamp'], format = '%Y-%m-%d %H:%M:%S')
    grouped_hourly_df.drop_duplicates(subset = ['serial', 'timestamp'], inplace = True)
    
    return grouped_hourly_df

###MAIN BODY FUNCTION
def main(secrets_PATH, start_date, end_date) -> None:
    #open secrets file
    secrets_df = pd.read_csv(secrets_PATH)

    #iterate through each key/secret pair and get the respective token (if user has access to multiple TSI accounts)
    for index, row in secrets_df.iterrows():
        client_key = row['key']
        client_secret = row['secret']
        dev_email = row['dev_email']

        client_token(client_key, client_secret)

        #notification that specific client token has been successfully received
        print(f'Received token from developer key {dev_email}')

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
        short_name = row['short_name']
        token_json_file = f'({dev_email})_token.json'

        #dump json files for each device in /flat_telemetry_json
        get_telemetry_flat(token_json_file, device_id, short_name, start_date, end_date)
    
    print('Device telemetry jsons have been successfully dumped into /flat_telemetry_json_RAW')

    #convert all telemetry jsons to csv files using json decoder function
    telemetry_jsons_PATHS = os.path.join(r'./flat_telemetry_json_RAW', '*.json')
    
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
    combined_csv.to_csv(os.path.join(r'./telemetry_outputs', 'telemetry_raw.csv'), index = False)

    #sort combined csv by datetime
    combined_csv['timestamp'] = pd.to_datetime(combined_csv['timestamp'], format='%Y-%m-%d %H:%M:%S')
    combined_csv.sort_values(by = 'timestamp', ascending = True, inplace = True)

    print('Merged raw csv successfully compiled')

    ### Level 0 QA
    #level 0 is just data type conversions and .csv files don't save data types so there's 
    #not much point in outputting csv for this df
    lvl0_raw_df = level_zero_raw(combined_csv)
    
    lvl0_hourly_df = level_zero_hourly(lvl0_raw_df)
    lvl0_hourly_df.to_csv(os.path.join(r'./telemetry_outputs', 'telemetry_lvl_0_hourly.csv'), index = False)

    print('Level 0 QA completed')

    ### Level 1 QA
    lvl1_raw_df = level_one_raw(combined_csv)
    lvl1_raw_df.to_csv(os.path.join(r'./telemetry_outputs', 'telemetry_lvl_1_raw.csv'), index = False)

    lvl1_hourly_df = level_one_hourly(combined_csv)
    lvl1_hourly_df.to_csv(os.path.join(r'./telemetry_outputs', 'telemetry_lvl_1_hourly.csv'), index = False)

    print('Level 1 QA completed')
    

if __name__ == "__main__":
    main(secrets_PATH, start_date, end_date)
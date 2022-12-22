'''
Author: Benjamin de Foy & Darren Wu
Date: 12/5/2022 

Converts flat telemetry json to .csv format using pd.json_normalize library function

PARAMS:
json_filename = flat telemetry json filename
'''

import json
import os
import pandas as pd

json_filename = 'c50fv8u7kn8vcc9cd6cg_2022-12-02T15:00:00Z_2022-12-22T15:00:00Z.json'

def flatten_json(json_filename):
    raw_json = open(os.path.join(r'./flat_telemetry_json', json_filename), encoding = 'utf-8', errors = 'ignore')
    flat_json = json.load(raw_json)
    raw_df = pd.json_normalize(flat_json)

    raw_df.to_csv(os.path.join(r'./flat_telemetry_csv_RAW', f'{json_filename}.csv'), index = False)

if __name__ == "__main__":
  flatten_json(json_filename)

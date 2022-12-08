'''
Author: Darren Wu
Date: 12/5/2022 

Takes account external v3 client_key and client_secret and requests token from TSI server. 
Dumps token data into /client_tokens folder.

PARAMS:
client_key = key linked to account in v3
client_secret = secret linked to account in v3
token_filename = filename for output data (.json)
'''

import requests
import json
import os

### TO CHANGE
client_key = 'ZEMIbhqwCe7MIVfGeq1pNA9nqAGpvpcVuaw9XEEmRXtgGt1I'
client_secret = 'ayuIiRntou61plHwEtasfC4HnxBqG02svGGhaUSszGVBm9PRvn0yNWgUAq5UwpJN'
token_filename = 'mike_bergin_token.json'

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

    with open(os.path.join(r'./client_tokens', token_filename), 'w') as f:
        json.dump(data, f)

if __name__ == "__main__":
  client_token(client_key, client_secret)
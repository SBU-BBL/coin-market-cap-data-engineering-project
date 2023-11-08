import os
import math
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import csv
import yaml


def get_data_from_api(url, headers):
    # Create api request & receive response
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        if result:
            data = result['data']
            return data
    else:
        print(response.status_code)


def save_json_to_csv(output, date, path):
    # if not exist
    if not os.path.exists(path):
        os.makedirs(path)
    
    full_path = path + f'{date}.csv'
    
    with open(full_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        
        # Write the header (keys from the first dictionary in the list)
        writer.writerow(output[0].keys())
        
        # Write the rows
        for row in output:
            writer.writerow(row.values())


if __name__ == '__main__':

    # get config
    with open("config.yml", 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    url = config['API']['EXCHANGE']['MAP']
    date = config['DATE']

    API_KEY = os.environ.get('API_KEY')

    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': API_KEY
    }

    # process
    exchange_map = get_data_from_api(url=url, headers=headers)
    save_json_to_csv(output=exchange_map,
        date=date,
        path='/home/flying-dragon/Documents/crypto-data-engineering-project/exchange_map/')


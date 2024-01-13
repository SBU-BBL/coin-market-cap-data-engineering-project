import os
import math
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import csv
import yaml
import json


def chunked_list(lst, chunk_size):
    """Yield successive chunk_size chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def get_data_from_api(url, headers, time_start, time_end, interval):
    # Join the coin IDs into a comma-separated string
    full_url = f"{url}?time_start={time_start}&time_end={time_end}&interval={interval}"
    
    response = requests.get(full_url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        if result:
            data = result['data']
            return data
    else:
        print(response.status_code)
        return []

def save_json(output, date, path):
    if not os.path.exists(path):
        os.makedirs(path)
    
    full_path = os.path.join(path, f'{date}.json')
    with open(full_path, "w") as json_file:
        json.dump(output, json_file, indent=4)

def get_coin_id(date, path):
    path += f'{date}.csv'
    id_list = pd.read_csv(path, usecols=[0]).iloc[:, 0].tolist()
    return id_list


if __name__ == '__main__':

    # get config
    with open("config.yml", 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    url = config['API']['GLOBAL_METRIC']['QUOTES_HISTORICAL']
    date = config['DATE']
    raw_zone_path = config['PATH']['RAW_ZONE']

    table_name = os.path.splitext(os.path.basename(__file__))[0].split('s2r_')[-1]
    table_path = raw_zone_path + f'/{table_name}/'

    # date processing
    current_day = datetime.strptime(date, '%Y%m%d')
    previous_day = current_day - timedelta(days=1)
    time_start = previous_day.strftime('%Y-%m-%dT23:55:00Z')
    time_end = current_day.strftime('%Y-%m-%dT23:59:59Z')
    
    # api config
    API_KEY = os.environ.get('API_KEY')
    API_KEY = '2ca92cfc-43ad-4ec6-9f43-353fb6bf7085'
    

    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': API_KEY
    }

    interval = '5m'
   
    quote_hist = get_data_from_api(
        url=url, 
        headers=headers, 
        time_start=time_start, 
        time_end=time_end, 
        interval=interval
    )

    if quote_hist:
        save_json(output=quote_hist, date=date, path=table_path)


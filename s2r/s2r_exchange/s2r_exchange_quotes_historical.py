import os
import math
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import csv
import yaml
import json
import argparse
from dotenv import load_dotenv


def chunked_list(lst, chunk_size):
    """Yield successive chunk_size chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def get_data_from_api(url, headers, coin_ids, time_start, time_end, interval):
    # Join the coin IDs into a comma-separated string
    ids_string = ','.join(map(str, coin_ids))
    full_url = f"{url}?id={ids_string}&time_start={time_start}&time_end={time_end}&interval={interval}&count=10000"
    
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
    for coin_data in output.values():
        coin_symbol = coin_data['slug']
        coin_path = os.path.join(path, coin_symbol)
        if not os.path.exists(coin_path):
            os.makedirs(coin_path)
        
        full_path = os.path.join(coin_path, f'{date}.json')
        data = coin_data['quotes']

        with open(full_path, "w") as json_file:
            json.dump(data, json_file, indent=4)

def get_coin_id(date, path):
    file_path = f'{path}{date}.csv'
    df = pd.read_csv(file_path)
    df['first_historical_data'] = pd.to_datetime(df['first_historical_data']).dt.tz_localize(None)
    given_date = pd.to_datetime(date, format='%Y%m%d')

    # Filter rows where 'first_historical_data' is less than or equal to the given date
    filtered_df = df[df['first_historical_data'] <= given_date]
    return filtered_df['id'].tolist()

# get config
with open("config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

url = config['API']['EXCHANGE']['QUOTES_HISTORICAL']
raw_zone_path = config['PATH']['RAW_ZONE']

table_name = os.path.splitext(os.path.basename(__file__))[0].split('s2r_')[-1]
table_path = raw_zone_path + f'/{table_name}/'

# Set up argument parsing
parser = argparse.ArgumentParser(description='Process')
parser.add_argument('--date', required=True, help='The date to process data for')

# Parse the arguments
args = parser.parse_args()
date = args.date

# date processing
current_day = datetime.strptime(date, '%Y%m%d')
previous_day = current_day - timedelta(days=1)
time_start = previous_day.strftime('%Y-%m-%dT23:55:00Z')
time_end = current_day.strftime('%Y-%m-%dT23:59:59Z')

# API key
load_dotenv()
API_KEY = os.getenv('API_KEY')

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY
}

# add parameters
exchange_map_path = config['PATH']['RAW_ZONE'] + '/exchange_map/'
coin_id_list = get_coin_id(date, exchange_map_path)

interval = '5m'

batch_size = 34
for coin_batch in chunked_list(coin_id_list, batch_size):
    exchange_maps = get_data_from_api(
        url=url, 
        headers=headers, 
        coin_ids=coin_batch, 
        time_start=time_start, 
        time_end=time_end, 
        interval=interval
    )
    if exchange_maps:
        save_json(output=exchange_maps, date=date, path=table_path)


import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import csv
import yaml
import json
from dotenv import load_dotenv
import argparse
import sys
# Add the root directory to sys.path
current_script_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_script_path, '..', '..'))
sys.path.append(project_root)
from utilities.check_log import is_batch_processed, log_processed_batch, get_log_file_path

def chunked_list(lst, chunk_size):
    """Yield successive chunk_size chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def get_data_from_api(url, headers, coin_ids, time_start, time_end, interval, time_period):
    # Join the coin IDs into a comma-separated string
    ids_string = ','.join(map(str, coin_ids))
    full_url = f"{url}?id={ids_string}&time_start={time_start}&time_end={time_end}&interval={interval}&time_period={time_period}&count=10000"

    attempt = 0
    max_attempts = 3
    while attempt < max_attempts:
        response = requests.get(full_url, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if result:
                data = result['data']
                return data
        elif response.status_code == 400:
            time.sleep(2 * attempt)  # backoff
        else:
            print(f"Error: Received status code {response.status_code}")
            raise SystemExit(f"Script terminated due to status code: {response.status_code}")
        attempt += 1


def save_json(output, date, path):
    for coin_data in output.values():
        coin_symbol = coin_data['symbol']
        sanitized_symbol = ''.join(char for char in coin_symbol if char.isalnum())
        coin_path = os.path.join(path, sanitized_symbol)
        if not os.path.exists(coin_path):
            os.makedirs(coin_path)
        
        full_path = os.path.join(coin_path, f'{date}.json')
        data = coin_data['quotes']

        with open(full_path, "w") as json_file:
            json.dump(data, json_file, indent=4)


def get_coin_id(date, path):
    df = pd.read_csv(path)
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

url = config['API']['CRYPTOCURRENCY']['OHLCV_HISTORICAL']
raw_zone_path = config['PATH']['RAW_ZONE']

table_name = os.path.splitext(os.path.basename(__file__))[0].split('s2r_')[-1]
endpoint_name = os.path.splitext(os.path.basename(__file__))[0].split('_')[1]
table_path = os.path.join(raw_zone_path, endpoint_name, table_name)

# Set up argument parsing
parser = argparse.ArgumentParser(description='Process')
parser.add_argument('--date', required=True, help='The date to process data for')

# Parse the arguments
args = parser.parse_args()
date = args.date

# date processing
current_day = datetime.strptime(date, '%Y%m%d')
previous_day = current_day - timedelta(days=1)
time_end = current_day.strftime('%Y-%m-%d')
time_start = previous_day.strftime('%Y-%m-%d')

# api key
load_dotenv()
API_KEY = os.getenv('API_KEY')

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY
}

# add parameters
# cryptocurrency_map_path = config['PATH']['RAW_ZONE'] + f'\\{endpoint_name}\\cryptocurrency_map\\cryptocurrency_map.csv'
cryptocurrency_map_path = os.path.join(raw_zone_path, endpoint_name, "cryptocurrency_map", "cryptocurrency_map.csv")
coin_id_list = get_coin_id(date, cryptocurrency_map_path)

interval = '1d'
time_period = 'daily'

batch_size = 34

log_file_path = get_log_file_path()
for coin_batch in chunked_list(coin_id_list, batch_size):
    # Check if the batch has already been processed
    if is_batch_processed(coin_batch, date, log_file_path):
        print(f"Skipping already processed batch for date: {date}")
        continue

    cryptocurrency_maps = get_data_from_api(
        url=url, 
        headers=headers, 
        coin_ids=coin_batch, 
        time_start=time_start, 
        time_end=time_end, 
        interval=interval,
        time_period=time_period
    )

    if cryptocurrency_maps:
        save_json(output=cryptocurrency_maps, date=date, path=table_path)
        log_processed_batch(coin_batch, date, log_file_path)
import os
import sys
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
current_script_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_script_path, '..', '..'))
sys.path.append(project_root)
from utilities.check_log import is_date_processed, log_processed_date, get_log_file_path

def chunked_list(lst, chunk_size):
    """Yield successive chunk_size chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def get_data_from_api(url, headers, time_start, time_end, interval):
    # Join the coin IDs into a comma-separated string
    full_url = f"{url}?time_start={time_start}&time_end={time_end}&interval={interval}"
    attempt = 0
    max_attempts = 3
    while attempt < max_attempts:
        response = requests.get(full_url, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if result:
                data = result['data']
                return data
        elif response.status_code == 429:
            time.sleep(20)
        elif response.status_code == 400:
            time.sleep(2 * attempt)  # backoff
        else:
            print(f"Error: Received status code {response.status_code}")
            if attempt >= 2:
                raise SystemExit(f"Script terminated due to status code: {response.status_code}")
        attempt += 1

def save_json(output, date, path):
    if not os.path.exists(path):
        os.makedirs(path)
    
    full_path = os.path.join(path, f'{date}.json')
    with open(full_path, "w") as json_file:
        json.dump(output, json_file, indent=4)

def get_coin_id(date, path):
    file_path = f'{path}{date}.csv'
    df = pd.read_csv(file_path)
    df['first_historical_data'] = pd.to_datetime(df['first_historical_data']).dt.tz_localize(None)
    given_date = pd.to_datetime(date, format='%Y%m%d')

    # Filter rows where 'first_historical_data' is less than or equal to the given date
    filtered_df = df[df['first_historical_data'] <= given_date]
    return filtered_df['id'].tolist()



# get config
with open(r"C:\\Users\\haodu\\OneDrive\\Desktop\\crypto\\crypto-data-engineering-project\\config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

url = config['API']['GLOBAL_METRIC']['QUOTES_HISTORICAL']
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
time_start = previous_day.strftime('%Y-%m-%dT23:55:00Z')
time_end = current_day.strftime('%Y-%m-%dT23:59:59Z')

# API key
load_dotenv()
API_KEY = os.getenv('API_KEY')

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY
}

interval = '5m'
log_file_path = get_log_file_path()

if not is_date_processed(date, log_file_path):
    quote_hist = get_data_from_api(
        url=url, 
        headers=headers, 
        time_start=time_start, 
        time_end=time_end, 
        interval=interval
    )
    if quote_hist:
        save_json(output=quote_hist, date=date, path=table_path)
        log_processed_date(date, log_file_path)
else:
    print(f"Skipping already processed date: {date}")


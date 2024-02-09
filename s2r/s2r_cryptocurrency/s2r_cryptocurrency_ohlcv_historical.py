import os
import math
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import csv
import yaml
import json
from dotenv import load_dotenv
import argparse


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


def save_json(output, date, path):
    # if not exist
    coin_symbol = output['symbol']
    path += f'{coin_symbol}/'
    if not os.path.exists(path):
        os.makedirs(path)
    
    full_path = path + f'{date}.json'
    data = output['quotes']

    with open(full_path, "w") as json_file:
        json.dump(data, json_file, indent=4)

def get_coin_id(date, path):
    path += f'{date}.csv'
    id_list = pd.read_csv(path, usecols=[0]).iloc[:, 0].tolist()
    return id_list


# get config
with open("config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

url = config['API']['CRYPTOCURRENCY']['OHLCV_HISTORICAL']
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
time_start = previous_day.strftime('%Y-%m-%d')
time_end = current_day.strftime('%Y-%m-%d')

# api key
load_dotenv()
API_KEY = os.getenv('API_KEY')

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY
}

# add parameters
cryptocurrency_map_path = config['PATH']['RAW_ZONE'] + '/cryptocurrency_map/'
coin_id_list = get_coin_id(date, cryptocurrency_map_path)

interval = 'hourly'
time_period = 'hourly'
for coin_id in coin_id_list:
    # process
    cryptocurrency_map = get_data_from_api(url=url + f'?id={str(coin_id)}&time_start={time_start}&time_end={time_end}&interval={interval}&time_period={time_period}', headers=headers)
    save_json(output=cryptocurrency_map,
        date=date,
        path=table_path)
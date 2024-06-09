import os
import requests
import csv
import yaml
import json
import argparse
from dotenv import load_dotenv

def get_data_from_api(url, headers, start, limit):
    # Create api request & receive response 
    paginated_url = f"{url}?start={start}&limit={limit}"
    response = requests.get(paginated_url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        if result:
            data = result['data']
            return data
    else:
        print(response.status_code)
    return []

def save_data_to_csv(output, csv_file, is_first_batch):
    if is_first_batch:
        # Write the header (keys from the first dictionary in the list)
        csv_file.writerow(output[0].keys())
    
    # Write the rows
    for row in output:
        csv_file.writerow(row.values())

def save_data_to_json(output, json_file_path):
    # Read existing data, modify, and write back to JSON file
    try:
        if os.path.exists(json_file_path) and os.path.getsize(json_file_path) > 0:
            with open(json_file_path, "r") as json_file:
                existing_data = json.load(json_file)
        else:
            existing_data = []
        
        existing_data.extend(output)
        
        with open(json_file_path, "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
    except json.JSONDecodeError as e:
        print(f"Error reading JSON file: {e}")


# get config
with open(r"config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

url = config['API']['CONTENT']['LATEST']
raw_zone_path = config['PATH']['RAW_ZONE']
table_name = os.path.splitext(os.path.basename(__file__))[0].split('s2r_')[-1]
table_path = raw_zone_path + f'/{table_name}/'

# Set up argument parsing
parser = argparse.ArgumentParser(description='Process')
parser.add_argument('--date', required=True, help='The date to process data for')

# Parse the arguments
args = parser.parse_args()
date = args.date

load_dotenv()
API_KEY = os.getenv('API_KEY')

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY
}

# process
limit = 200
start = 1
is_first_batch = True

full_path = table_path + f'{date}.json'
if not os.path.exists(table_path):
    os.makedirs(table_path)

while True:
    batch_data = get_data_from_api(url=url, headers=headers, start=start, limit=limit)
    if not batch_data:
        break

    save_data_to_json(batch_data, full_path) 

    if len(batch_data) < limit:
        # Last batch
        break
    start += limit  # Move to the next batch
        
print(start)
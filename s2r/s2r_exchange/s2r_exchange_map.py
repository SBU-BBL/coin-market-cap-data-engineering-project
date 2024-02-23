import os
import requests
import csv
import yaml
import argparse
from dotenv import load_dotenv

def chunked_list(lst, chunk_size):
    """Yield successive chunk_size chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def get_data_from_api(url, headers, start, limit):
    # Create api request & receive response 
    paginated_url = f"{url}&start={start}&limit={limit}"
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



# get config
with open("config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

url = config['API']['EXCHANGE']['MAP']
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

# API key
load_dotenv()
API_KEY = os.getenv('API_KEY')

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY
}

# process
limit = 5000
start = 1
is_first_batch = True

full_path = os.path.join(table_path, f'{table_name}.csv')
if not os.path.exists(table_path):
    os.makedirs(table_path)

with open(full_path, "w", newline="", encoding="utf-8") as csv_file:
    writer = csv.writer(csv_file)

    while True:
        batch_data = get_data_from_api(url=url, headers=headers, start=start, limit=limit)
        if not batch_data:
            break

        save_data_to_csv(batch_data, writer, is_first_batch)
        is_first_batch = False

        if len(batch_data) < limit:
            # Last batch
            break

        start += limit  # Move to the next batch

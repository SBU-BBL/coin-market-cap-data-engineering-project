import os
import requests
import csv
import yaml
import argparse
from dotenv import load_dotenv

def get_fiat_data_from_api(url, headers):
    # Create api request & receive response
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        if result:
            data = result['data']
            return data
    else:
        print(response.status_code)
    return []

def save_data_to_csv(output, csv_file):
    # Write the header (keys from the first dictionary in the list)
    csv_file.writerow(output[0].keys())
    
    # Write the rows
    for row in output:
        csv_file.writerow(row.values())

if __name__ == '__main__':
    # get config
    with open("config.yml", 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    fiat_url = config['API']['FIAT']['MAP']  # Adjust this according to your config structure
    raw_zone_path = config['PATH']['RAW_ZONE']
    table_name = 'fiat_currency_map'  # Change to the appropriate table name
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

    full_path = table_path + f'{date}.csv'
    if not os.path.exists(table_path):
        os.makedirs(table_path)

    with open(full_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)

        fiat_data = get_fiat_data_from_api(url=fiat_url, headers=headers)
        if fiat_data:
            save_data_to_csv(fiat_data, writer)

import os
import csv
import yaml
import json
import argparse
from dotenv import load_dotenv
import pandas as pd

with open(r"config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

# get path
golden_zone_path = config['PATH']['GOLDEN_ZONE']
raw_zone_path = config['PATH']['RAW_ZONE']

table_name = os.path.splitext(os.path.basename(__file__))[0].split('r2g_')[-1]
endpoint_name = os.path.splitext(os.path.basename(__file__))[0].split('_')[1]

golden_table_path = os.path.join(golden_zone_path, endpoint_name, table_name)
raw_table_path = os.path.join(raw_zone_path, endpoint_name, table_name)

# Ensure the golden_table_path exists
os.makedirs(golden_table_path, exist_ok=True)

# Traverse through each coin's folder in raw_table_path
for coin_name in os.listdir(raw_table_path):
    coin_folder_path = os.path.join(raw_table_path, coin_name)
    
    if os.path.isdir(coin_folder_path):
        data = []
        
        # Read each JSON file in the coin's folder
        json_files = [f for f in os.listdir(coin_folder_path) if f.endswith('.json')]
        json_files.sort()  # Sorts filenames lexicographically, which works for yyyymmdd format
        
        # Read each JSON file in the coin's folder in sorted order
        for filename in json_files:
            file_path = os.path.join(coin_folder_path, filename)
            
            with open(file_path, 'r') as file:
                json_data = json.load(file)
                for record in json_data:
                    timestamp = record['timestamp']
                    usd_data = record['quote']['USD']
                    data.append({
                        'coin_name': coin_name,
                        'timestamp': timestamp,
                        'percent_change_1h': usd_data.get('percent_change_1h', None),
                        'percent_change_24h': usd_data.get('percent_change_24h', None),
                        'percent_change_7d': usd_data.get('percent_change_7d', None),
                        'percent_change_30d': usd_data.get('percent_change_30d', None),
                        'price': usd_data.get('price', None),
                        'volume_24h': usd_data.get('volume_24h', None),
                        'market_cap': usd_data.get('market_cap', None),
                        'total_supply': usd_data.get('total_supply', None),
                        'circulating_supply': usd_data.get('circulating_supply', None)
                    })
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(golden_table_path, f"{coin_name}.csv")
        df.to_csv(output_file, index=False)

        # print(f"Data for {coin_name} saved to {output_file}")
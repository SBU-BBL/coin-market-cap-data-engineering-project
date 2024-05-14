import os
import yaml
import json
import pandas as pd

# Load configuration
with open(r"config.yml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

# Get paths
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
        
        # Get a list of JSON filenames and sort them by date (assumed format yyyymmdd.json)
        json_files = [f for f in os.listdir(coin_folder_path) if f.endswith('.json')]
        json_files.sort()  # Sorts filenames lexicographically, which works for yyyymmdd format
        
        # Read each JSON file in the coin's folder in sorted order
        for filename in json_files:
            file_path = os.path.join(coin_folder_path, filename)
            
            with open(file_path, 'r') as file:
                json_data = json.load(file)
                for record in json_data:
                    time_open = record['time_open']
                    time_close = record['time_close']
                    time_high = record['time_high']
                    time_low = record['time_low']
                    usd_data = record['quote']['USD']
                    data.append({
                        'coin_name': coin_name,
                        'time_open': time_open,
                        'time_close': time_close,
                        'time_high': time_high,
                        'time_low': time_low,
                        'open': usd_data.get('open', None),
                        'high': usd_data.get('high', None),
                        'low': usd_data.get('low', None),
                        'close': usd_data.get('close', None),
                        'volume': usd_data.get('volume', None),
                        'market_cap': usd_data.get('market_cap', None),
                        'timestamp': usd_data.get('timestamp', None)
                    })
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        df.drop_duplicates(subset='timestamp', inplace=True)

        
        # Save to CSV
        output_file = os.path.join(golden_table_path, f"{coin_name}.csv")
        df.to_csv(output_file, index=False)

        print(f"Data for {coin_name} saved to {output_file}")

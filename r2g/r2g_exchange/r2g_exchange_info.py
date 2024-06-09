import os
import csv
import yaml
import json
import argparse
from dotenv import load_dotenv
import pandas as pd
import shutil

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
print(table_name)
golden_table_path = os.path.join(golden_zone_path, endpoint_name, table_name)
raw_table_path = os.path.join(raw_zone_path, endpoint_name, "exchange",table_name)

# Ensure the golden_table_path exists
os.makedirs(golden_table_path, exist_ok=True)

# Move all files from raw_table_path to golden_table_path, overwriting any existing files
for filename in os.listdir(raw_table_path):
    raw_file = os.path.join(raw_table_path, filename)
    golden_file = os.path.join(golden_table_path, filename)
    if os.path.isfile(raw_file):
        shutil.copy2(raw_file, golden_file)
        print(f"Moved {raw_file} to {golden_file}")

print(f"All files moved from {raw_table_path} to {golden_table_path}.")
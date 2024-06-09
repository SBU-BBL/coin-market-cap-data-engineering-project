import zipfile
import os

def extract_zip(zip_file, extract_to):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

# Example usage:
zip_file_path = "D:/coin-market-cap-data-engineering-project/cryptocurrency_quotes_historical-003.zip"
extract_to_directory = 'D:/coin-market-cap-data-engineering-project/raw_zone/cryptocurrency'

if not os.path.exists(extract_to_directory):
    os.makedirs(extract_to_directory)

extract_zip(zip_file_path, extract_to_directory)

import json
import os
import sys

def log_processed_batch(coin_batch, date, log_path):
    """Log successfully processed batch for a given date in a JSON file."""
    try:
        with open(log_path, 'r') as log_file:
            log_data = json.load(log_file)
    except (FileNotFoundError, json.JSONDecodeError):
        log_data = {}

    log_data.setdefault(date, []).extend(coin_batch)

    with open(log_path, 'w') as log_file:
        json.dump(log_data, log_file, indent=4)

def is_batch_processed(coin_batch, date, log_path):
    """Check if the batch has already been processed."""
    try:
        with open(log_path, 'r') as log_file:
            log_data = json.load(log_file)
        return all(coin_id in log_data.get(date, []) for coin_id in coin_batch)
    except (FileNotFoundError, json.JSONDecodeError):
        return False
    
def get_log_file_path(zone='raw_zone'):
    """Create a log file path based on the script name and ensure it exists."""
    # Get the base name of the script without the .py extension
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    log_file_name = f"{script_name}.json"

    # Define the directory where you want to save the log file
    log_directory = os.path.join("logs", zone)

    # Create the directory if it does not exist
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Construct the full path for the log file
    log_file_path = os.path.join(log_directory, log_file_name)

    return log_file_path

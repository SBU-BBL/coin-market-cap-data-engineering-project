import subprocess
import os
from datetime import datetime, timedelta

def run_script(script_name, date):
    subprocess.run(['python', script_name, '--date', str(date)], check=True)

def get_directory_file_paths(root_directory):
    directory_file_paths = {}
    
    for subdir, dirs, files in os.walk(root_directory):
        # Use the relative path of the subdir as the dictionary key
        relative_subdir = os.path.relpath(subdir, root_directory)
        # Create a list of full file paths for the current subdir
        file_paths = [os.path.join(subdir, file) for file in files]
        # Add to the dictionary
        directory_file_paths[relative_subdir] = file_paths

    return directory_file_paths

def get_date(start_date, end_date):
    # Generate the list of dates
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)
    return date_list

repo_path = r'X:\repos\crypto-data-engineering-project\s2r\\'
directory_file_paths_dict = get_directory_file_paths(repo_path)

s2r_path = {
#    "s2r_content":[
#       "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_content\\s2r_content_latest.py",
#       "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_content\\s2r_content_post_latest.py",
#       "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_content\\s2r_content_post_top.py"
#    ],
   "s2r_cryptocurrency":[
#       "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_cryptocurrency\\s2r_cryptocurrency_map.py"
      "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_cryptocurrency\\s2r_cryptocurrency_ohlcv_historical.py",
#       "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_cryptocurrency\\s2r_cryptocurrency_quotes_historical.py"
   ]
#    "s2r_exchange":[
    #   "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_exchange\\s2r_exchange_info.py",
    #   "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_exchange\\s2r_exchange_map.py",
    #   "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_exchange\\s2r_exchange_quotes_historical.py"
#    ],
#    "s2r_fiat":[
#       "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_fiat\\s2r_fiat_map.py"
#    ],
#    "s2r_global_metric":[
#       "X:\\repos\\crypto-data-engineering-project\\s2r\\\\s2r_global_metric\\s2r_global_metric_quotes_historical.py"
#    ]
}

start_date = datetime(2024, 2, 16)
end_date = datetime(2024, 2, 16) 
dates_to_run = get_date(start_date, end_date)

for date in dates_to_run:
    for folder in s2r_path.keys():
        for script in s2r_path[folder]:
            run_script(script, date)


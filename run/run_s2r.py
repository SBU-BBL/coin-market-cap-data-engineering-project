import subprocess
import os
from datetime import datetime, timedelta

def run_script(script_name, date):
    try:
        result = subprocess.run(['python', script_name, '--date', str(date)], check=True, capture_output=True, text=True)
        print(f"Output of {script_name} for date {date}: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name} for date {date}: {e.stderr}")
        print(f"Script {script_name} terminated due to status code: {e.returncode}")

def get_directory_file_paths(root_directory, max_depth=3):
    directory_file_paths = {}
    exclude_dirs = {'golden_zone', 'raw_zone'}
    
    for subdir, dirs, files in os.walk(root_directory):
        # Exclude specified directories
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        # Calculate the current depth
        depth = subdir[len(root_directory):].count(os.sep)
        if depth >= max_depth:
            # If current depth exceeds max depth, do not go deeper
            dirs[:] = []
        
        
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

repo_path = 'D:/coin-market-cap-data-engineering-project'


directory_file_paths_dict = get_directory_file_paths(repo_path)

s2r_path = {
#     "s2r_content":[
#       "D:/coin-market-cap-data-engineering-project/s2r/s2r_content/s2r_content_latest.py",
#       "D:/coin-market-cap-data-engineering-project/s2r/s2r_content/s2r_content_post_latest.py",
#       "D:/coin-market-cap-data-engineering-project/s2r/s2r_content/s2r_content_post_top.py"
#    ],
   "s2r_cryptocurrency":[
      "D:/coin-market-cap-data-engineering-project/s2r/s2r_cryptocurrency/s2r_cryptocurrency_map.py",
      "D:/coin-market-cap-data-engineering-project/s2r/s2r_cryptocurrency/s2r_cryptocurrency_quotes_historical.py"
   ]
   ,
   "s2r_exchange":[
      "D:/coin-market-cap-data-engineering-project/s2r/s2r_exchange/s2r_exchange_info.py",
      "D:/coin-market-cap-data-engineering-project/s2r/s2r_exchange/s2r_exchange_map.py",
      "D:/coin-market-cap-data-engineering-project/s2r/s2r_exchange/s2r_exchange_quotes_historical.py"
   ],
   "s2r_fiat":[
      "D:/coin-market-cap-data-engineering-project/s2r/s2r_fiat/s2r_fiat_map.py"
   ],
   
#    "s2r_global_metric":[
#       "D:/coin-market-cap-data-engineering-project/s2r/s2r_global_metric/s2r_global_metric_quotes_historical.py"
#    ]
}


#"D:/coin-market-cap-data-engineering-project/s2r/s2r_cryptocurrency/s2r_cryptocurrency_ohlcv_historical.py"
print(directory_file_paths_dict)



start_date = datetime(2024, 5, 15)
# for cryptocurrency_map
# end_date = datetime(2023, 8, 1)
# for others
end_date = datetime(2024, 6, 4)


dates_to_run = get_date(start_date, end_date)

for date in dates_to_run:
    for folder in s2r_path.keys():
        for script in s2r_path[folder]:
            run_script(script, date)
            print(f'{date} done')


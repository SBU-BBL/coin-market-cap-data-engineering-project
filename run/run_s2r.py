import subprocess
import os

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

# Replace 'your_repo_path' with the path to your repository
repo_path = '/Users/vuh/Documents/crypto-data-engineering-project/s2r/'
directory_file_paths_dict = get_directory_file_paths(repo_path)


s2r_path = {
   "s2r_exchange":[
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_exchange/s2r_exchange_info.py",
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_exchange/s2r_exchange_map.py"
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_exchange/s2r_exchange_quotes_historical.py",
   ],
   "s2r_cryptocurrency":[
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_cryptocurrency/s2r_cryptocurrency_map.py",
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_cryptocurrency/s2r_cryptocurrency_ohlcv_historical.py",
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_cryptocurrency/s2r_cryptocurrency_quotes_historical.py"
   ],
   "s2r_global_metric":[
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_global_metric/s2r_global_metric_quotes_historical.py"
   ],
   "s2r_fiat":[
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_fiat/s2r_fiat_map.py"
   ],
   "s2r_content":[
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_content/s2r_post_comment.py",
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_content/s2r_post_latest.py",
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_content/s2r_content_latest.py",
    "/Users/vuh/Documents/crypto-data-engineering-project/s2r/s2r_content/s2r_post_top.py"
   ]
}


def run_script(script_name, date):
    subprocess.run(['python', script_name, '--date', str(date)], check=True)

dates_to_run = ['2023-11-01', '2023-11-02'] 

for date in dates_to_run:
    for script in scripts:
        run_script(script, date)

import os
import requests
import yaml
import json

def get_latest_posts_from_api(url, headers, symbol=None, last_score=None):
    params = {
        'symbol': symbol if symbol else None,
        'last_score': last_score if last_score else None
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        result = response.json()
        if result and 'data' in result:
            data = result['data']['list']
            last_score = result['data'].get('last_score', None)
            return data, last_score
    else:
        print("Failed to fetch data:")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
    return [], None

def save_data_to_json(output, json_file_path):
    # Read existing data, modify, and write back to JSON file
    try:
        if os.path.exists(json_file_path) and os.path.getsize(json_file_path) > 0:
            with open(json_file_path, "r") as json_file:
                existing_data = json.load(json_file)
        else:
            existing_data = []
        
        existing_data.extend(output)
        
        with open(json_file_path, "w") as json_file:
            json.dump(existing_data, json_file, indent=4)
    except json.JSONDecodeError as e:
        print(f"Error reading JSON file: {e}")


if __name__ == '__main__':
    #get symbol from user
    user_symbol = ""
    while not user_symbol:
        user_symbol = input("Enter the cryptocurrency symbol you want to get the latest posts for (e.g., BTC, ETH): ").strip()
    
    # get config
    with open("config.yml", 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    url = config['API']['CONTENT']['POST_LATEST']  # Adjust as per your config
    date = config['DATE']
    raw_zone_path = config['PATH']['RAW_ZONE']
    table_name = os.path.splitext(os.path.basename(__file__))[0].split('s2r_')[-1]
    table_path = raw_zone_path + f'/{table_name}/'

    # API_KEY = os.environ.get('API_KEY')
    API_KEY = '2ca92cfc-43ad-4ec6-9f43-353fb6bf7085'

    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': API_KEY
    }

    full_path = table_path + f'{date}.json'
    if not os.path.exists(table_path):
        os.makedirs(table_path)

    last_score = None

    while True:
        # json_file.seek(0)
        batch_data, new_last_score = get_latest_posts_from_api(url=url, headers=headers, symbol=user_symbol, last_score=last_score)
        if not batch_data:
            break

        save_data_to_json(batch_data, full_path)

        if new_last_score is None or new_last_score == last_score:
            # No more data to fetch or no new data
            break
        last_score = new_last_score

    print("Data scraping completed.")
    

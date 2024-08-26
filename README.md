# SBU-BBL CoinMarketCap (CMC) Data Engineering Project

## Overview
This project is designed to facilitate the automated collection, processing, and analysis of cryptocurrency data using the CoinMarketCap API. The project is structured into different stages that handle data acquisition, transformation, and storage, making the data ready for research and analysis.

## Project Structure
The project is organized into several folders and files, each serving a specific role in the data pipeline:

### 1. `config.yml`
This configuration file, located at the root of the project directory, is critical for managing API interactions and file paths. It defines the endpoints used for data retrieval and specifies where raw and processed data should be stored.

- **API**: Contains endpoints for various CoinMarketCap API categories such as `EXCHANGE`, `CRYPTOCURRENCY`, `GLOBAL_METRIC`, `FIAT`, and `CONTENT`.
- **PATH**: Specifies the directory paths for `RAW_ZONE` (where raw data is stored) and `GOLDEN_ZONE` (where processed data is stored).

### 2. `s2r` Folder (Source to Raw)
This folder contains scripts that interact with the CoinMarketCap API (https://coinmarketcap.com/api/documentation/v1/) to collect data. These scripts make API calls, retrieve data, and store the raw JSON responses in the directory specified by `RAW_ZONE` in the `config.yml` file.

### 3. `r2g` Folder (Raw to Golden)
Scripts in this folder process the raw data retrieved by the `s2r` scripts. The data is transformed into a more structured format, CSV files, and stored in the `GOLDEN_ZONE` directory, making it ready for analysis and research.

### 4. `run` Folder
This folder contains automation scripts that streamline the execution of the `s2r` scripts across specified date ranges. The main script within this folder handles the scheduling and logging of these executions, ensuring that each API call is made for the correct date and any errors are logged appropriately.

## Installation
To set up the project on your local machine, follow these steps:

### 1. Clone the Repository:
```bash
git clone https://github.com/<yourusername>/coin-market-cap-data-engineering-project.git
```

### 2. Navigate to the Project Directory: 
```bash
cd coin-market-cap-data-engineering-project
```


### 3. Install Required Python Packages:
```bash
pip install -r requirements.txt
```

# Usage

## Data Collection and processing
The main automation script handles the execution of all necessary data collection scripts for a given date range.

## Steps to Execute the Project:

### 1. Set the Date Range for Data Processing:
In the main script, set your desired start and end dates:

```bash
start_date = datetime(2024, 5, 15)
end_date = datetime(2024, 6, 4)
```

### 2. Modify `s2r_path` Dictionary:

Ensure that the `s2r_path` dictionary includes paths to all the scripts you want to run.

### 3. Run the Automation Script:

Execute the script to start the data collection process:

```bash
python run_sr2.py
```
The script will iterate through the specified date range, executing the relevant `s2r` scripts and storing the data in the `RAW_ZONE`.

### 4. Verify Data Collection:
Ensure all raw data is collected and stored in the `RAW_ZONE` directory as specified in `config.yml`.

### 5. Execute Raw to Golden Transformation:
Manually execute the following scripts to transform the raw data into processed CSV files:

```bash
python r2g_cryptocurrency_quotes_historical.py
python r2g_exchange_quotes_historical.py
```
The other files should already have been run but you can run the other files in the folder as well. The `map` and `info` data generally need to only be updated once since it is a collection of the coin names etc. You can still rerun it just to make sure. It should be a quick rerun. 

# Customization

## Directory Depth: 
You can adjust the maximum directory depth for file searches by modifying the `max_depth` parameter in the `get_directory_file_paths` function.

# Error Handling 
The script is equipped with error handling mechanisms to capture and log any issues that arise during execution. If a script fails, the error will be printed to the console, and the automation process will continue with the next script.

# Resources
For more information on the CoinMarketCap API, visit the official documentation: https://coinmarketcap.com/api/documentation/v1/

# Next Steps

## 1. Automate Raw to Golden Transformation
As a future enhancement, consider writing a script to automate the execution of the `r2g` folder to fully automate the data pipeline and put the script in the `run` folder. Also, try to implement a logging mechanism so that you don't have to rewrite the data and will only add from the most recent date in the folder. You can refer to how the logging mechanism is implemented in any of the scripts in the `s2r` folder. 

## 2. Implement Auto Run Feature
Implement a system where the scripts are run automatically. Use the system tools to set up a timer for when the scripts should be run. Ex. Weekly, Bi-weekly etc. 




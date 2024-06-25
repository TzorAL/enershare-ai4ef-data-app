import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import openpyxl
import os
import csv
import json
from ..utils.LUTs import Stations, Meteorology
from ..utils.db_utils import store_to_db, table_exists_in_schema, get_table_as_df, get_table_last_row_as_df
from dagster import (
    multi_asset, 
    AssetIn, 
    AssetOut, 
    MetadataValue, 
    Output
)
import gc
import time

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

def delete_last_row(csv_file_path):
    temp_file_path = csv_file_path + '.tmp'

    with open(csv_file_path, 'r', newline='') as read_file, open(temp_file_path, 'w', newline='') as write_file:
        reader = csv.reader(read_file)
        writer = csv.writer(write_file)
        
        previous_row = next(reader, None)
        for row in reader:
            writer.writerow(previous_row)
            previous_row = row
    
    os.replace(temp_file_path, csv_file_path)

def extract_last_date(csv_filename):

    storage_df = pd.read_csv(csv_filename)
    print(f'Storage tail: {storage_df.tail()}')
    
    # Get the last date from the DataFrame
    last_date = storage_df["Datums"].iloc[-1].replace('.', '-')
    
    # replace the dots with hyphens
    last_date = last_date.replace('.', '-')
    
    # Split the date string into day, month, and year
    day, month, year = last_date.split('-')
    
    # Rearrange the parts to yy-mm-dd
    last_date = f"{year}-{month}-{day}"
    
    return last_date

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Assets ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

@multi_asset(
    name="latvian_meteo_op",
    group_name="latvian_meteo_assets",
    required_resource_keys={"meteo_config",'db_config'},
    outs={'weather_data': AssetOut(io_manager_key="csv_io_manager")})
async def latvian_meteo_op(context):
    start_date = None
    # current_date = None

    meteo_config = context.resources.meteo_config
    db_confg = context.resources.db_config
    schema_name = db_confg.schema_name

    stations = Stations()
    meteorology = Meteorology()

    # Get all attributes of the classes
    station_attributes = vars(stations)
    meteorology_attributes = vars(meteorology)

    # metadata_dict = {}
    count = 0

    # Print all combinations
    for station_name, station_code in station_attributes.items():
        for meteorology_name, meteorology_code in meteorology_attributes.items():
            
            print(f"Station: {station_name} ({station_code}), Meteorology: {meteorology_name} ({meteorology_code})")
            table_name = f'{station_name.lower()}_{meteorology_name}'

            # data_storage_path = f'{context.resources.csv_io_manager.path_prefix}/{station_name}-{meteorology_name}.csv'

            # check if the file exists and has data
            # if(os.path.exists(data_storage_path) and os.path.getsize(data_storage_path) > 0): 
            #     start_date = extract_last_date(data_storage_path)            
            # else: # If the last_date is None, set the start_date to the initial_date
            #     start_date = meteo_config.initial_date
            
            print(table_name, schema_name)
            if(table_exists_in_schema(table_name, schema_name)):
                table_df = await get_table_last_row_as_df(schema_name, table_name)
                start_date = table_df.iloc[0]['Datums'] # get the last date from the table
                print(f"Last date in the table: {start_date}")

                # Add one day to the start date to avoid fetching the same data
                date_obj = dt.strptime(start_date, "%d.%m.%Y")
                start_date_obj = date_obj + timedelta(days=1)
                start_date = start_date_obj.strftime("%d.%m.%Y")

                context.log.info(f"Last date in the table: {start_date}")
            else:
                start_date = meteo_config.initial_date
                context.log.info(f"Table {table_name} does not exist in the database. Setting the start date to {start_date}")

            # Remove one day from date to avoid adding incomplete current date data
            date_obj = dt.strptime(meteo_config.current_date, "%d.%m.%Y")
            end_date_obj = date_obj - timedelta(days=1)
            end_date = end_date_obj.strftime("%d.%m.%Y")

            print(end_date, start_date)

            # if the end date is less than the start date, skip the current station and meteorology combination
            # This means that db has caught up to current end date and goes after (to incomplete data)
            if(dt.strptime(end_date, "%d.%m.%Y") < dt.strptime(start_date, "%d.%m.%Y")): 
                print(f"{count}. Data for {station_name} ({station_code}) and {meteorology_name} ({meteorology_code}) is up to date.")
                context.log.info(f"{count}. Data for {station_name} ({station_code}) and {meteorology_name} ({meteorology_code}) is up to date.")
                count = count + 1            
                continue

            # Construct the URL with provided parameters
            params = {
                "format": "xls",
                "mode": "meteo",
                "sakuma_datums": dt.strptime(start_date, "%d.%m.%Y").strftime("%Y-%m-%d"),
                "beigu_datums": dt.strptime(end_date, "%d.%m.%Y").strftime("%Y-%m-%d"),
                "stacija_id": station_code,
                "raditaja_id": meteorology_code
            }

            # Construct the URL with parameters
            url_with_params = f"{meteo_config.global_url}?{'&'.join([f'{key}={value}' for key, value in params.items()])}"

            print(f'Current URL: {url_with_params}')
            
            df = None
            try:
                # Read the data from the URL
                df = pd.read_excel(url_with_params, header=2)
            except Exception as e: # If an error occurs, log it and continue to the next station and meteorology combination
                context.log.error(f"Error reading Excel file: {e}")
                # metadata_dict.update({f'{table_name}': 'Failed to update!'})
                # Continue to the next station and meteorology combination
                count = count + 1
                continue
            
            print(f'{count}. Data fetched from {df["Datums"].iloc[0]} till {df["Datums"].iloc[-1]}')

            # Check if last row contains NaN values
            if df.iloc[-1].isnull().any():
                # Remove last row
                df = df.drop(df.index[-1])
            
            if(db_confg.store2db):
                await store_to_db(data=df, table_name=table_name, schema_name=schema_name)

            # metadata_dict.update({f'{table_name}': 'updated!'})
            context.log.info(f"{count}. Data stored in {table_name}")

            # if os.path.exists(f"storage_data/{table_name}.csv"):
            #     # If the file exists, append the data to the file and keep existing header
            #     df.to_csv(f'storage_data/{table_name}.csv', mode='a', index=False, header=False, encoding='utf-8-sig')
            # else:
            #     # If the file doesn't exist, create it and write the data with its header
            #     df.to_csv(f'storage_data/{table_name}.csv', index=False, header=True, encoding='utf-8-sig')
            
            count = count + 1
            
            # Force garbage collection
            gc.collect()
            
        time.sleep(5)  # Example: Sleep for 5 seconds at the end of each station processing

    metadata_dict = {
        "count": count,
        "last_update": dt.now().strftime("%d.%m.%Y %H:%M:%S")
    }
    
    return Output(value=pd.DataFrame.from_dict(metadata_dict, orient='index'), 
                  metadata={
                      "output": json.dumps(metadata_dict),
                      "count_new_updates": count
                })
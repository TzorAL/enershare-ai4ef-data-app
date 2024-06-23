import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text, Table
from dagster import AssetOut, Output, multi_asset, MetadataValue
from ..utils.db_utils import create_schema, store_to_db
import requests
import json
import yaml

# Get the current directory
current_dir = os.path.dirname(__file__)

# Set the path to the .env file in the parent directory
env_path = os.path.join(os.path.dirname(current_dir), '.env')

# Load the .env file
load_dotenv(env_path)

# Access environmental variables
database_url = os.environ.get('DATABASE_URL')
jwt_token = os.environ.get('JWT_TOKEN')
forward_id = os.environ.get('FORWARD_ID')
forward_sender = os.environ.get('FORWARD_SENDER')
connector_url = os.environ.get('CONNECTOR_URL')

# Create engine with database URL
engine = create_engine(database_url, pool_pre_ping=True)

# Headers
headers = {
    'Authorization': 'Bearer' + jwt_token,
    'Forward-Id': forward_id,
    'Forward-Sender': forward_sender
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
def get_endpoints(pilot):
    api_description_url = os.environ.get(f'{pilot.upper()}_API_DESCRIPTION_URL')
    response = requests.get(api_description_url)
    openapi_spec = yaml.safe_load(response.text)  # Load YAML data

    # Extract GET endpoints
    endpoints = []

    # Traverse through paths in the OpenAPI spec
    for path, path_info in openapi_spec['paths'].items():
        # Check if GET method exists for the path
        if 'get' in path_info and path != '/':
            # Extract endpoint URL
            endpoint_url = path
            endpoints.append(endpoint_url.lstrip("/"))  # Remove leading slash
    
    print(endpoints)
    return endpoints

async def request_pilot_data(pilot, endpoint, store2db):

    temp_headers = headers.copy()
    temp_headers['Forward-Id'] = temp_headers['Forward-Id'] + pilot
    print(f'Headers: {temp_headers}')
    api_version = os.environ.get(f'{pilot.upper()}_API_VERSION')

    total_records_pulled = 0
    num_records = 0
    data_frames = []
    message = []

    params = {'limit': 400000, 'offset': 0}

    request_url = f'{connector_url}/{api_version}/{endpoint}'

    while True:
        print(f"Fetching data with limit {params['limit']} and offset {params['offset']}...")
        response = requests.get(request_url, headers=temp_headers, params=params)
        if response.status_code == 200:
            try:
                data = response.json()  # Attempt to decode JSON
                # Append the identified rows to df_existing
                df = pd.DataFrame(data)
                print(df.head())
                data_frames.append(df)
                total_records_pulled += len(df)  # Update total records pulled
                print(f"Fetched {num_records} records. Total records pulled so far: {total_records_pulled}") 
                # print(json.dumps(data[:-1], indent=4))
            except ValueError:  # includes simplejson.decoder.JSONDecodeError
                print("Response content is not valid JSON")
                print(response.text)
                return {"message": "Response content is not valid JSON",
                        "text": response.text}
        else:
            print(f"Request failed with status code: {response.status_code}")
            print("Response text:", response.text)
            return {"message": f"Request failed with status code: {response.status_code}",
                    'text': response.text}

        params['offset'] += 400000
        print(f"Response length: {len(response.json())}")

        if len(response.json()) < 400000:
            # Concatenate all DataFrames into a single DataFrame
            print('Response length is less than 400000. Exiting loop...')
            print("Concatenating all DataFrames into a single DataFrame...")
            final_df = pd.concat(data_frames, ignore_index=True)
            print('Number of entires:', len(final_df))
            if(store2db): 
                await create_schema(pilot.lower()) # create schema if not exists - wait for schema to be created
                if(store_to_db(final_df, endpoint, pilot.lower())): # store df to db
                    message.append({"message": f"\'{endpoint}\' from \'{pilot}\' has been added to db"})
                else: 
                    return {"error": f"Failed to store \'{endpoint}\' from \'{pilot}\' to db"}
            else:
                message.append({"message": f"{endpoint} from {pilot} has been fetched successfully."})

            preview = final_df.head(1).to_json(orient="records", date_format="iso", date_unit="s", default_handler=str)
            message.append({"preview": json.loads(preview)})
            message.append({"number_of_entries": len(final_df)})
            if not os.path.exists(f'../{pilot}'): os.makedirs(f'../{pilot}')
            final_df.to_csv(f'../{pilot}_{endpoint}.csv', header=True, index=False)
            break
    
    print(message)
    return message

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Assets ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

@multi_asset(
    name="get_pilot_data", 
    group_name="ds_assets",
    required_resource_keys={'ds_config'},
    outs={'pilot_data': AssetOut()})
async def get_pilot_data(context):
    ds_config = context.resources.ds_config

    message = await request_pilot_data(ds_config.pilot, ds_config.endpoint, ds_config.store2db)
    
    return Output(value=message, 
                    metadata={
                        "preview": MetadataValue.json(message),
                    })

@multi_asset(
    name="get_all_pilot_data", 
    group_name="ds_assets",
    required_resource_keys={'ds_config'},
    outs={'all_pilot_data': AssetOut()})
async def get_all_pilot_data(context):
    ds_config = context.resources.ds_config

    endpoints = get_endpoints(ds_config.pilot)

    message = []
    
    for endpoint in endpoints:
        endpoint_message = await request_pilot_data(ds_config.pilot, endpoint, ds_config.store2db)
        message.append(endpoint_message)

    return Output(value=message, 
                    metadata={
                        "preview": MetadataValue.json(message),
                    })
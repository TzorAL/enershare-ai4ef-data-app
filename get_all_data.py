import os
from dotenv import load_dotenv
import requests
import yaml
import json
import sys

load_dotenv()
# Access environmental variables
port = os.environ.get('DS_VIZ_PORT')
domain = os.environ.get('DOMAIN')

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
    
    return endpoints

def merge_messages(array_of_arrays_of_dicts):
    flattened_list = []
    for array_of_dicts in array_of_arrays_of_dicts:
        flattened_list.extend(array_of_dicts)
    return flattened_list

def get_all_data(pilot='Pilot7'):
    if len(sys.argv) < 2:
        print("Please provide pilot you want to use (e.g PilotX).")
        return
    
    pilot=sys.argv[1].capitalize()

    endpoints = get_endpoints(pilot)
    print(endpoints)

    message = []
    url = f'http://{domain}:{port}/get_data/'

    for endpoint in endpoints:
        params = {'endpoint': endpoint, 'pilot': pilot, "save": True}
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            # Access the return value (JSON data in this case)
            return_value = response.json()
            message.append(return_value)   
            print("Return value:", return_value)
        else:
            print("Failed to get return value. Status code:", response.status_code)

    merged_message = merge_messages(message)
    print(json.dumps(merged_message, indent=4))

if __name__ == "__main__":
    get_all_data()
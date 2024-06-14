from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import httpx
import uvicorn
import pandas as pd
import json
import requests
import json
import os
from dotenv import load_dotenv

from sqlalchemy import create_engine, MetaData, text, Table
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker, Session
from fastapi import  Depends

from typing import Optional
import yaml
#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Globals ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

tags_metadata = [
    {"name": "Schemas", "description": "Schema-related endpoints"},
    {"name": "Tables", "description": "Table-related endpoints"},
    {"name": "Data", "description": "Data-related endpoints"},
]

app = FastAPI(
    title="Data request from dataspace API",
    description="Collection of REST APIs for requesting data from dataspace",
    version="0.0.1",
    openapi_tags=tags_metadata,
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load variables from .env file
load_dotenv()

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

#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Create a dependency to get the database session
def get_db_session():
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        yield db
    finally:
        db.close()

# Create a dependency to get the database metadata
def get_metadata(schema='public'):
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=schema)    
    return metadata

def create_dataframe(data):
    if isinstance(data, dict) and all(isinstance(value, (int, float, str, bool)) for value in data.values()):
        data = [data]  # Wrap in a list to treat it as a single row
    return pd.DataFrame(data)

def store_to_db(data, table_name, pilot):
    try:
        if isinstance(data, pd.DataFrame):
            data.to_sql(table_name, engine, schema=pilot, if_exists='replace', index=False)
            print("Data has been successfully stored in the database.")
        else:
            # Convert DataFrame to SQL table
            create_dataframe(data).to_sql(table_name, engine, schema=pilot, if_exists='replace', index=False)
            print("Data has been successfully stored in the database.")
    except Exception as e:
            print("An error occurred while storing data in the database:", e)
            return False
    return True

# Function to create schema
async def create_schema(schema_name):
    try:
        with engine.connect() as connection:
            if engine.dialect.has_schema(connection, schema_name):
                print(f"Schema \"{schema_name}\" succesfully created")
            else:
                connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema_name};'))
                # connection.execute(CreateSchema(schema_name, if_not_exists=True))                
                connection.commit()
                print(f"Schema \"{schema_name}\" succesfully created")
    except Exception as e:
        print("An error occurred while creating scheama in the database:", e)

async def find_schemas():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT schema_name FROM information_schema.schemata"))
        schema_names = [row[0] for row in result.fetchall()]
        non_system_schemas = [schema for schema in schema_names if not schema.startswith("pg_") and schema != "information_schema"]
        return non_system_schemas  

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

#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Endpoints ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@app.get('/print_schemas',tags=['Schemas'])
async def print_schemas():
    return {"non_system_schemas": await find_schemas()}

@app.post("/upload-csv/", tags=["Data"])
async def upload_csv(csv_file: UploadFile = File(...), 
                     table_name: str = Query('efcomp', description="Custom table name"),
                     schema_name: str = Query('pilot7', description="Custom schema name")):
    try:
        with engine.connect() as connection:
            await create_schema(schema_name.lower()) # create schema if not exists - wait for schema to be created
            df = pd.read_csv(csv_file.file)
            df.to_sql(table_name, connection, schema=schema_name, if_exists='replace', index=False)
            # metadata.clear()
            # metadata.reflect(bind=engine)
            get_metadata()
            return {"message": f"CSV file uploaded and inserted into the '{table_name}' table successfully."}
    except Exception as e:
        return {"message": f"An error occurred: {str(e)}"}

# TODO: Add a new endpoint to get all data from all endpoints
# TODO: Add option to get last n records from a table
@app.get("/get_data", tags=['Data'])
async def get_data(endpoint: str = Query('efcomp', description="API endpoint defined in API description file"),
                    pilot: str = Query('Pilot7', description="Pilot name as mentioned in dataspace agent (e.g. 'Pilot7')"),
                    save: bool = Query(True, description='Option to save data to database')):

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
            if(save): 
                await create_schema(pilot.lower()) # create schema if not exists - wait for schema to be created
                if(store_to_db(final_df, endpoint, pilot.lower())): # store df to db
                    message.append({"message": f"\'{endpoint}\' from \'{pilot}\' has been added to db"})
                else: 
                    return {"error": f"Failed to store \'{endpoint}\' from \'{pilot}\' to db"}

            preview = final_df.head(1).to_json(orient="records", date_format="iso", date_unit="s", default_handler=str)
            message.append({"preview": json.loads(preview)})
            message.append({"number_of_entries": len(final_df)})
            final_df.to_csv('data.csv', header=True, index=False)
            break
    
    print(message)
    return message
            
@app.get("/get_all_data", tags=['Data'])
async def get_all_data(pilot: str = Query('Pilot7', description="Pilot name as mentioned in dataspace agent (e.g. 'Pilot7')"),
                       save: bool = Query(True, description='Option to save data to database')):

    endpoints = get_endpoints(pilot)

    message = []
    port = os.environ.get('port')
    
    for endpoint in endpoints:
        params = {'endpoint': endpoint, 'pilot': pilot, "save": save}
        
        response = requests.get(f'http://localhost:{port}/get_data/', params=params)
        if response.status_code == 200:
            # Access the return value (JSON data in this case)
            return_value = response.json()
            message.append(return_value[0]["message"])
            print("Return value:", return_value)
        else:
            print("Failed to get return value. Status code:", response.status_code)
        
    return message

# Endpoint to get a list of tables in the database
@app.get("/get_tables", tags=["Tables"])
async def get_tables():

    non_system_schemas = []
    try:
        non_system_schemas = await find_schemas()
    except Exception as e:
        print("An error occurred while searching schemas in the database:", e)

    print(non_system_schemas)

    try:
        tables = []
        for schema_name in non_system_schemas:
            metadata = get_metadata(schema=schema_name)
            for table_name in metadata.tables:
                tables.append({"table": table_name, "schema": schema_name})
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to get the first entries of a table 
@app.get("/get_table_preview", tags=["Tables"])
async def get_table_preview(
    table_name: str = Query('pilot7.efcomp', description="Schema.table name"),
    db: Session = Depends(get_db_session),
):
    schema_name = table_name.split('.')[0]
    metadata = get_metadata(schema=schema_name)

    # Check if the table exists
    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    # Get the table from metadata
    table = metadata.tables[table_name]

    # Query the database to get all rows from the table
    result = db.query(table).all()

    # Convert the result to a DataFrame
    df = pd.DataFrame(result)

    # Convert DataFrame to JSON string with handling of non-serializable types
    json_str = df.head(100).to_json(orient="records", date_format="iso", date_unit="s", default_handler=str)

    # Parse JSON string back to Python objects
    return json.loads(json_str)

# TODO: In case you drop the last table of a schema, delete the schema as well
@app.delete("/drop-table/", tags=["Tables"])
async def drop_table(table_name: str = Query('pilot7.efcomp', description="Schema.table name")):
    schema_name = table_name.split('.')[0]

    try:
        metadata = get_metadata(schema=schema_name)
        # Check if the table exists before dropping it
        
        # Load the table metadata
        table = Table(table_name, metadata, autoload_with=engine)
        # Drop the table
        table.drop(engine)

        return {"message": f"Table '{table_name}' in schema '{schema_name}' dropped successfully."}
    except NoSuchTableError:
        return {"message": f"Table '{table_name}' in schema '{schema_name}' does not exist."}
    except Exception as e:
        return {"message": f"An error occurred: {str(e)}"}
    
@app.get("/")
async def root():
    return {"message": "Congratulations! Your API is working as expected. Now head over to http://localhost:8889/docs"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8889, reload=True)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import uvicorn
import pandas as pd
import json
import requests
import json
import os
from dotenv import load_dotenv

from sqlalchemy import create_engine, MetaData, text
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
        # Convert DataFrame to SQL table
        create_dataframe(data).to_sql(table_name, engine, schema=pilot, if_exists='replace', index=False)
        print("Data has been successfully stored in the database.")
    except Exception as e:
        print("An error occurred while storing data in the database:", e)

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

def get_endpoints(openapi_url):
    response = requests.get(openapi_url)
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

#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Endpoints ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@app.get('/print_schemas',tags=['Schemas'])
async def print_schemas():
    return {"non_system_schemas": await find_schemas()}

@app.get("/get_data", tags=['Data'])
async def get_data( api_version: str  = '0.5',
                    endpoint: str = 'efcomp',
                    pilot: str = 'Pilot7',
                    save: bool = True):

    temp_headers = headers.copy()
    temp_headers['Forward-Id'] = temp_headers['Forward-Id'] + pilot
    # print(temp_headers)

    request_url = f'{connector_url}/{api_version}/{endpoint}/'
    # print(request_url)
    response = requests.get(request_url, headers=temp_headers)
    # data = ["1","2","3","4"]
    # Check if request was successful (status code 200)
    if response.status_code == 200:
        try:
            print(f"Response size: {len(response.content)} bytes")
            data = response.json()  # Attempt to decode JSON
            if(save): 
                await create_schema(pilot.lower()) # create schema if not exists - wait for schema to be created
                store_to_db(data, endpoint, pilot.lower()) # store df to db
                print(data)
            return {"message": f"{endpoint} from {pilot} has been added to db"} #data[:10]
        except ValueError:  # includes simplejson.decoder.JSONDecodeError
            print("Response content is not valid JSON")
            print(response.text)
            return {"message": "Response content is not valid JSON",
                    "response_text": response.text}
        except Exception as e:
            # Handle any exceptions that might occur during JSON parsing or other operations
            print("Error:", e)
            return {"Error:", e} # Return None or handle the error as appropriate
    else:
        print(f"Request failed with status code: {response.status_code}")
        print("Response text:", response.text)
        return {"message": f"Request failed with status code: {response.status_code}"}


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
@app.get("/get_table", tags=["Tables"])
async def get_table_as_df(
    table_name: str = 'pilot7.efcomp',
    schema_name: str = 'pilot7',
    db: Session = Depends(get_db_session),
):

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

    return df.to_dict(orient="records")

@app.get("/")
async def root():
    return {"message": "Congratulations! Your API is working as expected. Now head over to http://localhost:8889/docs"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9876, reload=True)

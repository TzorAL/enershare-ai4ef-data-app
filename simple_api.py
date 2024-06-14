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
# TODO: Add commented line below in a .env file to set the DATABASE_URL
# DATABASE_URL=postgresql://ds_viz_user:ds_viz_password_123@postgres_db:5432/ds_viz_db
database_url = os.environ.get('DATABASE_URL') 
# Create engine with database URL
engine = create_engine(database_url, pool_pre_ping=True)
if not database_url:
    raise ValueError("DATABASE_URL environment variable is not set")

print(f"Connecting to database with URL: {database_url}")

try:
    engine = create_engine(database_url, pool_pre_ping=True)
    # Test connection
    with engine.connect() as connection:
        result = connection.execute("SELECT 1")
        print(f"Connection test result: {result.fetchone()}")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    
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

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ API Endpoints ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@app.get('/print_schemas',tags=['Schemas'])
async def print_schemas():
    return {"non_system_schemas": await find_schemas()}

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

    return df.to_dict(orient="records")

@app.post("/upload-csv/", tags=["Data"])
async def upload_csv(csv_file: UploadFile = File(...), 
                     table_name: str = Query('efcomp', description="Custom table name"),
                     schema_name: str = Query('pilot7', description="Custom schema name")):
    try:
        with engine.connect() as connection:
            df = pd.read_csv(csv_file.file)
            df.to_sql(table_name, connection, schema=schema_name, if_exists='replace', index=False)
            # metadata.clear()
            # metadata.reflect(bind=engine)
            get_metadata()
            return {"message": f"CSV file uploaded and inserted into the '{table_name}' table successfully."}
    except Exception as e:
        return {"message": f"An error occurred: {str(e)}"}


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
    return {"message": "Congratulations! Your API is working as expected. Now head over to http://localhost:7654/docs"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7654, reload=True)
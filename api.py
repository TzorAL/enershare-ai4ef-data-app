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

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, Session
from fastapi import  Depends

#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Globals ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

tags_metadata = [
    {"name": "Get table dataframe", "description": "get table as pandas dataframe"},
    {"name": "Get tables list", "description": "get list of tables in db"},
    {"name": "Get table dataframe", "description": "get table as pandas dataframe"},
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

# API endpoint
# url = 'https://enershare.epu.ntua.gr/provider-data-app/openapi/0.5/'    # https://<baseurl>/<data-app-path>/openapi/<beckend-service-version>/
# endpoint = 'efcomp'                                                     # API endpoint 

# Load variables from .env file
load_dotenv()

# Access environmental variables
database_url = os.environ.get('DATABASE_URL')
jwt_token = os.environ.get('JWT_TOKEN')
forward_id = os.environ.get('FORWARD_ID')
forward_sender = os.environ.get('FORWARD_SENDER')

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
def get_metadata():
    metadata = MetaData()
    metadata.reflect(bind=engine)    
    return metadata

def store_to_db(data, table_name):

    df = pd.DataFrame(data)
    # print(df.head())
    # return
    try:
        # Convert DataFrame to SQL table
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("Data has been successfully stored in the database.")
    except Exception as e:
        print("An error occurred while storing data in the database:", e)
        
#  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Endpoints ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@app.post("/get_all_data", tags=['Get Data'])
async def get_all_data( url: str  = 'http://enershare.epu.ntua.gr/provider-data-app/openapi/0.5/',
                        endpoint: str = 'efcomp',
                        store_2_db: bool = False):

    response = requests.get(url+endpoint, headers=headers)

    # Check if request was successful (status code 200)
    if response.status_code == 200:
        try:
            data = response.json()  # Attempt to decode JSON
            if(store_2_db): store_to_db(data, endpoint)
            return data
        except ValueError:  # includes simplejson.decoder.JSONDecodeError
            print("Response content is not valid JSON")
            print(response.text)
    else:
        print(f"Request failed with status code: {response.status_code}")
        print("Response text:", response.text)

# Endpoint to get a list of tables in the database
@app.get("/get_tables", tags=["Get tables list"])
async def get_tables(metadata: MetaData = Depends(get_metadata)):
    try:
        return {"tables": list(metadata.tables.keys())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to get the first entries of a table 
@app.get("/get_table", tags=["Get table dataframe"])
async def get_table_as_df(
    table_name: str,
    db: Session = Depends(get_db_session),
    metadata: MetaData = Depends(get_metadata)
):
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

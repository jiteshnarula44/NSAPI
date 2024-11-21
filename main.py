import os
import time
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from io import BytesIO
from dotenv import load_dotenv
import requests
from requests_oauthlib import OAuth1
from azure.storage.blob import BlobServiceClient
from pandas import json_normalize

# Load environment variables
load_dotenv()

# Initialize FastAPI and Blob Storage client
app = FastAPI()
blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
container_name = "testcontainer"

class DateRange(BaseModel):
    start_date: str
    end_date: str

def extract_value_or_text(data, return_value=False):
    """Extract text or value from nested lists/dictionaries."""
    if isinstance(data, list) and len(data) > 0:
        return data[0]['text'] if not return_value else data[0]['value']
    elif isinstance(data, dict):
        return data.get('text') if not return_value else data.get('value')
    return None

def fetch_netsuite_data(start_date: str, end_date: str, mo_start_date: str, filter_type: int) -> dict:
    url = "https://452948.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=104&deploy=1"
    consumer_key = os.getenv("NETSUITE_CONSUMER_KEY")
    consumer_secret = os.getenv("NETSUITE_CONSUMER_SECRET")
    token = os.getenv("NETSUITE_TOKEN")
    token_secret = os.getenv("NETSUITE_TOKEN_SECRET")
    netsuite_account = os.getenv("NETSUITE_ACCOUNT")

    auth = OAuth1(consumer_key, consumer_secret, token, token_secret, signature_method='HMAC-SHA256', realm=netsuite_account)

    filters = []

    if filter_type == 2:
        filters = [
            [["trandate", "within", start_date, end_date]],
            "AND",
            ["trandate", "after", "01/01/2017"],
            "AND",
            ["mainline", "is", "F"],
            "AND",
            ["type", "anyof", "CustCred", "Estimate", "SalesOrd", "CustInvc"]
        ]
    elif filter_type == 3:
        filters = [
            [["lastmodifieddate", "within", mo_start_date, end_date]],
            "AND",
            ["trandate", "after", "01/01/2017"],
            "AND",
            ["mainline", "is", "F"],
            "AND",
            ["type", "anyof", "CustCred", "Estimate", "SalesOrd", "CustInvc"]
        ]

    try:
        response = requests.post(
            url,
            auth=auth,
            headers={"Content-Type": "application/json"},
            json={
                "type": "TRANSACTION",
                "columns": [
                    {"name": "amount"},
                    {"name": "closedate"},
                    {"name": "createdby"},
                    {"name": "tranid", "join": "createdfrom"},
                    {"name": "trandate", "join": "createdfrom"},
                    {"name": "entity"},
                    {"name": "trandate"},
                    {"name": "datecreated"},
                    {"name": "custentity_dealer_alignment", "join": "customer"},
                    {"name": "custbody57"},
                    {"name": "custbody_from_cet"},
                    {"name": "displayname", "join": "item"},
                    {"name": "item"},
                    {"name": "rate"},
                    {"name": "entitystatus"},
                    {"name": "custbody102"},
                    {"name": "custbodypm_contact"},
                    {"name": "tranid"},
                    {"name": "quantity"},
                    {"name": "salesrep"},
                    {"name": "shipcity"},
                    {"name": "shipcountry"},
                    {"name": "shipdate"},
                    {"name": "shipstate"},
                    {"name": "shipzip"},
                    {"name": "statusref"},
                    {"name": "type"},
                    {"name": "custbody13"},
                    {"name": "lastmodifieddate"},
                    {"name": "recordtype"},
                    {"name": "ordertype"},
                    {"name": "linesequencenumber"},
                    {"name": "custitem8", "join": "item"},
                    {"name": "custitem_esp_item_category", "join": "item"},
                    {"name": "custitem_esp_item_subcategory", "join": "item"},
                    {"name": "custitem_esp_cus_item_brand", "join": "item"}
                ],
                "filters": filters,
            },
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def filter_and_process_data(json_data: dict) -> pd.DataFrame:
    """Normalize JSON and process nested data columns."""
    df = json_normalize(json_data['pages'], record_path='data')

    for column in df.columns:
        if df[column].notnull().all() and isinstance(df[column].iloc[0], (list, dict)):
            df[column] = df[column].apply(lambda x: extract_value_or_text(x, return_value=False))

    if 'values.tranid' in df.columns and 'values.linesequencenumber' in df.columns:
        df['row_number'] = df.groupby(['values.tranid', 'values.linesequencenumber']).cumcount() + 1
        df = df[df['row_number'] == 1]
        df.drop(columns=['row_number'], inplace=True, errors='ignore')

    return df

def upload_to_blob(df: pd.DataFrame, filename: str):
    """Upload DataFrame as CSV to Azure Blob Storage."""
    try:
        csv_data = df.to_csv(index=False)
        byte_data = BytesIO(csv_data.encode('utf-8'))

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
        blob_client.upload_blob(byte_data, overwrite=True)
        print(f"Data uploaded to blob storage: {filename}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading to blob storage: {str(e)}")

def get_chunks(start_date: datetime, end_date: datetime, chunk_size_days: int):
    """Generate non-overlapping date ranges (chunks) for the given period."""
    chunks = []
    current_start_date = start_date

    while current_start_date < end_date:
        current_end_date = min(current_start_date + timedelta(days=chunk_size_days), end_date)
        chunks.append((current_start_date, current_end_date))
        current_start_date = current_end_date + timedelta(days=1)

    return chunks

@app.post("/netsuite-data-combined")
async def get_combined_netsuite_data(date_range: DateRange):
    try:
        start_time = time.time()
        start_date = datetime.strptime(date_range.start_date, "%m/%d/%Y")
        end_date = datetime.strptime(date_range.end_date, "%m/%d/%Y")
        mo_start_date = (end_date - timedelta(days=3)).strftime("%m/%d/%Y")

        combined_df = pd.DataFrame()

        # Generate chunks
        chunks = get_chunks(start_date, end_date, chunk_size_days=40)

        # Fetch and process data in chunks
        for current_start_date, current_end_date in chunks:
            json_data = fetch_netsuite_data(
                current_start_date.strftime("%m/%d/%Y"),
                current_end_date.strftime("%m/%d/%Y"),
                mo_start_date,
                filter_type=2
            )
            df = filter_and_process_data(json_data)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Calculate elapsed time
        elapsed_time = time.time() - start_time

        # Generate filename with today's date and runtime enclosed in {}
        today_date = datetime.now().strftime("%Y%m%d")
        filename = f"netsuite_data_{today_date}_{{{elapsed_time:.2f}}}.csv"

        # Upload data to blob
        upload_to_blob(combined_df, filename)

        return {"message": f"Data processed and uploaded successfully in {elapsed_time:.2f} seconds."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

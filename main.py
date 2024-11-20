import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
import requests
from requests_oauthlib import OAuth1
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import time

# Load environment variables
load_dotenv()
app = FastAPI()

class DateRange(BaseModel):
    start_date: str
    end_date: str

# Initialize Azure Blob Storage client
blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))

def extract_value_or_text(data: dict, field_name: str, return_value: bool = True):
    if isinstance(data, dict) and 'value' in data and 'text' in data:
        return data['value'] if return_value else data['text']
    return data

def fetch_netsuite_data(start_date: str, end_date: str, mo_start_date: str, filter_type: int) -> dict:
    url = "https://452948.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=104&deploy=1"
    consumer_key = os.getenv("NETSUITE_CONSUMER_KEY")
    consumer_secret = os.getenv("NETSUITE_CONSUMER_SECRET")
    token = os.getenv("NETSUITE_TOKEN")
    token_secret = os.getenv("NETSUITE_TOKEN_SECRET")
    netsuite_account = os.getenv("NETSUITE_ACCOUNT")

    auth = OAuth1(consumer_key, consumer_secret, token, token_secret, signature_method='HMAC-SHA256', realm=netsuite_account)

    filters = []

    if filter_type == 2:  # Filter 2: Based on trandate
        filters = [
            [["trandate", "within", start_date, end_date]],
            "AND",
            ["trandate", "after", "01/01/2017"],
            "AND",
            ["mainline", "is", "F"],
            "AND",
            ["type", "anyof", "CustCred", "Estimate", "SalesOrd", "CustInvc"]
        ]
    elif filter_type == 3:  # Filter 3: Based on lastmodifieddate
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
                    {"name": "amount"}, {"name": "closedate"}, {"name": "createdby"}, {"name": "tranid", "join": "createdfrom"},
                    {"name": "trandate", "join": "createdfrom"}, {"name": "entity"}, {"name": "trandate"}, {"name": "datecreated"},
                    {"name": "custentity_dealer_alignment", "join": "customer"}, {"name": "custbody57"}, {"name": "custbody_from_cet"},
                    {"name": "displayname", "join": "item"}, {"name": "item"}, {"name": "rate"}, {"name": "entitystatus"},
                    {"name": "custbody102"}, {"name": "custbodypm_contact"}, {"name": "tranid"}, {"name": "quantity"},
                    {"name": "salesrep"}, {"name": "shipcity"}, {"name": "shipcountry"}, {"name": "shipdate"}, {"name": "shipstate"},
                    {"name": "shipzip"}, {"name": "statusref"}, {"name": "type"}, {"name": "custbody13"}, {"name": "lastmodifieddate"},
                    {"name": "recordtype"}, {"name": "ordertype"}, {"name": "linesequencenumber"}
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
    # Normalize the JSON response to a DataFrame
    df = pd.json_normalize(json_data['pages'], record_path='data')

    # Extract nested values if needed
    for column in df.columns:
        if isinstance(df[column].iloc[0], dict):
            df[column] = df[column].apply(lambda x: extract_value_or_text(x, column, return_value=True))

    return df

def save_to_blob(csv_data: BytesIO, blob_name: str):
    container_name = "testcontainer"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_data, blob_type="BlockBlob", overwrite=True)

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

        chunks = get_chunks(start_date, end_date, chunk_size_days=40)

        # Process data in chunks
        for current_start_date, current_end_date in chunks:
            json_data_filter2 = fetch_netsuite_data(
                current_start_date.strftime("%m/%d/%Y"), current_end_date.strftime("%m/%d/%Y"), mo_start_date, filter_type=2)
            if "error" in json_data_filter2:
                raise HTTPException(status_code=500, detail=json_data_filter2["error"])

            df_filter2 = filter_and_process_data(json_data_filter2)
            combined_df = pd.concat([combined_df, df_filter2], ignore_index=True)

        # Process the data for Filter 3 (based on `lastmodifieddate`)
        json_data_filter3 = fetch_netsuite_data(
            start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y"), mo_start_date, filter_type=3)
        if "error" in json_data_filter3:
            raise HTTPException(status_code=500, detail=json_data_filter3["error"])

        df_filter3 = filter_and_process_data(json_data_filter3)
        combined_df = pd.concat([combined_df, df_filter3], ignore_index=True)

        # Remove unnecessary columns
        columns_to_drop = [
            'values.createdby', 'values.entity', 'values.customer.custentity_dealer_alignment', 'values.item',
            'values.entitystatus', 'values.custbody102', 'values.custbodypm_contact', 'values.salesrep',
            'values.shipcountry', 'values.statusref', 'values.type', 'values.custbody13'
        ]
        combined_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

        # Filter out any rows with item_new == '-Not Taxable-'
        if 'values.item_new' in combined_df.columns:
            combined_df = combined_df[combined_df['values.item_new'] != '-Not Taxable-']

        # Deduplication logic
        if 'values.tranid' in combined_df.columns and 'values.linesequencenumber' in combined_df.columns:
            combined_df['row_number'] = combined_df.groupby(['values.tranid', 'values.linesequencenumber']).cumcount() + 1
            combined_df = combined_df[combined_df['row_number'] == 1]
            combined_df.drop(columns=['row_number'], inplace=True, errors='ignore')

        # Save to Blob Storage
        output_combined = BytesIO()
        combined_df.to_csv(output_combined, index=False)
        output_combined.seek(0)

        # Calculate runtime and save file to Blob
        end_time = time.time()
        runtime_seconds = round(end_time - start_time, 2)
        current_date = datetime.now().strftime("%m-%d-%Y")
        blob_name_combined = f"netsuite_combined_filtered_data_{current_date}_{{{runtime_seconds}}}.csv"
        save_to_blob(output_combined, blob_name_combined)

        return {"message": "Data processed and saved successfully", "runtime_seconds": runtime_seconds}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

import requests
import pandas as pd
import time
import json
import os
from datetime import datetime, timezone
from google.cloud import storage
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv() #loads .env file when running locally 

APP_TOKEN       = os.environ["SOCRATA_APP_TOKEN"]          # required, no default
BUCKET_NAME     = os.environ["GCS_BUCKET_NAME"]            # required, no default
service_account_json = os.environ["SERVICE_ACCOUNT_JSON"]  # required, no default
DEFAULT_START   = os.environ.get("DEFAULT_START", "2025-01-01T00:00:00.000Z")
PAGE_SIZE       = int(os.environ.get("PAGE_SIZE", "50000"))

METADATA_PREFIX = "metadata/"
DATA_PREFIX = "bronze/complaints/"
INCOME_PREFIX = "bronze/median_income/"
STATE_BLOB = METADATA_PREFIX + "state.json"


def authenticate(service_account):
    client = storage.Client.from_service_account_json(service_account)
    bucket = client.bucket(BUCKET_NAME)
    return bucket

def read_state(bucket):
    blob = bucket.blob(STATE_BLOB)
    if blob.exists():
        state = json.loads(blob.download_as_text())
        last_updated_at = state.get("last_updated_at", DEFAULT_START)
    else:
        last_updated_at = DEFAULT_START
    # print(f"Resuming from: {last_updated_at}")
    return last_updated_at


def write_state(bucket, last_updated_at):
    blob = bucket.blob(STATE_BLOB)
    blob.upload_from_string(
        json.dumps({"last_updated_at": last_updated_at}),
        content_type="application/json"
    )


def complaints(service_account_json):

    bucket = authenticate(service_account_json)

    last_updated_at = read_state(bucket)
    max_updated_at = last_updated_at
    offset = 0

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    local_file = f"data_{run_timestamp}.ndjson"
    data_blob = DATA_PREFIX + f"data_{run_timestamp}.ndjson"

    fetched_any = False
    print(f"Starting data fetch from {last_updated_at} with offset {offset}")

    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )))

    try:
        while True:
            response = session.get(
                "https://data.cityofnewyork.us/resource/erm2-nwe9.json?"
                "$select=*,:updated_at,:created_at,:version,:id&"
                f"$where=:updated_at>'{last_updated_at}' AND created_date>='2025-01-01T00:00:00'&"
                f"$order=:updated_at ASC, unique_key ASC&$limit={PAGE_SIZE}&"
                f"$offset={offset}&$$app_token={APP_TOKEN}"
            )

            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and "errorCode" in data:
                print(f"API error: {data.get('message', data)}")
                break

            print(f"Response ({len(data)} records): {data[:2] if isinstance(data, list) else data}")

            if len(data) == 0:
                print("No more data to fetch.")
                break

            print(f"Fetched {len(data)} records, offset: {offset}")

            df = pd.DataFrame(data)
            df.to_json(local_file, orient="records", lines=True, mode="a")
            fetched_any = True

            page_max = df[":updated_at"].max()
            if page_max > max_updated_at:
                max_updated_at = page_max

            if len(data) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            time.sleep(2)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if fetched_any:
            print(f"Uploading {local_file} to gs://{BUCKET_NAME}/{data_blob}")
            bucket.blob(data_blob).upload_from_filename(local_file)
            write_state(bucket, max_updated_at)
            print(f"State updated to {max_updated_at}")
            os.remove(local_file)
        else:
            print("No data fetched, skipping upload and state update.")


def median_income(service_account_json):
    bucket = authenticate(service_account_json)
    response = requests.get(
        "https://api.census.gov/data/2022/acs/acs5/subject?get=NAME,S1901_C01_012E&for=zip%20code%20tabulation%20area:*"
    )
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    local_file = "median_income.parquet"
    data_blob = INCOME_PREFIX + f"median_income.parquet"
    df.to_parquet(local_file, index=False)
    bucket.blob(data_blob).upload_from_filename(local_file)
    os.remove(local_file)
    print(f"Median income data uploaded to gs://{BUCKET_NAME}/{data_blob}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', choices=['complaints', 'median_income'], required=True)
    args = parser.parse_args()

    if args.task == 'complaints':
        complaints(service_account_json)
    else:
        median_income(service_account_json)


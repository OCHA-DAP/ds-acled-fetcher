import requests
import pandas as pd
import logging
import ocha_stratus as stratus
from datetime import datetime
from constant import *


# --- AUTH: OAuth2 password flow ---
def get_token():
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "password",
            "username": USERNAME,
            "password": PASSWORD,
            "client_id": "acled"
        }
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_raw_data():
    try:
        date_check = stratus.load_blob_data("acled_conflict/download_date.parquet", stage="dev", container_name="projects/acled-conflict")

        if pd.to_datetime(date_check["acled_download_date"].iloc[0]).date() == datetime.today().date():
            logging.debug("ACLED DATA WERE ALREADY DOWNLOADED TODAY")

    except Exception:
        logging.debug("No existing download date found or failed to read. Proceeding to download.")

    logging.debug("Downloading ACLED data")

    token = get_token()

    params = {
        "limit": 0,
        "_format": "json",
        "event_date_where": ">",
        "event_date": "2026-02-27"
    }

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(API_BASE_URL, params=params, headers=headers)
    response.raise_for_status()

    data = response.json().get("data", [])

    # Convert to DataFrame
    df_acled = pd.DataFrame(data)

    # Save download date
    date_df = pd.DataFrame({
        "acled_download_date": [datetime.today().date()]
    })

    stratus.upload_csv_to_blob(date_df, "download_date.csv",  container_name="projects/acled-conflict")
    stratus.upload_csv_to_blob(df_acled, "raw.csv", container_name="projects/acled-conflict")

    logging.info("ACLED data downloaded and saved")
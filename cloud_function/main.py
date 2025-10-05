"""
Google Cloud Function for ingesting earthquake data from the USGS API.

This function is triggered via HTTP. It fetches earthquake data from the
USGS GeoJSON API for a predefined time range (e.g., the last 365 days)
and uploads the raw JSON data to a specified Google Cloud Storage bucket,
serving as the pipeline's Bronze layer.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import storage

# --- Variables Configuration ---
USGS_API_BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
DESTINATION_BLOB_NAME = "bronze/raw_earthquakes.json"

# --- Global Clients ---
# Initialize clients outside the function handler to leverage connection reuse,
# improving performance and avoiding cold starts on every invocation.
try:
    storage_client = storage.Client()
except Exception as e:
    logging.error("Failed to initialize Google Cloud Storage client: %s", e)
    storage_client = None

def fetch_and_store_usgs_data(request):
    """
    Main entry point for the HTTP-triggered Cloud Function. Fetches USGS
    earthquake data and stores it in Google Cloud Storage.

    Args:
        request (flask.Request): The HTTP request object. Although not used in
                                 this function's logic, it is required by the
                                 Cloud Functions framework.

    Returns:
        A tuple containing a response message and an HTTP status code.
        e.g., ("Success message", 200) or ("Error message", 500)
    """
    
    if not storage_client:
        error_msg = "Storage client is not initialized. Cannot proceed."
        logging.critical(error_msg)
        return (error_msg, 500)

    if not GCS_BUCKET_NAME:
        error_msg = "GCS_BUCKET_NAME environment variable is not set."
        logging.critical(error_msg)
        return (error_msg, 500)

    logging.info("Starting USGS earthquake data ingestion...")

    # --- 1. Define API Query Parameters ---
    # Fetch data for the last 365 days from the current time in UTC.
    end_datetime_utc = datetime.now(timezone.utc)
    start_datetime_utc = end_datetime_utc - timedelta(days=365)

    params = {
        'format': 'geojson',
        'starttime': start_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S'),
        'endtime': end_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S'),
        'minmagnitude': 2.5,
        'limit': 20000  # Max limit supported by the API
    }
    
    try:
        # --- 2. Fetch Data from USGS API ---
        logging.info("Requesting data from USGS API with params: %s", params)
        response = requests.get(USGS_API_BASE_URL, params=params, timeout=120)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        
        earthquake_data = response.json()
        feature_count = len(earthquake_data.get('features', []))
        logging.info("Successfully received data for %d earthquakes from API.", feature_count)
        
        # --- 3. Upload Data to Google Cloud Storage ---
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(DESTINATION_BLOB_NAME)
        
        blob.upload_from_string(
            json.dumps(earthquake_data),
            content_type='application/json'
        )
        
        success_message = f"Successfully uploaded raw data to gs://{GCS_BUCKET_NAME}/{DESTINATION_BLOB_NAME}"
        logging.info(success_message)
        return (success_message, 200)

    except requests.exceptions.RequestException as e:
        error_message = f"HTTP request to USGS API failed: {e}"
        logging.error(error_message, exc_info=True)
        return (error_message, 500)
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logging.error(error_message, exc_info=True)
        return (error_message, 500)
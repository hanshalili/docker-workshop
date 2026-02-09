import os
import sys
import urllib.request
from google.cloud import storage
from google.api_core.exceptions import Forbidden, NotFound

# ================= CONFIG =================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")  # must be set
BUCKET_NAME = os.getenv("GCS_BUCKET")         # must be set
# ==========================================

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]  # Jan–Jun
DOWNLOAD_DIR = "data"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def preflight():
    if not GCP_PROJECT_ID:
        print("❌ GCP_PROJECT_ID env var not set")
        sys.exit(1)

    if not BUCKET_NAME:
        print("❌ GCS_BUCKET env var not set")
        sys.exit(1)

    print("✅ Using ADC (no JSON)")
    print("Project:", GCP_PROJECT_ID)
    print("Bucket:", BUCKET_NAME)

def create_bucket_if_needed(client):
    try:
        client.get_bucket(BUCKET_NAME)
        print(f"✅ Bucket exists: {BUCKET_NAME}")
    except NotFound:
        print(f"Creating bucket: {BUCKET_NAME}")
        bucket = storage.Bucket(client, name=BUCKET_NAME)
        bucket.location = "US"
        client.create_bucket(bucket)
        print(f"✅ Bucket created: {BUCKET_NAME}")
    except Forbidden:
        print(f"❌ Bucket name not available: {BUCKET_NAME}")
        print("Pick a new globally-unique bucket name")
        sys.exit(1)

def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    local_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    if os.path.exists(local_path):
        print(f"Skipping download (exists): {local_path}")
        return local_path

    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, local_path)
    return local_path

def upload_file(client, local_path):
    bucket = client.bucket(BUCKET_NAME)
    blob_name = os.path.basename(local_path)
    blob = bucket.blob(blob_name)

    print(f"Uploading {blob_name}")
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded gs://{BUCKET_NAME}/{blob_name}")

def main():
    preflight()

    client = storage.Client(project=GCP_PROJECT_ID)

    create_bucket_if_needed(client)

    for month in MONTHS:
        file_path = download_file(month)
        upload_file(client, file_path)

    print("\n🎉 DONE — 6 files uploaded to GCS")

if __name__ == "__main__":
    main()

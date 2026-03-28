import os
from pathlib import Path
import sys
from dotenv import load_dotenv
from google.cloud import storage
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)

GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
today = datetime.now().strftime("%Y-%m-%d")

if len(sys.argv) == 1:
    selected_date = today
elif len(sys.argv) == 2:
    try:
        arg_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        selected_date = sys.argv[1]
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)
else:
    print("Usage: python upload_raw_to_gcs.py [YYYY-MM-DD] or no arguments for today's date")
    sys.exit(1)

CATALOG = Path(f"data/raw/steam_catalog/steam_catalog_{selected_date}.jsonl")
APP_DETAILS = Path(f"data/raw/steam_appdetails/steam_appdetails_{selected_date}.jsonl")
REVIEWS = Path(f"data/raw/steam_reviews/steam_reviews_{selected_date}.jsonl")

def main():
    if not GCP_BUCKET_NAME:
        raise ValueError("Missing GCP_BUCKET_NAME in .env")

    if not CATALOG.exists():
        raise FileNotFoundError(f"File not found: {CATALOG}")
    if not APP_DETAILS.exists():
        raise FileNotFoundError(f"File not found: {APP_DETAILS}")
    upload_files(CATALOG)
    upload_files(APP_DETAILS)
    upload_files(REVIEWS)


def upload_files(file_path: Path):
    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET_NAME)
    blob_path = str(file_path)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(file_path))
    print(f"Uploaded to gs://{GCP_BUCKET_NAME}/{blob_path}")

print(str(CATALOG))
print(str(APP_DETAILS))
print(str(REVIEWS))

if __name__ == "__main__":
    main()
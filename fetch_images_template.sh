#!/bin/bash

# --- Exit if any command fails ---
set -e

# --- Check for CSV argument ---
if [ -z "$1" ]; then
  echo "Usage: $0 path/to/input.csv"
  exit 1
fi

CSV_PATH="$1"
OUTPUT_FOLDER="${2:-utm_trs_images}"

# --- Export S3 environment variables ---
export S3_ENDPOINT="https://gateway:port"
export S3_BUCKET="bucket_name"
export S3_ACCESS_KEY="key_name"
export S3_PREFIX="prefix/"
export S3_SECRET_KEY="secret key"
export S3_URL_EXPIRY="3600"
export S3_REGION="s3_region"

echo "Using CSV: $CSV_PATH"
echo "Output folder: $OUTPUT_FOLDER"
echo "S3 environment variables loaded."

# --- Run the Python script ---
python3 fetch_images.py --csv "$CSV_PATH" --output "$OUTPUT_FOLDER"

echo "Done."

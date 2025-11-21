#!/bin/bash

# --- Exit if any command fails ---
set -e

# --- Check for CSV argument ---
if [ -z "$1" ]; then
  echo "Usage: $0 path/to/input.csv collection_name [output_folder]"
  exit 1
fi

# --- Check for collection argument ---
if [ -z "$2" ]; then
  echo "Error: Missing collection argument."
  echo "Usage: $0 path/to/input.csv collection_name [output_folder]"
  exit 1
fi

CSV_PATH="$1"
COLLECTION="$2"
OUTPUT_FOLDER="${3:-utm_trs_images}"

# --- Export S3 environment variables ---
export S3_ENDPOINT="https://gateway:port"
export S3_BUCKET="bucket_name"
export S3_ACCESS_KEY="key_name"
export S3_PREFIX="prefix/"
export S3_SECRET_KEY="secret key"
export S3_URL_EXPIRY="3600"
export S3_REGION="s3_region"

echo "Using CSV: $CSV_PATH"
echo "Collection: $COLLECTION"
echo "Output folder: $OUTPUT_FOLDER"
echo "S3 environment variables loaded."

# --- Run the Python script ---
python3 fetch_images.py \
  --csv "$CSV_PATH" \
  --collection "$COLLECTION" \
  --output "$OUTPUT_FOLDER"

echo "Done."

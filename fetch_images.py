from s3_server_utils import S3Connection
import os
import argparse
import pandas as pd
import shutil

def download_image_list(rel_paths, output_folder="utm_trs_images", collection=None):
    """
    Downloads a list of S3 objects using S3Connection.storage_download()
    and saves them into a local output folder.

    rel_paths: list of relative S3 keys (e.g. ["ab/cd/abcdefghijk.jpg", "12/34/123456d6734.tif"])
    output_folder: where to save downloaded copies
    """
    s3 = S3Connection()

    os.makedirs(output_folder, exist_ok=True)

    downloaded_files = []

    for rel in rel_paths:
        rel_prefix = f"{collection}{os.sep}originals{os.sep}{rel[0:2]}{os.sep}{rel[2:4]}"
        rel = f"{rel_prefix}{os.sep}{rel}"
        print(f"Checking: {rel}")
        if not s3.storage_exists(rel):
            print(f"Not found on S3: {rel}")
            continue

        try:
            tmp_path = s3.storage_download(rel)   # this creates a temp file
            filename = os.path.basename(rel)
            local_path = os.path.join(output_folder, filename)

            shutil.copy(tmp_path, local_path)
            s3.remove_tempfile(tmp_path)

            print(f"Saved {rel} → {local_path}")
            downloaded_files.append(local_path)

        except Exception as e:
            print(f"Error downloading {rel}: {e}")

    return downloaded_files


def load_paths_from_csv(csv_path, column_name):
    """
    Safely reads a CSV and extracts the given column into a list.
    """
    try:
        df = pd.read_csv(csv_path)

        if column_name not in df.columns:
            print(f"Column '{column_name}' not found in CSV.")
            return []

        col = df[column_name].dropna().astype(str).tolist()
        print(f"Loaded {len(col)} paths from CSV.")

        return col

    except Exception as e:
        print(f"Error reading CSV file '{csv_path}': {e}")
        return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download images from S3 using a CSV list of relative paths.")

    parser.add_argument("--csv", required=True,
                        help="Path to the CSV file containing S3 relative paths (e.g. 'attachmentlocation').")

    parser.add_argument("--collection", required=True,
                        help="collection directory name")

    parser.add_argument("--column", default="attachmentlocation",
                        help="Column name in the CSV containing S3 relative paths.")

    parser.add_argument("--output", default="utm_trs_images",
                        help="Local folder to save downloaded images (default: utm_trs_images).")

    args = parser.parse_args()

    print(f"Reading CSV: {args.csv}")
    rel_paths = load_paths_from_csv(args.csv, args.column)

    if not rel_paths:
        print("No paths found — exiting.")
        exit(1)

    print(f"Downloading {len(rel_paths)} images…")
    download_image_list(rel_paths=rel_paths, output_folder=args.output, collection=args.collection)

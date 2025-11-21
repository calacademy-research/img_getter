from s3_server_utils import S3Connection
import os
import argparse
import pandas as pd
import shutil
import time  # NEW
from PIL import Image


def skip_existing_file(output_file_path, max_size_kb=None):
    """
    If output exists and (optionally) is already under the size threshold,
    skip reprocessing.
    """
    if os.path.exists(output_file_path):
        if max_size_kb is None:
            print(f"Skipping {os.path.basename(output_file_path)}, already exists.")
            return True

        size_kb = os.path.getsize(output_file_path) / 1024
        if size_kb <= max_size_kb:
            print(
                f"Skipping {os.path.basename(output_file_path)}, "
                f"already <= {max_size_kb} KB."
            )
            return True
    return False


# NEW: generic copy-with-retry helper
def copy_with_retry(src, dst, description="", use_copyfile=True,
                    delay=30, max_total_wait=300):
    """
    Copy a file from src to dst with retries.

    - delay: seconds between retries
    - max_total_wait: total time in seconds before giving up

    Returns True on success, False on permanent failure.
    """
    start = time.time()
    attempt = 1

    while True:
        try:
            if use_copyfile:
                shutil.copyfile(src, dst)
            else:
                shutil.copy(src, dst)
            return True
        except Exception as e:
            elapsed = time.time() - start
            if elapsed >= max_total_wait:
                print(
                    f"Giving up on {description} after {attempt} attempts "
                    f"and {int(elapsed)}s: {e}"
                )
                return False

            print(
                f"Copy failed for {description} (attempt {attempt}): {e}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)
            attempt += 1


def save_image_with_retry(image, output_file_path, file_name,
                          quality=80, delay=30, max_total_wait=300):
    """
    Save a PIL image to the destination path with retries.
    Used when we are not using a temp file + copy, but writing directly.
    retry for up to max total wait time for copying to destination folder.
    """
    start = time.time()
    attempt = 1

    while True:
        try:
            image.save(
                output_file_path,
                "JPEG",
                quality=quality,
                optimize=True,
                subsampling=0,
            )
            return True
        except Exception as e:
            elapsed = time.time() - start
            if elapsed >= max_total_wait:
                print(
                    f"Giving up on saving {file_name} after {attempt} attempts "
                    f"and {int(elapsed)}s: {e}"
                )
                return False

            print(
                f"Save failed for {file_name} (attempt {attempt}): {e}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)
            attempt += 1


def compress_image_quality(
    image,
    tmp_file_path,
    output_file_path,
    file_name,
    start_quality,
    max_size_kb,
):
    """
    Iteratively compress image to be <= max_size_kb, starting from start_quality.
    Will not go below quality 20.

    image: PIL Image instance
    tmp_file_path: where to save intermediate JPEG
    output_file_path: final destination
    """
    img_quality = start_quality

    while img_quality > 20:
        image.save(
            tmp_file_path,
            "JPEG",
            quality=img_quality,
            optimize=True,
            subsampling=0,
        )
        current_size_kb = os.path.getsize(tmp_file_path) / 1024

        if current_size_kb <= max_size_kb:
            # Use copy_with_retry instead of a single copyfile
            ok = copy_with_retry(
                tmp_file_path,
                output_file_path,
                description=f"{file_name} (resized)",
                use_copyfile=True,
            )
            # Clean up temp file regardless
            try:
                os.remove(tmp_file_path)
            except OSError:
                pass

            if ok:
                print(
                    f"Image {file_name} resized successfully "
                    f"({current_size_kb:.1f} KB @ quality {img_quality})"
                )
            else:
                print(
                    f"Warning: {file_name} reached size target but "
                    f"could not be written to destination."
                )
            return

        if current_size_kb > (max_size_kb + 300):
            img_quality -= 5  # Decrease quality by 5 if > 0.3 MB above limit
        else:
            img_quality -= 1  # Decrease quality by 1 when close to target

    print(
        f"Warning: Could not reduce {file_name} to under "
        f"{max_size_kb} KB without dropping below min quality."
    )
    # Keep the last attempt as "best effort"
    ok = copy_with_retry(
        tmp_file_path,
        output_file_path,
        description=f"{file_name} (best-effort resize)",
        use_copyfile=True,
    )
    try:
        os.remove(tmp_file_path)
    except OSError:
        pass

    if not ok:
        print(
            f"Warning: Best-effort copy for {file_name} also failed; "
            f"moving on to next image."
        )


def download_image_list(
    rel_paths,
    output_folder="utm_trs_images",
    collection=None,
    max_size_kb=None,
    quality=80,
    resize_to=None
):
    """
    Downloads a list of S3 objects using S3Connection.storage_download()
    and saves them into a local output folder.

    If max_size_kb and/or resize_to are provided, images are recompressed
    and/or resized using the ImageResizer logic.

    rel_paths: list of relative S3 keys (e.g. ["c1c7...jpg", "9a39...jpg"])
    output_folder: where to save final images
    collection: collection name (e.g. "botany")
    max_size_kb: target maximum file size in KB (optional)
    quality: starting JPEG quality for compression
    resize_to: tuple (width, height) to resize to (optional)
    """
    s3 = S3Connection()

    os.makedirs(output_folder, exist_ok=True)

    tmp_dir = "tmp_dir"
    os.makedirs(tmp_dir, exist_ok=True)

    downloaded_files = []

    for rel in rel_paths:
        rel_prefix = f"{collection}{os.sep}originals{os.sep}{rel[0:2]}{os.sep}{rel[2:4]}"
        rel_key = f"{rel_prefix}{os.sep}{rel}"

        print(f"Checking: {rel_key}")

        if not s3.storage_exists(rel_key):
            print(f"Not found on S3: {rel_key}")
            continue

        try:
            tmp_s3_path = s3.storage_download(rel_key)  # raw download
            filename = os.path.basename(rel)
            output_file_path = os.path.join(output_folder, filename)

            if skip_existing_file(output_file_path, max_size_kb=max_size_kb):
                s3.remove_tempfile(tmp_s3_path)
                continue

            # If no resize or compression requested, just copy:
            if max_size_kb is None and resize_to is None:
                ok = copy_with_retry(
                    tmp_s3_path,
                    output_file_path,
                    description=f"{filename} (download-only)",
                    use_copyfile=False,
                )
                s3.remove_tempfile(tmp_s3_path)

                if ok:
                    print(f"Saved (no resize) {rel_key} → {output_file_path}")
                    downloaded_files.append(output_file_path)
                else:
                    print(
                        f"Failed to save {filename} after retries; "
                        f"moving on to next image."
                    )
                continue

            # Load, optionally resize, then compress
            with Image.open(tmp_s3_path) as image:
                print(f"Processing (resize/compress) file {filename}")

                if resize_to is not None:
                    image = image.resize(resize_to)

                tmp_resize_path = os.path.join(tmp_dir, filename)

                if max_size_kb is not None:
                    # Resizing + size-constrained compression
                    compress_image_quality(
                        image=image,
                        tmp_file_path=tmp_resize_path,
                        output_file_path=output_file_path,
                        file_name=filename,
                        start_quality=quality,
                        max_size_kb=max_size_kb,
                    )
                else:
                    # Just resize, no size constraint — use save_image_with_retry
                    ok = save_image_with_retry(
                        image=image,
                        output_file_path=output_file_path,
                        file_name=filename,
                        quality=quality,
                    )
                    if ok:
                        print(
                            f"Image {filename} resized (dims only) "
                            f"→ {output_file_path}"
                        )
                    else:
                        print(
                            f"Failed to save resized {filename} after retries; "
                            f"moving on to next image."
                        )

            s3.remove_tempfile(tmp_s3_path)
            downloaded_files.append(output_file_path)

        except Exception as e:
            print(f"Error downloading/resizing {rel_key}: {e}")

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


def parse_resize_to(resize_str):
    """
    Parse a resize string like '2838x3745' into a (width, height) tuple.
    """
    if not resize_str:
        return None
    try:
        w_str, h_str = resize_str.lower().split("x")
        return int(w_str), int(h_str)
    except Exception:
        raise ValueError(
            f"Invalid --resize-to value '{resize_str}'. "
            f"Expected format WIDTHxHEIGHT, e.g. 2838x3745."
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Download images from S3 using a CSV list of relative paths, "
                    "optionally resizing/compressing them."
    )

    parser.add_argument(
        "--csv",
        required=True,
        help="Path to the CSV file containing S3 relative paths (e.g. 'attachmentlocation').",
    )

    parser.add_argument(
        "--collection",
        required=True,
        help="Collection directory name (e.g. 'botany').",
    )

    parser.add_argument(
        "--column",
        default="attachmentlocation",
        help="Column name in the CSV containing S3 relative paths.",
    )

    parser.add_argument(
        "--output",
        default="utm_trs_images",
        help="Local folder to save downloaded images (default: utm_trs_images).",
    )

    parser.add_argument(
        "--max-size-kb",
        type=int,
        default=None,
        help="Maximum image size in KB. If set, images are recompressed down to this size.",
    )

    parser.add_argument(
        "--quality",
        type=int,
        default=80,
        help="Starting JPEG quality for compression (default: 80).",
    )

    parser.add_argument(
        "--resize-to",
        type=str,
        default=None,
        help="Resize images to WIDTHxHEIGHT before compression, e.g. '2838x3745'.",
    )

    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print(f"Reading CSV: {args.csv}")
    rel_paths = load_paths_from_csv(args.csv, args.column)

    if not rel_paths:
        print("No paths found — exiting.")
        exit(1)

    resize_to_tuple = parse_resize_to(args.resize_to) if args.resize_to else None

    print(f"Downloading {len(rel_paths)} images…")
    download_image_list(
        rel_paths=rel_paths,
        output_folder=args.output,
        collection=args.collection,
        max_size_kb=args.max_size_kb,
        quality=args.quality,
        resize_to=resize_to_tuple
    )

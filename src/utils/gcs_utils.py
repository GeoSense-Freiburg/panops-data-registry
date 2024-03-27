"""Google Cloud Storage utility functions."""

import argparse
import os
from pathlib import Path

from google.cloud import storage
from google.cloud.exceptions import Forbidden, NotFound
from tqdm import tqdm

from src.utils.log_utils import setup_logger

log = setup_logger(__name__, "INFO")


def cli() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download all files from a Google Cloud Storage bucket."
    )
    parser.add_argument(
        "bucket_name", type=str, help="Name of the Google Cloud Storage bucket."
    )
    parser.add_argument(
        "local_path", type=str, help="Local directory to download files to."
    )
    return parser.parse_args()


def download_blob(blob: storage.Blob, out_dir: str | os.PathLike) -> None:
    """
    Download a blob from a storage bucket.

    Args:
        blob (storage.Blob): The blob to download.
        out_dir (str | os.PathLike): The output directory to save the downloaded file.

    Returns:
        None
    """
    blob_name = Path(blob.name).name  # pyright: ignore[reportArgumentType]
    log.info("Downloading %s to %s", blob_name, str(out_dir))
    out_file_path = Path(out_dir, blob_name)

    if out_file_path.exists():
        log.warning(
            "File %s already exists at %s. Overwriting...",
            blob_name,
            str(out_file_path),
        )
        out_file_path.unlink()
    blob.download_to_filename(out_file_path)


def download_blobs(
    blobs: list[storage.Blob],
    out_dir: str | os.PathLike,
) -> None:
    """
    Download multiple blobs from a storage bucket.

    Args:
        blobs (list[storage.Blob]): List of blobs to download.
        out_dir (str | os.PathLike): The output directory to save the downloaded files.

    Returns:
        None
    """
    for i, blob in enumerate(blobs):
        log.info(
            "Downloading %s... (%s/%s)",
            blob.name,
            i + 1,
            len(blobs),
        )
        download_blob(blob, out_dir)


def download_blob_if_exists(
    file_stem: str,
    bucket: storage.Bucket,
    out_dir: str | os.PathLike,
    overwrite: bool = True,
):
    """
    Downloads a blob from a storage bucket if it exists.

    Args:
        file_stem (str): The stem of the file name.
        bucket (storage.Bucket): The storage bucket object.
        out_dir (str | os.PathLike): The output directory to save the downloaded file.

    Returns:
        None
    """
    file_name = file_stem + ".tif"
    local_file_path = Path(out_dir) / file_name

    blob = bucket.blob(file_name)

    if blob.exists():
        if not local_file_path.exists() or overwrite:
            download_blob(blob, out_dir)
        else:
            log.info(
                "File %s already exists at %s. Skipping download...",
                file_name,
                str(out_dir),
            )
    else:
        log.warning(
            "File %s not found in bucket. Checking if split into parts...",
            file_name,
        )
        blobs = list(bucket.list_blobs(prefix=Path(file_name).stem))
        if blobs:
            download_blobs(blobs, out_dir)
        else:
            log.error("File %s not found in bucket.", file_name)


def download_bucket(bucket_name: str, local_path: str | os.PathLike) -> None:
    """Download all files from a Google Cloud Storage bucket to a local directory."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs())

    log.info("Downloading files from bucket %s to %s...", bucket_name, local_path)

    for blob in tqdm(blobs):
        download_blob(blob, local_path)


def validate_bucket_and_create_if_not_exists(bucket_id: str) -> storage.Bucket:
    """
    Validate a Google Cloud Storage bucket and create it if it does not exist.

    Args:
        bucket_id (str): The ID of the Google Cloud Storage bucket.

    Returns:
        storage.Bucket: The Google Cloud Storage bucket.
    """
    storage_client = storage.Client()

    log.info("Getting bucket %s...", bucket_id)
    try:
        bucket = storage_client.get_bucket(bucket_id)
    except NotFound:
        log.warning("Bucket %s not found. Creating...", bucket_id)
        bucket = storage_client.create_bucket(
            bucket_id,
            location="europe-west1",
            predefined_acl="private",
        )
    except Forbidden:
        log.error(
            "Access denied to bucket %s, or the name is already taken. Exiting...",
            bucket_id,
        )
        raise

    return bucket


def main() -> None:
    """
    Entry point of the program.

    This function parses command line arguments using the `cli` function and
    calls the `download_bucket` function to download files from a specified
    bucket to a local path.
    """
    args = cli()
    download_bucket(args.bucket_name, args.local_path)


if __name__ == "__main__":
    main()

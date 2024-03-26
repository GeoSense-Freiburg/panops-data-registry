"""Google Cloud Storage utility functions."""

import argparse
import os
from pathlib import Path

from google.cloud import storage
from tqdm import tqdm

from src.utils.log_utils import setup_logger

log = setup_logger(__name__, "INFO")


def download_bucket(bucket_name: str, local_path: str | os.PathLike) -> None:
    """Download all files from a Google Cloud Storage bucket to a local directory."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs())

    log.info("Downloading files from bucket %s to %s...", bucket_name, local_path)

    for blob in tqdm(blobs):
        blob.download_to_filename(Path(local_path) / blob.name)


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

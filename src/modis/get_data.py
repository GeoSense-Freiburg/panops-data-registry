import logging
import os
import time
from pathlib import Path

import ee
from dotenv import find_dotenv, load_dotenv
from google.cloud import storage

from src.conf.parse_params import config
from src.utils.log_utils import setup_file_logger, setup_logger

load_dotenv(find_dotenv())
project_root = os.environ["PROJECT_ROOT"]


def setup_logging(log_file: str | os.PathLike) -> logging.Logger:
    setup_logger()
    log = logging.getLogger(__name__)
    # setup_file_logger(log_file=Path(project_root, log_file))
    return log


def main(cfg: dict = config["modis"]):
    log = setup_logging(cfg["log_file"])

    # Initialize the Earth Engine client
    ee.Initialize()

    # Get a list of tasks
    tasks = ee.batch.Task.list()

    # Create a storage client
    storage_client = storage.Client()

    # Specify your bucket name
    bucket_name = cfg["bucket"]

    # Get the bucket
    bucket = storage_client.get_bucket(bucket_name)

    out_dir = Path(project_root, cfg["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    # While there are tasks
    while tasks:
        # For each task
        for task in tasks.copy():
            # Get the task status
            status = task.status()

            # If the task has completed
            if status["state"] == "COMPLETED":
                # Get the output file name
                file_name = status["description"] + ".tif"

                # Get the blob
                blob = bucket.blob(file_name)

                # If the blob exists
                if blob.exists():
                    # Download the file
                    log.info("Downloading %s...", file_name)
                    blob.download_to_filename(Path(out_dir, file_name))
                    log.info("Downloaded %s", file_name)
                else:
                    # Write a message to the log file
                    log.warning("File %s not found in bucket.", file_name)

                # Remove the task from the list
                tasks.remove(task)

            # If the task has failed
            elif status["state"] == "FAILED":
                # Write the failure to the log file
                log.error("Task %s failed: %s", task.id, status["error_message"])

                # Remove the task from the list
                tasks.remove(task)

        # Wait for a while before checking the tasks again
        time.sleep(60)


if __name__ == "__main__":
    main()

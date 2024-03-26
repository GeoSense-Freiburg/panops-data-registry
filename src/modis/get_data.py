"""
Check for completed MODIS tasks and download the data from Google Cloud Storage.

Citation:
Vermote, E., Wolfe, R. (2021). <i>MODIS/Terra Surface Reflectance Daily L2G Global 1km
and 500m SIN Grid V061</i> [Data set]. NASA EOSDIS Land Processes Distributed Active
Archive Center. Accessed 2024-03-25 from https://doi.org/10.5067/MODIS/MOD09GA.061
"""

import os
import time
from pathlib import Path

import ee
from dotenv import find_dotenv, load_dotenv
from google.cloud import storage

from src.conf.parse_params import config
from src.utils.log_utils import setup_logger

load_dotenv(find_dotenv(), verbose=True, override=True)
project_root = os.environ["PROJECT_ROOT"]


def main(cfg: dict = config["modis"]):
    """
    Main function to download MODIS data from Google Earth Engine and store it in Google
    Cloud Storage.

    Args:
        cfg (dict): Configuration dictionary containing the necessary parameters.

    Returns:
        None
    """
    log = setup_logger(__name__, "INFO")

    log.info("Initializing Earth Engine...")
    ee.Initialize()

    log.info("Getting task list...")
    tasks = ee.batch.Task.list()

    log.info("Connecting to Google Cloud Storage...")
    storage_client = storage.Client()

    log.info("Getting bucket %s...", cfg["bucket"])
    bucket = storage_client.get_bucket(cfg["bucket"])

    out_dir = Path(project_root, cfg["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info("Checking for completed tasks...")
    while tasks:
        for task in tasks.copy():
            status = task.status()

            if status["state"] == "COMPLETED":
                file_name = status["description"] + ".tif"
                local_file_path = Path(out_dir) / file_name

                blob = bucket.blob(file_name)

                if blob.exists():
                    if not local_file_path.exists():
                        log.info("Downloading %s... to %s", file_name, str(out_dir))
                        blob.download_to_filename(str(local_file_path))
                    else:
                        log.info(
                            "File %s already exists at %s", file_name, str(out_dir)
                        )
                else:
                    log.warning("File %s not found in bucket.", file_name)

                tasks.remove(task)

            elif status["state"] == "FAILED":
                log.error("Task %s failed: %s", task.id, status["error_message"])
                tasks.remove(task)

        time.sleep(60)


if __name__ == "__main__":
    main()

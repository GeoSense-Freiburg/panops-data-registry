"""
Preprocess ESA WorldCover data using Google Earth Engine.
This script:
    1. Retrieves the ESA WorldCover dataset from GEE.
    2. Exports the image at 1 km resolution to Google Cloud Storage.
    3. Downloads the exported data to the local filesystem (if desired).
"""

import argparse
import logging
import os
from pathlib import Path

import ee
from dotenv import find_dotenv, load_dotenv

from src.conf.parse_params import config
from src.utils.gee_utils import (
    ExportParams,
    _export_image,
    download_when_complete,
    get_ic,
)
from src.utils.log_utils import setup_logger

# Setup
load_dotenv(find_dotenv(), verbose=True, override=True)
project_root = os.environ["PROJECT_ROOT"]
log = setup_logger(__name__)


def cli():
    """Parse command-line arguments.

    This function parses the command-line arguments for the ESA WorldCover data
    preprocessing script. It creates an argument parser and adds the necessary arguments
    for downloading the data and setting the logging level. The function then returns the
    parsed arguments.

    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Preprocess ESA WorldCover data.")
    parser.add_argument(
        "--download",
        "-d",
        action="store_true",
        help="Download the data from Google Cloud Storage to the local filesystem.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Set the logging level to INFO.",
    )
    return parser.parse_args()


def main(cfg: dict = config["esa_worldcover"]) -> None:
    """
    Main function for preprocessing data.

    Args:
        cfg (dict): Configuration dictionary containing parameters for preprocessing.

    Returns:
        None
    """
    args = cli()
    if args.verbose:
        log.setLevel(logging.INFO)

    ee.Initialize()
    log.info("Earth Engine initialized.")

    wc = get_ic(cfg["collection_id"]).first()

    export_params = ExportParams(
        crs=cfg["crs"],
        scale=cfg["scale"],
        target=cfg["target"],
        folder=cfg["bucket"],
    )

    out_path = Path(project_root, cfg["out_path"])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    task = _export_image(wc, out_path.stem, export_params)

    if args.download:
        download_when_complete(cfg["bucket"], out_path.parent, [task], args.verbose)


if __name__ == "__main__":
    main()

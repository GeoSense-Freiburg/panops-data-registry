"""
Preprocess and download SoilGrids data.
This script:
    1. Retrieves the SoilGrids dataset from GEE.
    2. Exports images for each soil property at each depth profile at 1 km resolution to
       Google Cloud Storage.
"""

import argparse
import logging
import os
from pathlib import Path

import ee
from dotenv import find_dotenv, load_dotenv

from src.conf.parse_params import config
from src.utils.gee_utils import ExportParams, download_when_complete, export_image
from src.utils.log_utils import setup_logger

# Setup
load_dotenv(find_dotenv(), override=True, verbose=True)
project_root = os.environ["PROJECT_ROOT"]
log = setup_logger(__name__)


def cli() -> argparse.Namespace:
    """
    Parse command line arguments for preprocessing SoilGrids data.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(description="Preprocess SoilGrids data.")
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate export tasks, but do not run them.",
    )
    return parser.parse_args()


def export_soilgrids_images(
    soil_properties: list,
    soil_stat: str,
    collection_id: str,
    export_params: ExportParams,
    dry_run: bool = False,
) -> list[ee.batch.Task]:
    """
    Export soilgrids images for the given soil properties and statistics.

    Args:
        soil_properties (list): List of soil properties to export.
        soil_stat (str): Soil statistic to export.
        collection_id (str): ID of the soilgrids collection.
        export_params (ExportParams): Parameters for exporting the images.
        dry_run (bool, optional): Whether to perform a dry run. Defaults to False.

    Returns:
        list[ee.batch.Task]: List of export tasks.

    """
    tasks = []
    for prop in soil_properties:
        image = ee.Image(f"{collection_id}/{prop}_{soil_stat}")
        for band in image.bandNames().getInfo():
            task = export_image(
                image.select(band), band, export_params, dry_run=dry_run
            )
            tasks.append(task)

    return tasks


def download_soilgrids_images(
    tasks: list[ee.batch.Task],
    bucket: str,
    out_dir: str | os.PathLike,
    verbose: bool = False,
) -> None:
    """
    Downloads soilgrids images from Earth Engine tasks to a specified directory.

    Args:
        tasks (list[ee.batch.Task]): A list of Earth Engine tasks representing the images
            to download.
        bucket (str): The name of the bucket where the images will be stored.
        out_dir (str | os.PathLike): The output directory where the images will be saved.
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """
    out_dir = Path(project_root, out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    download_when_complete(bucket, out_dir, tasks, verbose)


def main(cfg: dict = config["soilgrids"]) -> None:
    """
    Main function to export SoilGrids images to Google Cloud Storage and optionally
    download them.

    Args:
        cfg (dict): Configuration dictionary containing parameters for exporting and
            downloading SoilGrids images.

    Returns:
        None
    """
    args = cli()
    if args.verbose:
        log.setLevel(logging.INFO)

    ee.Initialize()

    log.info("Exporting SoilGrids images to Google Cloud Storage...")
    export_params = ExportParams(
        crs=cfg["crs"],
        scale=cfg["scale"],
        target=cfg["target"],
        folder=cfg["bucket"],
    )
    tasks = export_soilgrids_images(
        soil_properties=cfg["soil_properties"],
        soil_stat=cfg["soil_stat"],
        collection_id=cfg["collection_id"],
        export_params=export_params,
        dry_run=args.dry_run,
    )

    if args.download and not args.dry_run:
        log.info("Downloading images from Google Cloud Storage...")
        download_soilgrids_images(
            tasks=tasks,
            bucket=cfg["bucket"],
            out_dir=cfg["out_dir"],
            verbose=args.verbose,
        )


if __name__ == "__main__":
    main()

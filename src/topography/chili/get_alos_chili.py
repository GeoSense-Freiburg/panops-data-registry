"""
Download ALOS CHILI (Compound Topographic Index) data from Google Earth Engine at 1 km resolution.

Citation:
Theobald, D. M., Harrison-Atlas, D., Monahan, W. B., & Albano, C. M. (2015).
Ecologically-relevant maps of landforms and physiographic diversity for climate adaptation planning.
PloS one, 10(12), e0143619.
"""

# pylint: disable=no-member

import argparse
import os

import ee
from dotenv import find_dotenv, load_dotenv

from src.conf.parse_params import config
from src.utils.gee_utils import (
    ExportParams,
    download_when_complete,
    ee_init,
    export_collection,
)
from src.utils.log_utils import setup_logger

load_dotenv(find_dotenv(), override=True, verbose=True)
project_root = os.environ["PROJECT_ROOT"]
log = setup_logger(__name__, "INFO")


def get_alos_chili_image(cfg: dict) -> ee.Image:
    """
    Get ALOS CHILI image from Google Earth Engine.

    Args:
        cfg (dict): Configuration parameters for ALOS CHILI data.

    Returns:
        ee.Image: ALOS CHILI image.
    """
    chili_image = ee.Image(cfg["collection_id"]).select(cfg["band"])
    return chili_image


def process_chili_image_ee(chili_image: ee.Image, cfg: dict) -> ee.Image:
    """
    Process ALOS CHILI image for export.

    Args:
        chili_image (ee.Image): Raw ALOS CHILI image.
        cfg (dict): Configuration parameters for processing.

    Returns:
        ee.Image: Processed ALOS CHILI image.
    """
    # Rename the band to a descriptive name
    processed_image = chili_image.rename("ALOS_CHILI_constant")
    return processed_image


def export_chili_image(cfg: dict) -> list[ee.batch.Task]:
    """
    Export ALOS CHILI data.

    Args:
        cfg (dict): Configuration parameters for exporting ALOS CHILI data.

    Returns:
        list[ee.batch.Task]: List of export tasks for ALOS CHILI data.
    """
    chili_image = get_alos_chili_image(cfg)
    processed_image = process_chili_image_ee(chili_image, cfg)

    export_params = ExportParams(
        crs=cfg["crs"],
        scale=cfg["scale"],
        target=cfg["target"],
        folder=cfg["bucket"],
        nodata=cfg["nodata"],
    )

    tasks = export_collection(
        ee.ImageCollection([processed_image]), export_params, flatten=True
    )
    return tasks


def cli() -> argparse.Namespace:
    """
    Command line interface for getting ALOS CHILI data.
    """
    parser = argparse.ArgumentParser(description="Get ALOS CHILI data.")
    parser.add_argument("--debug", "-d", action="store_true", help="Debug mode.")
    return parser.parse_args()


def main(cfg: dict = config, args: argparse.Namespace | None = None) -> None:
    """
    Main function for getting ALOS CHILI data.

    Args:
        cfg (dict): Configuration dictionary for ALOS CHILI data.
    """

    if args is None:
        args = cli()

    ee_init(cfg["ee"]["project_id"], cfg["ee"]["high_volume"])

    chili_cfg = cfg["alos_chili"]

    if args.debug:
        chili_cfg["scale"] = 222000
        chili_cfg["out_dir"] = "debug/alos_chili"

    tasks = export_chili_image(chili_cfg)
    download_when_complete(chili_cfg["bucket"], chili_cfg["out_dir"], tasks, True)


if __name__ == "__main__":
    main()

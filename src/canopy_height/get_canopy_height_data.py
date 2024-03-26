"""
Download canopy height data from Google Earth Engine at 1 km resolution.

Citation:
Lang, Nico, Walter Jetz, Konrad Schindler, and Jan Dirk Wegner. "A high-resolution
canopy height model of the Earth." arXiv preprint arXiv:2204.08322 (2022).
"""

# pylint: disable=no-member

import os

import ee
from dotenv import find_dotenv, load_dotenv

from src.conf.parse_params import config
from src.utils.gee_utils import ExportParams, download_when_complete, export_collection
from src.utils.log_utils import setup_logger

load_dotenv(find_dotenv(), override=True, verbose=True)
project_root = os.environ["PROJECT_ROOT"]
log = setup_logger(__name__, "INFO")


def export_canopy_height(cfg: dict) -> list[ee.batch.Task]:
    """
    Export canopy height data.

    Args:
        cfg (dict): Configuration parameters for exporting canopy height data.

    Returns:
        list[ee.batch.Task]: List of export tasks for canopy height data.
    """
    height = (
        ee.Image(cfg["height_collection"])
        .select("b1")
        .rename("ETH_GlobalCanopyHeight_2020_v1")
    )
    sd = (
        ee.Image(cfg["sd_collection"])
        .select("b1")
        .rename("ETH_GlobalCanopyHeightSD_2020_v1")
    )
    export_params = ExportParams(
        crs=cfg["crs"],
        scale=cfg["scale"],
        target=cfg["target"],
        folder=cfg["bucket"],
    )

    tasks = export_collection(
        ee.ImageCollection([height, sd]), export_params, flatten=True
    )
    return tasks


def main(cfg: dict = config["canopy_height"]) -> None:
    """
    Main function for getting canopy height data.

    Args:
        cfg (dict): Configuration dictionary for canopy height data.
    """
    ee.Initialize()
    tasks = export_canopy_height(cfg)
    download_when_complete(cfg["bucket"], cfg["out_dir"], tasks, True)


if __name__ == "__main__":
    main()

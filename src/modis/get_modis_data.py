"""Generate global multiyear monthly averages of MODIS Terra surface reflectance data from
    2000-2023 at 1 km resolution.
"""

# pylint: disable=no-member

import argparse
import logging
import os
from pathlib import Path

import ee
from dotenv import find_dotenv, load_dotenv

from src.conf.parse_params import config
from src.utils.gee_utils import (
    ExportParams,
    calculate_monthly_averages,
    download_when_complete,
    export_collection,
    get_ic,
    mask_clouds,
)
from src.utils.log_utils import setup_logger

log = setup_logger(__name__)


def add_ndvi(ic: ee.ImageCollection) -> ee.ImageCollection:
    """Calculate NDVI from an ImageCollection and add it as a band."""

    def _calculate_ndvi(image):
        ndvi = image.normalizedDifference(["sur_refl_b02", "sur_refl_b01"]).rename(
            "ndvi"
        )
        return image.addBands(ndvi)

    ic_ndvi = ic.map(_calculate_ndvi)
    return ic_ndvi


def get_modis_ic(
    cfg: dict,
) -> ee.ImageCollection:
    """Get MODIS Terra surface reflectance data from 2000-2023."""
    modis = get_ic(
        cfg["product"],
        cfg["date_start"],
        cfg["date_end"],
    )

    modis = mask_clouds(modis, cfg["qa_band"]).select(cfg["bands"])

    return modis


def mask_and_cast_int16(ic: ee.ImageCollection) -> ee.ImageCollection:
    """Mask clouds and cast the ImageCollection to int16."""
    return ic.map(lambda image: image.unmask(-32768)).map(lambda image: image.toInt16())


def process_modis_ic_ee(ic: ee.ImageCollection, cfg: dict) -> ee.ImageCollection:
    """Process MODIS Terra surface reflectance ImageCollection by computing monthly averages"""
    year_start = cfg["date_start"].split("-")[0]
    year_end = cfg["date_end"].split("-")[0]
    ic = calculate_monthly_averages(ic, year_start, year_end)
    return mask_and_cast_int16(ic)


def export_modis_ic(ic: ee.ImageCollection, cfg: dict) -> list[ee.batch.Task]:
    """Export the ImageCollection."""
    export_params = ExportParams(
        crs=cfg["crs"],
        scale=cfg["scale"],
        target=cfg["target"],
        folder=cfg["bucket"],
        nodata=-cfg["nodata"],
    )
    tasks = export_collection(ic, export_params, flatten=True, dry_run=cfg["test"])
    return tasks


def cli() -> argparse.Namespace:
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Generate global multiyear monthly averages of MODIS Terra surface"
        "reflectance data from 2000-2023 at 1 km resolution."
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose mode")
    parser.add_argument("--test", "-t", action="store_true", help="Test mode")

    return parser.parse_args()


def main(cfg: dict = config["modis"]) -> None:
    """Generate global multiyear monthly averages of MODIS Terra surface reflectance data from
    2000-2023 at 1 km resolution."""
    load_dotenv(find_dotenv())
    out_dir = Path(os.environ["PROJECT_ROOT"]) / cfg["out_dir"]

    args = cli()

    if args.verbose:
        log.setLevel(logging.INFO)

    cfg["test"] = args.test

    if args.test:
        log.info("Running in test mode")
        cfg["date_start"] = "2021-01-01"
        cfg["date_end"] = "2022-01-31"
        cfg["scale"] = 112000

    log.info("Initializing the Earth Engine API")
    ee.Initialize()

    log.info("Getting MODIS Terra surface reflectance data")
    modis_ic = get_modis_ic(cfg)

    log.info("Processing into monthly avgs and exporting to Google Cloud Storage...")
    modis_ic = process_modis_ic_ee(modis_ic, cfg)
    tasks = export_modis_ic(modis_ic, cfg)

    log.info("Downloading from Google Cloud Storage...")
    download_when_complete(cfg["bucket"], out_dir, tasks)

    log.info("Done.")


if __name__ == "__main__":
    main()

"""Generate global multiyear monthly averages of MODIS Terra surface reflectance data from
    2000-2023 at 1 km resolution.
"""

# pylint: disable=no-member

import argparse
import logging
import os
from pathlib import Path

import dask.distributed
import ee
import xarray as xr
import zarr
from dotenv import find_dotenv, load_dotenv

from src.conf.parse_params import config
from src.utils.gee_utils import (
    ExportParams,
    calculate_monthly_averages,
    export_collection,
    get_ic,
    mask_clouds,
)
from src.utils.log_utils import setup_logger

setup_logger()
log = logging.getLogger(__name__)


def setup(use_dask: bool = False):
    """Set up the Earth Engine API and Dask client."""
    ee.Initialize()
    if not use_dask:
        return None
    log.info("Setting up Dask client")
    client = dask.distributed.Client()
    return client


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


def process_modis_ic_xee(ic: ee.ImageCollection, cfg: dict) -> xr.Dataset:
    """Process MODIS Terra surface reflectance ImageCollection by computing monthly averages
    using xarray."""

    ds = xr.open_dataset(
        ic,  # pyright: ignore[reportArgumentType]
        engine="ee",
        crs=cfg["crs"],
        scale=cfg["scale"],
    )
    ds = ds.groupby("time.month").mean()
    return ds


def mask_and_cast_int16(ic: ee.ImageCollection) -> ee.ImageCollection:
    """Mask clouds and cast the ImageCollection to int16."""
    return ic.map(lambda image: image.unmask(-32768)).map(lambda image: image.toInt16())


def process_modis_ic_ee(ic: ee.ImageCollection, cfg: dict) -> ee.ImageCollection:
    """Process MODIS Terra surface reflectance ImageCollection by computing monthly averages"""
    year_start = cfg["date_start"].split("-")[0]
    year_end = cfg["date_end"].split("-")[0]
    ic = calculate_monthly_averages(ic, year_start, year_end)
    return mask_and_cast_int16(ic)


def export_modis_ic(ic: ee.ImageCollection, cfg: dict):
    """Export the ImageCollection."""
    export_params = ExportParams(
        crs=cfg["crs"],
        scale=cfg["scale"],
        target=cfg["target"],
        folder=cfg["bucket"],
    )
    export_collection(ic, export_params, test=cfg["test"])


def save_to_zarr(ds: xr.Dataset, output_path: str | os.PathLike):
    """Save the xarray Dataset to Zarr format."""
    compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=zarr.Blosc.AUTOSHUFFLE)
    ds.to_zarr(
        str(output_path),
        encoding={var: {"compressor": compressor} for var in ds.data_vars},
    )


def cli() -> argparse.Namespace:
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Generate global multiyear monthly averages of MODIS Terra surface"
        "reflectance data from 2000-2023 at 1 km resolution."
    )
    parser.add_argument("--dask", "-d", action="store_true", help="Use Dask")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose mode")
    parser.add_argument(
        "--method",
        "-m",
        default="ee",
        choices=["ee", "xee"],
        help="Processing method. Note that xee method is experimental and only works for"
        "datasets < 32 MB.",
    )
    parser.add_argument("--test", "-t", action="store_true", help="Test mode")

    return parser.parse_args()


def main(cfg: dict = config["modis"]) -> None:
    """Generate global multiyear monthly averages of MODIS Terra surface reflectance data from
    2000-2023 at 1 km resolution."""
    load_dotenv(find_dotenv())
    project_dir = Path(os.environ["PROJECT_ROOT"])

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
    setup(use_dask=args.dask)

    log.info("Getting MODIS Terra surface reflectance data")
    modis_ic = get_modis_ic(cfg)

    if args.method == "xee":
        log.info(
            "Processing into monthly avgs and saving the xarray Dataset to Zarr format"
        )
        ds = process_modis_ic_xee(modis_ic, cfg)
        save_to_zarr(ds, project_dir / cfg["output_path"])

    if args.method == "ee":
        log.info(
            "Processing into monthly avgs and exporting to Google Cloud Storage..."
        )
        modis_ic = process_modis_ic_ee(modis_ic, cfg)
        export_modis_ic(modis_ic, cfg)


if __name__ == "__main__":
    main(config["modis"])

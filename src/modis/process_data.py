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
from src.utils.gee import aggregate_ic_monthly, get_ic, mask_clouds
from src.utils.log_utils import setup_logger

setup_logger()
log = logging.getLogger(__name__)


def setup(no_dask: bool = False):
    """Set up the Earth Engine API and Dask client."""
    ee.Initialize()
    if no_dask:
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
    modis_cfg: dict,
) -> ee.ImageCollection:
    """Get MODIS Terra surface reflectance data from 2000-2023."""
    modis = get_ic(
        modis_cfg["product"],
        modis_cfg["date_start"],
        modis_cfg["date_end"],
    )

    modis = mask_clouds(modis, modis_cfg["qa_band"]).select(modis_cfg["bands"])
    modis = add_ndvi(modis)
    return modis


def process_modis_ic_xee(ic: ee.ImageCollection, modis_cfg: dict) -> xr.Dataset:
    """Process MODIS Terra surface reflectance ImageCollection by computing monthly averages
    using xarray."""

    ds = xr.open_dataset(
        ic,  # pyright: ignore[reportArgumentType]
        engine="ee",
        crs=modis_cfg["crs"],
        scale=modis_cfg["scale"],
    )
    ds = ds.groupby("time.month").mean()
    return ds


def process_modis_ic_ee(ic: ee.ImageCollection, modis_cfg: dict) -> None:
    """Process MODIS Terra surface reflectance ImageCollection by computing monthly averages"""

    ic = aggregate_ic_monthly(ic)

    months = range(1, 13)

    # # Get the first image in the collection to retrieve band names
    first_image = ee.Image(ic.first())
    # bands = first_image.bandNames().getInfo()

    # for month in months:
    #     for band in bands:
    #         mean_image = (
    #             ic.filter(ee.Filter.calendarRange(month, month, "month"))
    #             .select(band)
    #             .mean()
    #         )

    #         year_start = modis_cfg["date_start"].split("-")[0]
    #         year_end = modis_cfg["date_end"].split("-")[0]
    #         res_in_km = modis_cfg["scale"] / 1000

    #         export_params = {
    #             "image": mean_image,
    #             "description": f"modis_{band}_mean_{month}_{year_start}-{year_end}_"
    #             f"{res_in_km:.2g}km",
    #             "bucket": modis_cfg["bucket"],
    #             "fileNamePrefix": f"modis_{band}_mean_{month}_{year_start}-{year_end}_"
    #             f"{res_in_km:.2g}km",
    #             "scale": modis_cfg["scale"],
    #             "fileFormat": "GeoTIFF",
    #             "formatOptions": {"cloudOptimized": True},
    #             "maxPixels": 1e13,
    #         }

    #         # Export the Image to Google Cloud Storage
    #         task = ee.batch.Export.image.toCloudStorage(**export_params)
    #         task.start()


def save_to_zarr(ds: xr.Dataset, output_path: str | os.PathLike):
    """Save the xarray Dataset to Zarr format."""
    compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=zarr.Blosc.AUTOSHUFFLE)
    ds.to_zarr(
        str(output_path),
        encoding={var: {"compressor": compressor} for var in ds.data_vars},
    )


def main(cfg: dict = config):
    """Generate global multiyear monthly averages of MODIS Terra surface reflectance data from
    2000-2023 at 1 km resolution."""
    parser = argparse.ArgumentParser(
        description="Generate global multiyear monthly averages of MODIS Terra surface"
        "reflectance data from 2000-2023 at 1 km resolution."
    )
    parser.add_argument("--no-dask", "-d", action="store_true", help="Use Dask")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose mode")
    parser.add_argument(
        "--method",
        "-m",
        default="ee",
        choices=["ee", "xee"],
        help="Processing method. Note that xee method is experimental and only works for"
        "datasets < 32 MB.",
    )

    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.INFO)

    load_dotenv(find_dotenv())
    project_dir = Path(os.environ["PROJECT_ROOT"])
    modis_cfg = cfg["modis"]

    log.info("Initializing the Earth Engine API")
    setup(no_dask=args.no_dask)

    log.info("Getting MODIS Terra surface reflectance data")
    modis_ic = get_modis_ic(modis_cfg)

    if args.method == "xee":
        log.info(
            "Processing into monthly avgs and saving the xarray Dataset to Zarr format"
        )
        ds = process_modis_ic_xee(modis_ic, modis_cfg)
        save_to_zarr(ds, project_dir / modis_cfg["output_path"])

    log.info("Processing into monthly avgs and exporting to Google Cloud Storage...")
    process_modis_ic_ee(modis_ic, modis_cfg)


if __name__ == "__main__":
    main()

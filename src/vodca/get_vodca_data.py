"""
Preprocesses the VODCA dataset into mean, and lower and upper quantiles for each band
and exports the images at 1 km resolution to Google Cloud Storage.

Dataset citation:
Moesinger, Leander, Dorigo, Wouter, De Jeu, Richard, Van der Schalie, Robin, Scanlon,
Tracy, Teubner, Irene, & Forkel, Matthias. (2019).
The Global Long-term Microwave Vegetation Optical Depth Climate Archive VODCA (1.0)
[Data set].
Zenodo. https://doi.org/10.5281/zenodo.2575599

Paper citation:
Moesinger, Leander, Wouter Dorigo, Richard de Jeu, Robin van der Schalie, Tracy Scanlon,
Irene Teubner, and Matthias Forkel.
"The global long-term microwave vegetation optical depth climate archive (VODCA)." Earth
System Science Data 12, no. 1 (2020): 177-196.
"""

# pylint: disable=no-member

import argparse
import os
from pathlib import Path

import ee
from dotenv import find_dotenv, load_dotenv

from src.conf.parse_params import config
from src.utils.gee_utils import ExportParams, download_when_complete, export_collection
from src.utils.log_utils import setup_logger
from src.utils.raster_utils import merge_rasters

load_dotenv(find_dotenv(), override=True, verbose=True)
project_root = os.environ["PROJECT_ROOT"]
log = setup_logger(__name__, "INFO")


def cli() -> argparse.Namespace:
    """
    Parse command line arguments for preprocessing VODCA data.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(description="Preprocess VODCA data.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate export tasks, but do not run them.",
    )
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Merge multipart files only.",
    )
    return parser.parse_args()


def preprocess_vodca(cfg: dict) -> ee.ImageCollection:
    """
    Preprocesses VODCA data based on the provided configuration.

    Args:
        cfg (dict): A dictionary containing the configuration parameters for preprocessing.

    Returns:
        ee.ImageCollection: The preprocessed VODCA data as an Earth Engine Image Collection.
    """
    images = []
    for band in cfg["bands"]:
        ic = (
            ee.ImageCollection(f"{cfg['collection_base']}/{band}")
            .select("b1")
            .map(
                lambda image: image.select("b1").rename(
                    f"vodca_{band.lower()}"  # pylint: disable=cell-var-from-loop
                )
            )
            .map(lambda image: image.resample("bilinear"))
        )
        image = ic.reduce(ee.Reducer.mean())
        percentiles = ic.reduce(
            ee.Reducer.percentile(
                cfg["percentiles"],
            )
        )
        image = image.addBands(percentiles).clamp(0, 1)
        images.append(image)

    ic = ee.ImageCollection(images)
    return ic


def export_vodca(
    ic: ee.ImageCollection, cfg: dict, dry_run: bool = False
) -> list[ee.batch.Task]:
    """
    Export VODCA data.

    Args:
        ic (ee.ImageCollection): The Earth Engine Image Collection to export.
        cfg (dict): A dictionary containing export configuration parameters.
        dry_run (bool, optional): If True, performs a dry run without actually exporting the data. Defaults to False.

    Returns:
        list[ee.batch.Task]: A list of Earth Engine Tasks representing the export operations.
    """
    export_params = ExportParams(
        crs=cfg["crs"],
        scale=cfg["scale"],
        target=cfg["target"],
        folder=cfg["bucket"],
    )

    tasks = export_collection(ic, export_params, flatten=True, dry_run=dry_run)

    return tasks


def check_multipart_files(out_dir: Path) -> list[str] | None:
    """
    Check for multipart files in the specified directory.

    Args:
        out_dir (Path): The directory to search for multipart files.

    Returns:
        list[str] | None: A list of unique prefixes of multipart files found in the directory,
                          or None if no multipart files are found.
    """
    files = list(out_dir.glob("*00000*"))
    if files:
        prefixes = []
        for file in files:
            prefixes.append(file.stem.split("00000")[0])
        return list(set(prefixes))
    return None


def merge_multipart_files(out_dir: Path, prefixes: list[str]) -> None:
    """
    Merge multiple raster files with the given prefixes into a single file.

    Args:
        out_dir (Path): The output directory where the merged file will be saved.
        prefixes (list[str]): A list of prefixes used to identify the files to be merged.

    Returns:
        None
    """
    for prefix in prefixes:
        log.info("Merging files with prefix %s...", prefix)
        files = sorted(list(out_dir.glob(f"{prefix}*")))
        out_file = out_dir / f"{prefix}.tif"
        merge_rasters([str(file) for file in files], str(out_file))
        for file in files:
            file.unlink()


def main(cfg: dict = config["vodca"]) -> None:
    """
    Main function to preprocess and export VODCA dataset.

    Args:
        cfg (dict): Configuration dictionary for VODCA dataset. Defaults to `config["vodca"]`.

    Returns:
        None
    """
    args = cli()

    ee.Initialize()
    out_dir = Path(project_root, cfg["out_dir"])

    if not args.merge_only:
        log.info("Preprocessing VODCA dataset...")
        ic = preprocess_vodca(cfg)

        log.info("Exporting VODCA dataset to Google Cloud Storage...")
        tasks = export_vodca(ic, cfg, args.dry_run)

        if not args.dry_run:
            log.info("Downloading data from Google Cloud Storage...")
            out_dir.mkdir(parents=True, exist_ok=True)
            download_when_complete(cfg["bucket"], out_dir, tasks, True)

    if not args.dry_run:
        prefixes = check_multipart_files(out_dir)
        if prefixes is not None:
            log.info("Multipart files detected. Merging...")
            merge_multipart_files(out_dir, prefixes)

    log.info("Done.")


if __name__ == "__main__":
    main()

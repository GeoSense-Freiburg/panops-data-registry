"""Initiate a GBIF download and save it to disk once it is ready."""

import json
import time
import zipfile
from pathlib import Path
from typing import Optional

import click
from dotenv import find_dotenv, load_dotenv
from pygbif import occurrences as occ  # pylint: disable=import-error

from src.utils.log_utils import setup_logger

log = setup_logger(__name__, "INFO")


class GbifDownloadFailure(Exception):
    """Exception raised when a GBIF download job fails."""

    def __init__(self, message="GBIF download job failed."):
        self.message = message
        super().__init__(self.message)


def init_gbif_download(query: dict) -> str:
    """
    Initiate a GBIF download job in the Apache Parquet format and return the job key.
    """
    download_key = occ.download(query, format="SIMPLE_PARQUET")  # type: ignore
    return download_key[0]


def check_download_status(key: str) -> str:
    """Check the status of a GBIF download job."""
    status = occ.download_meta(key)
    return status["status"]


def download_request_to_disk(
    key: str, output_path: Path, name: Optional[str] = None
) -> Path:
    """
    Download a completed GBIF download job's zipfile and metadata.

    Args:
        key (str): The key of the completed GBIF download job.
        output_path (Path): The path where the downloaded files will be saved.
        name (Optional[str], optional): The name to be used for the downloaded files. If
            None, then the default name will be used (the GBIF download key).

    Returns:
        Path: The full path of the downloaded zipfile.
    """
    occ.download_get(key, str(output_path))

    # This is how it is downloaded by default
    output_full_path = output_path / f"{key}.zip"

    if name is not None:
        # Rename output file with name
        named_file = output_path / f"{name}.zip"
        output_full_path = output_full_path.rename(named_file)

    # Also save the metadata alongside the data
    with open(
        output_path / f"{key if name is None else name}.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(occ.download_meta(key), f)

    return output_full_path


def check_download_job_and_download_file(
    key: str, output_path: Path, name: Optional[str] = None, max_hours: int | float = 6
) -> Path:
    """
    Checks a pending GBIF download for a given amount of time, downloads the file once
    it is ready.

    Args:
        key (str): The key of the GBIF download job to check.
        output_path (Path): The path where the downloaded file will be saved.
        name (Optional[str], optional): The name of the downloaded file. Defaults to None.
        max_hours (int | float, optional): The maximum number of hours to wait for the download
            to complete. Defaults to 6.

    Raises:
        GbifDownloadFailure: If the download job fails or exceeds the maximum waiting time.

    """
    start_time = time.time()
    while True:
        status = check_download_status(key)
        if status == "SUCCEEDED":
            output_file = download_request_to_disk(
                key=key, output_path=output_path, name=name
            )
            break

        if status == "FAILED":
            raise GbifDownloadFailure(f"Download job {key} failed.")

        if time.time() - start_time > max_hours * 60 * 60:
            output_path.unlink()
            raise GbifDownloadFailure(
                f"Download job {key} did not complete within" f"{max_hours}. Aborting"
            )
        time.sleep(60)

    return output_file


def unzip_and_rename(file_path: Path) -> None:
    """
    Unzip a file and rename the unzipped directory to the original name.

    Args:
        file_path (Path): The path to the zip file.
    """
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        # Extract all the contents of zip file in current directory
        zip_ref.extractall(path=file_path.parent)

        # Get the name of the directory that was extracted (most likely
        # "occurrence.parquet", but that may change in the future)
        extracted_dir = Path(zip_ref.namelist()[0]).parent
        extracted_dir = file_path.parent / extracted_dir
        extracted_dir.rename(file_path.with_suffix(".parquet"))

    # Remove the original zip file
    file_path.unlink()


@click.command()
@click.option(
    "-q",
    "--query",
    "query_file",
    type=click.Path(path_type=Path),
    default=Path("references/gbif/query_all_tracheophyta.json"),
    show_default=True,
)
@click.option(
    "-n",
    "--name",
    "name",
    type=click.STRING,
    help="Human-readable name of the query. Will be used as the download name.",
)
@click.option(
    "-k",
    "--key",
    "key",
    type=click.STRING,
    help="Key of download already created. Will simply re-download.",
)
@click.option(
    "-o",
    "--output",
    "output_path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path().cwd(),
    show_default=True,
)
def main(query_file: Path, name: str, key: str, output_path: Path):
    """Check the status of a GBIF download job and download the CSV file once it's ready."""
    load_dotenv(find_dotenv())  # Find local .env to expose GBIF credentials

    if query_file.suffix != ".json":
        raise ValueError("Query file must be a .json file.")

    if not output_path.is_dir():
        raise ValueError("Output path must be a directory.")

    with open(query_file, "r", encoding="utf-8") as f:
        gbif_query = json.load(f)

    if key is None:
        log.info("No key provided. Initializing new query.")
        download_key = init_gbif_download(gbif_query)
    else:
        log.info("Re-checking download using key: %s", key)
        download_key = key

    log.info("Checking if download job is ready...")
    output_file = check_download_job_and_download_file(
        key=download_key, output_path=output_path, name=name
    )

    log.info("Unzipping and renaming downloaded occurrences...")
    unzip_and_rename(output_file)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

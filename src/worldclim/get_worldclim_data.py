"""
Downloads WorldClim BIO variables (v2.1).
"""

import os
import zipfile
from pathlib import Path

import requests
from dotenv import find_dotenv, load_dotenv

from src.conf.parse_params import config
from src.utils.log_utils import setup_logger

load_dotenv(find_dotenv(), override=True, verbose=True)
project_root = os.environ["PROJECT_ROOT"]
log = setup_logger(__name__, "INFO")


def main(cfg: dict = config["worldclim"]) -> None:
    """
    Downloads and extracts WorldClim BIO variables.

    Args:
        cfg (dict): Configuration dictionary containing the necessary parameters.

    Returns:
        None
    """
    out_dir = Path(project_root, cfg["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info("Downloading WorldClim BIO variables")
    response = requests.get(cfg["url"], timeout=30)
    response.raise_for_status()

    zip_file_name = response.url.split("/")[-1]
    zip_out = out_dir / zip_file_name

    with open(zip_out, "wb") as f:
        f.write(response.content)

    log.info("Extracting WorldClim BIO variables")
    extract_dir = Path(cfg["out_dir"], zip_file_name.stem.replace(".", "-", 1))
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(Path(cfg["out_dir"], zip_file_name), "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    # Clean up by removing zip file
    zip_out.unlink()

    log.info("Done.")


if __name__ == "__main__":
    main()

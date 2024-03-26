"""
Downloads WorldClim BIO variables (v2.1) at 30s resolution.

Citation:
Fick, S.E. and R.J. Hijmans, 2017. WorldClim 2: new 1km spatial resolution climate
surfaces for global land areas. International Journal of Climatology 37 (12): 4302-4315. 
"""

import os
import zipfile
from pathlib import Path

import requests
from dotenv import find_dotenv, load_dotenv
from tqdm import tqdm

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

    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024

    zip_file_name = response.url.split("/")[-1].replace(".", "-", 1)
    zip_out = out_dir / zip_file_name

    with tqdm(total=total_size, unit="B", unit_scale=True) as progress_bar:
        with open(zip_out, "wb") as f:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                f.write(data)

    log.info("Extracting WorldClim BIO variables")
    extract_dir = Path(out_dir, zip_out.stem)
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(Path(out_dir, zip_file_name), "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    # Clean up by removing zip file
    zip_out.unlink()

    log.info("Done.")


if __name__ == "__main__":
    main()

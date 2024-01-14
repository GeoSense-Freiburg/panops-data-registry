"""Download raw data"""
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import hydra
import requests
from omegaconf import DictConfig

log = logging.getLogger(__name__)


def make_dir_if_new(new_dir: Path):
    """Check if directory exists and create it if it doesn't."""
    if not new_dir.exists():
        new_dir.mkdir(parents=True, exist_ok=True)
        log.info("Created directory %s", new_dir)


def download_file(url: str, odir: Path) -> Path:
    """Download file from url."""
    make_dir_if_new(odir)
    filename = url.split("/")[-1]

    log.info("Starting download of %s...", filename)

    with open(Path(odir, filename), "wb") as file:
        response = requests.get(url, timeout=10)
        file.write(response.content)

    log.info("Downloaded %s.", filename)

    return odir / filename


def download_files(urls: list[str], odirs: list[Path]) -> None:
    """Download files from urls concurrently."""
    try:
        with ThreadPoolExecutor() as executor:
            results = executor.map(download_file, urls, odirs)
            try:
                for _ in results:
                    continue
            except RuntimeError as e:
                log.error(e)
    except TimeoutError:
        log.warning("Failed to download")


def get_datasets_download_info(datasets: list[dict]) -> tuple[list[str], list[Path]]:
    """Get urls and output directories for datasets to download."""
    urls = [ds["url"] for ds in datasets]
    odirs = [Path(ds["odir"]) for ds in datasets]
    return urls, odirs


@hydra.main(version_base=None, config_path="../conf", config_name="config")
def main(cfg: DictConfig) -> None:
    """Download raw data."""
    datasets_to_download = [
        cfg.external_datasets[ds] for ds in cfg.external_datasets.to_download
    ]
    log.info("Downloading %s", cfg.external_datasets.to_download)
    download_files(*get_datasets_download_info(datasets_to_download))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

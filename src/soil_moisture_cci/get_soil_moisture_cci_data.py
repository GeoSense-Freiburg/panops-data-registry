"""
Downloads ESA CCI Soil Moisture daily L3S files (0.25 deg, NetCDF) from the CEDA archive.

Files are mirrored with the archive layout:
    <out_dir>/<PRODUCT>/<version>/<year>/ESACCI-SOILMOISTURE-L3S-*.nc

Existing files with the same size as on the server are skipped, so the script can be
re-run to resume an interrupted download.

Citation:
Dorigo, W., Wagner, W., Albergel, C., et al., 2017. ESA CCI Soil Moisture for improved
Earth system understanding: State-of-the art and future directions. Remote Sensing of
Environment 203: 185-215. https://doi.org/10.1016/j.rse.2017.07.001

Gruber, A., Scanlon, T., van der Schalie, R., Wagner, W., and Dorigo, W., 2019. Evolution
of the ESA CCI Soil Moisture climate data records and their underlying merging
methodology. Earth System Science Data 11: 717-739.
https://doi.org/10.5194/essd-11-717-2019
"""

import argparse
import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from dotenv import find_dotenv, load_dotenv
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from src.conf.parse_params import config
from src.utils.log_utils import setup_logger

load_dotenv(find_dotenv(), override=True, verbose=True)
project_root = os.environ["PROJECT_ROOT"]
log = setup_logger(__name__, "INFO")

# CEDA occasionally drops connections under many requests, so retry with backoff
session = requests.Session()
retries = Retry(
    total=8,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"],
)
session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=16))


def cli() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Download ESA CCI Soil Moisture data.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the files and total size that would be downloaded, then exit.",
    )
    parser.add_argument(
        "--test",
        "-t",
        action="store_true",
        help="Only download the first 3 files of the first year (quick check).",
    )
    return parser.parse_args()


def list_entries(url: str) -> list[str]:
    """Return the last path segment of every link in a CEDA directory listing."""
    response = session.get(url, timeout=60)
    response.raise_for_status()
    hrefs = re.findall(r'href="([^"?#]+)"', response.text)
    return sorted({h.rstrip("/").split("/")[-1] for h in hrefs})


def version_key(version: str) -> tuple[int, ...]:
    """Sort key for version strings like 'v09.1'."""
    return tuple(int(x) for x in re.findall(r"\d+", version))


def resolve_version(product_url: str, version: str) -> str:
    """Return the requested version, or the newest one on the server if 'latest'."""
    available = [e for e in list_entries(product_url) if re.fullmatch(r"v\d+\.\d+", e)]
    if not available:
        raise RuntimeError(f"No versions found at {product_url}")
    if version == "latest":
        return max(available, key=version_key)
    if version not in available:
        raise ValueError(f"Version {version} not in {available} at {product_url}")
    return version


def remote_size(url: str) -> int | None:
    """Content length of a remote file, or None if the server does not report it."""
    response = session.head(url, timeout=60, allow_redirects=True)
    response.raise_for_status()
    size = response.headers.get("content-length")
    return int(size) if size is not None else None


def download_file(url: str, out_path: Path) -> tuple[str, int, str]:
    """Download one file via a .part file; skip it if a same-sized copy exists."""
    size = remote_size(url)
    if out_path.exists() and (size is None or out_path.stat().st_size == size):
        return out_path.name, out_path.stat().st_size, "skipped"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    with session.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(1024 * 1024):
                f.write(chunk)

    if size is not None and tmp_path.stat().st_size != size:
        tmp_path.unlink()
        raise IOError(f"Size mismatch for {url}")
    tmp_path.rename(out_path)
    return out_path.name, size or out_path.stat().st_size, "downloaded"


def collect_files(cfg: dict, test: bool = False) -> list[tuple[str, Path]]:
    """Build the list of (url, local path) pairs for all requested products and years."""
    out_dir = Path(project_root, cfg["out_dir"])
    files = []
    for product in cfg["products"]:
        product_url = f"{cfg['base_url'].rstrip('/')}/{product}/"
        version = resolve_version(product_url, cfg["version"])
        version_url = f"{product_url}{version}/"
        log.info("%s: using version %s", product, version)

        years = [int(e) for e in list_entries(version_url) if re.fullmatch(r"\d{4}", e)]
        if cfg.get("year_start"):
            years = [y for y in years if y >= cfg["year_start"]]
        if cfg.get("year_end"):
            years = [y for y in years if y <= cfg["year_end"]]
        if test:
            years = years[:1]

        for year in tqdm(years, desc=f"Listing {product}"):
            year_url = f"{version_url}{year}/"
            names = [
                e
                for e in list_entries(year_url)
                if e.startswith("ESACCI-SOILMOISTURE") and e.endswith(".nc")
            ]
            if test:
                names = names[:3]
            files += [
                (f"{year_url}{n}", out_dir / product / version / str(year) / n)
                for n in names
            ]
    return files


def main(cfg: dict = config["soil_moisture_cci"]) -> None:
    """Download ESA CCI Soil Moisture daily files."""
    args = cli()
    files = collect_files(cfg, test=args.test)
    log.info("Found %d files", len(files))

    if args.dry_run:
        with ThreadPoolExecutor(cfg["n_workers"]) as pool:
            sizes = list(
                tqdm(
                    pool.map(lambda f: remote_size(f[0]) or 0, files),
                    total=len(files),
                    desc="Checking sizes",
                )
            )
        log.info("Total size: %.1f GB", sum(sizes) / 1e9)
        return

    out_dir = Path(project_root, cfg["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest, failed = [], []
    with ThreadPoolExecutor(cfg["n_workers"]) as pool:
        futures = {pool.submit(download_file, url, path): url for url, path in files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):
            url = futures[future]
            try:
                name, size, status = future.result()
                manifest.append((name, size, status, url))
            except (requests.RequestException, IOError) as e:
                log.warning("Failed: %s (%s)", url, e)
                failed.append(url)

    with open(out_dir / "manifest.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["file", "bytes", "status", "url"])
        writer.writerows(sorted(manifest))

    if failed:
        (out_dir / "failed.txt").write_text("\n".join(failed) + "\n", encoding="utf-8")
        log.warning("%d files failed; re-run the script to retry them", len(failed))
    log.info("Done.")


if __name__ == "__main__":
    main()

PANOPS Data Registry
==============================

A data registry of data commonly used in the PANOPS project of the Geosense group at the
University of Freiburg.

Project Organization
------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── data               <- Data available for download with `dvc get` or `dvc import`
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   └── gbif           <- Downloads GBIF data according to a query object in references
    │       └── download_gbif.py
    │
    ├── tests              <- Pytest test scripts
    ├── .dvc               <- Contains DVC tmp files (IMPORTANT! The DVC cache lives elsewhere in the filesystem)
    ├── dvc.yaml           <- Defines DVC pipeline stages
    ├── params.yaml        <- Define parameters used in DVC pipeline stages
    ├── pyproject.toml     <- Human-readable project dependencies managed with Poetry
    ├── poetry.lock        <- File used by Poetry to install dependencies
    └── .pre-commit-config.yaml <- pre-commit Git hooks


--------

<p><small>Project structure based loosely on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

## Usage
### Important!
This project is still in active development, so before using it, please contact daniel.lusk@geosense.uni-freiburg.de.

### Downloading data (TODO: Update with correct instructions)
Downloading data from this registry is simple—that's the point!

To see what data is available, use `dvc list`, for example:
```bash
dvc list -R --dvc-only path/to/panops-data-registry data
```
The above command lists all files available in the `data` directory of the PANOPS data registry (located in this case at `path/to/panops-data-registry`). The `-R` flag tells DVC to list all directory contents recursively, and `--dvc-only` only shows the actual data, excluding system files like `__pycache__` and `.gitignore`.

Download the data you want with `dvc get` or `dvc import`. Use `dvc get` if you don't want to automatically track your local copy of the data in your own DVC project. Using `dvc import` checks the files into DVC versioning.
```bash
dvc import path/to/panops-data-registry data/raw/gbif/all_tracheophyta.zip -o myproject/data/raw/gbif
```

> [!NOTE]
> DVC supports remote storage, just like Git. In this case, you would **not** want to set this registry as a DVC remote for your project, since it should be considered *read-only*. Configure a DVC remote for your project only if you want to push your data (and its intermediate transformations and features) to a project-specific remote.

## Development
### Setup

#### 1. Clone the project repository

```bash
git clone https://github.com/GeoSense-Freiburg/panops-data-registry
cd panops-data-registry
```

#### 2. Install poetry and DVC.

```bash
pipx install poetry==~1.7
pipx install dvc
```
> [!TIP]
> If you don't have root access, you can use `condax`, a conda-flavored implementation of `pipx`.
> ```bash
> pip install --user condax
> ```

#### 3. Create a virtual environment

```bash
conda create -n panops-data-registry -c conda-forge python=3.10
conda activate panops-data-registry
```

#### 4. Install dependencies
Install dependencies with Poetry.
```bash
poetry install
```

#### 5. Install pre-commit Git hooks (optional)

```bash
pre-commit install
```

> [!NOTE]
> If you'd prefer not to use `pre-commit` for managing Git hooks, simply remove `pre-commit` with
> ```bash
> poetry remove pre-commit
> ```
> If you've already installed the hooks with `pre-commit install`, you'll need to run `pre-commit uninstall`.

 

### Generate datasets directly (no DVC)
You can run each stage individually without `dvc repro`. Most stages read their default parameters from `params.yaml`. Adjust values there as needed before running the corresponding command.

> Note: Some stages export via Google Earth Engine (GEE) to Google Cloud Storage (GCS) and then download to the local filesystem. Ensure your environment is authenticated for GEE and that the GCS buckets referenced in `params.yaml` exist and are accessible.

#### GBIF occurrences
- **Script**: `src/gbif/get_gbif_data.py`
- **Key parameters (via CLI flags)**:
  - `-q/--query`: Path to GBIF query JSON (e.g., `references/gbif/query_all_tracheophyta.json`).
  - `-n/--name`: Human-readable name used for the downloaded files.
  - `-o/--output`: Output directory (must exist) where files are saved.
  - `-k/--key` (optional): Existing GBIF download key to re-download.
- **Run**:
```bash
mkdir -p data/gbif
python src/gbif/get_gbif_data.py \
  -q references/gbif/query_all_tracheophyta.json \
  -n all_tracheophyta_[YYYY-MM-DD] \
  -o data/gbif
```

#### MODIS monthly averages
- **Script**: `src/modis/get_modis_data.py`
- **Configure in `params.yaml` under `modis`**: `date_start`, `date_end`, `product`, `bands`, `qa_band`, `nodata`, `crs`, `scale`, `target`, `bucket`, `out_dir`.
- **Flags**: `-v/--verbose`, `-t/--test` (shorter time range, quicker run).
- **Run**:
```bash
python src/modis/get_modis_data.py -v
```

#### ESA WorldCover
- **Script**: `src/esa_worldcover/get_esa_worldcover_data.py`
- **Configure in `params.yaml` under `esa_worldcover`**: `collection_id`, `crs`, `scale`, `target`, `bucket`, `out_path`.
- **Flags**: `-v/--verbose`, `-d/--download` (download from GCS after export).
- **Run**:
```bash
python src/esa_worldcover/get_esa_worldcover_data.py -v -d
```

#### SoilGrids
- **Script**: `src/soilgrids/get_soilgrids_data.py`
- **Configure in `params.yaml` under `soilgrids`**: `collection_id`, `soil_properties`, `soil_stat`, `nodata`, `crs`, `scale`, `target`, `bucket`, `out_dir`.
- **Flags**: `-v/--verbose`, `-d/--download`, `--dry-run` (create tasks only).
- **Run**:
```bash
python src/soilgrids/get_soilgrids_data.py -v -d
```

#### WorldClim BIO variables
- **Script**: `src/worldclim/get_worldclim_data.py`
- **Configure in `params.yaml` under `worldclim`**: `url`, `out_dir`.
- **Run**:
```bash
python src/worldclim/get_worldclim_data.py
```

#### Canopy height (ETH 2020)
- **Script**: `src/canopy_height/get_canopy_height_data.py`
- **Configure in `params.yaml` under `canopy_height`**: `height_collection`, `sd_collection`, `nodata`, `crs`, `scale`, `target`, `bucket`, `out_dir`.
- **Run**:
```bash
python src/canopy_height/get_canopy_height_data.py
```

#### VODCA
- **Script**: `src/vodca/get_vodca_data.py`
- **Configure in `params.yaml` under `vodca`**: `collection_base`, `bands`, `percentiles`, `crs`, `scale`, `target`, `bucket`, `out_dir`.
- **Flags**: `--dry-run` (create tasks only), `--merge-only` (only merge multipart files already downloaded).
- **Run**:
```bash
python src/vodca/get_vodca_data.py
```

#### ALOS CHILI (Compound Topographic Index)
- **Script**: `src/topography/chili/get_alos_chili.py`
- **Configure in `params.yaml`**:
  - Under `ee`: `project_id`, `high_volume`.
  - Under `alos_chili`: `collection_id`, `band`, `nodata`, `crs`, `scale`, `target`, `bucket`, `out_dir`.
- **Flags**: `-d/--debug` (coarser scale and debug output directory).
- **Run**:
```bash
python src/topography/chili/get_alos_chili.py
```

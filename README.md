# Geosense EO Data Registry
This is a data registry of Earth observation data commonly used by the Geosense group at the University of Freiburg.

## Usage
Coming soon!

## Development setup

### 1. Clone the project repository

```bash
git clone https://github.com/dluks/eo-data
cd eo-data
```

### 2. Install poetry and DVC.

```bash
pipx install poetry==~1.7
pipx install dvc
```
> [!TIP]
> If you don't have root access, you can use `condax`, a conda-flavored implementation of `pipx`.
> ```bash
> pip install --user condax
> ```

### 3. Create a virtual environment

```bash
mamba create -n eo-data -c conda-forge python=3.10
mamba activate eo-data
```

> [!NOTE]
> `mamba` is simply a faster implementation of `conda`. If you don't have `mamba` installed, `conda` will also work.

### 4. Install dependencies
This checks to confirm python and conda environment setup. If on Windows, or if you'd simply prefer not to use `make`, you can use `poetry install` instead.
```bash
make requirements
```

### 5. Install pre-commit Git hooks (optional)

```bash
pre-commit install
```

> [!NOTE]
> If you'd prefer not to use `pre-commit` for managing Git hooks, simply remove `pre-commit` with
> ```bash
> poetry remove pre-commit
> ```
> If you've already installed the hooks with `pre-commit install`, you'll need to run `pre-commit uninstall`.

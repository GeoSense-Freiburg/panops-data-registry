Geosense EO Data Registry
==============================

A data registry of Earth observation data commonly used by the Geosense group at the University of Freiburg.

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data               <- Data available for use with DVC
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    │── pyproject.toml    <- Human-readable project dependencies managed with Poetry
    │── poetry.lock       <- File used by Poetry to install dependencies
    └── .pre-commit-config.yaml <- pre-commit Git hooks


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

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

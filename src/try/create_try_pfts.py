import logging
import os
import zipfile
from pathlib import Path

import pandas as pd


def main() -> None:
    log = logging.getLogger(__name__)

    log.info("Reading zipfile...")
    ZIP_PATH = Path("./data/try/TRY_Life_Forms.zip")

    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        with zip_ref.open("TRY_Life_Forms/19233.txt") as f:
            df = pd.read_csv(f, delimiter="\t", encoding="ISO-8859-1").drop(
                columns=["Unnamed: 27"]
            )

    log.info("Processing data...")
    # Select columns of interest and convert all original pft values to lowercase
    df = df[["AccSpeciesID", "AccSpeciesName", "OrigValueStr"]].assign(
        OrigValueStr=lambda _df: _df["OrigValueStr"].str.lower()
    )

    # Convert columns to more efficient data types
    df = df.astype(
        {
            "AccSpeciesID": "uint32[pyarrow]",
            "AccSpeciesName": "string[pyarrow]",
            "OrigValueStr": "category",
        }
    )

    # Define the search terms (received from Teja)
    search_terms_tree = [
        "tree",
        "Tree",
        "TREE",
        "seedling",
        "hardwood",
        "softwood",
        "Hardwood",
        "Softwood",
        "Tree_Fern",
        "Tree_",
        "Small_Tree",
        "trees/T/Tree",
        "Tree/Tree",
        "Tree V",
        "Tree VII",
        "Tree IX",
        "Tree V",
        "Tree IV",
        "Tree III",
        "Tree II",
        "Tree I",
        "Tree/Treelet",
        "Treen",
        "T/Tree",
        "T/tree/Tree",
        "tree/Tree",
        "trees/Shrub",
        "trees/T/tree/Tree",
        "Tree/Shrub",
        "Trees",
        "trees",
        "Tree_Shrub",
        "Shrub_Tree",
        "Tree | Shrub",
        "Shrub | Tree",
        "Tree | Tree",
        "USforestTrees",
    ]

    search_terms_grass = [
        "herb",
        "Herb",
        "HERB",
        "herbs",
        "graminoid",
        "Graminoid",
        "GRAMINOID",
        "Forb",
        "forb",
        "Grasses&Sedges",
        "Grass",
        "grass",
        "GRASS",
        "sedge",
        "SEDGE",
        "fern",
        "Fern",
        "FERN",
        "Grassland",
        "Annual Grass",
        "Perennial Grass",
        "grassland",
    ]

    search_terms_shrub = [
        "shrub",
        "Shrub",
        "SHRUB",
        "seedling",
        "vine",
        "Vine",
        "VINE",
        "liana",
        "Liana",
        "LIANA",
        "Terrestrial_Shrub",
        "Shrub forest belt",
        "Dwarf Shrub community",
        "Shrub/Aquatic",
        "Shrub/Aquatic",
        "Shrub/Parasite",
        "Shrubs",
        "Shrubland",
        "shrubland",
        "Shrub, Subshrub",
        "Shrub",
    ]

    # Convert all terms to lowercase and remove duplicates
    search_terms_tree = set([t.lower() for t in search_terms_tree])
    search_terms_grass = set([t.lower() for t in search_terms_grass])
    search_terms_shrub = set([t.lower() for t in search_terms_shrub])

    log.info("Assigning PFTs to species...")
    # Assign PFTs to the species
    df.loc[df["OrigValueStr"].isin(search_terms_tree), "pft"] = "Tree"
    df.loc[df["OrigValueStr"].isin(search_terms_grass), "pft"] = "Grass"
    df.loc[df["OrigValueStr"].isin(search_terms_shrub), "pft"] = "Shrub"
    df = df.dropna(subset=["pft"])

    log.info("Grouping by species and selecting majority PFT...")
    # Group by species and select the most common PFT (majority count)
    df = (
        df.groupby("AccSpeciesID")
        .agg(
            AccSpeciesName=("AccSpeciesName", "first"),
            pft=("pft", lambda x: x.mode()[0]),
        )
        .reset_index()
    )

    log.info("Saving data...")
    # Save to file
    df.to_parquet("./data/try/try_pft_v2.parquet")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
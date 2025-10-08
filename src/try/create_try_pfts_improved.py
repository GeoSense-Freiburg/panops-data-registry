import logging
import re
from pathlib import Path

import pandas as pd


def assign_pft_priority(growth_form: str, search_terms: dict) -> str | None:
    """
    Assign PFT using priority-based matching.

    Priority order: Tree > Shrub > Grass

    For compound forms like "herb_tree_shrub", this will:
    1. Split by common delimiters (_, /, |, comma, space)
    2. Check each token against search terms
    3. Return the highest priority match
    """
    if pd.isna(growth_form):
        return None

    # Filter out non-growth forms
    non_growth_forms = {
        "yes",
        "no",
        "absence",
        "presence",
        "unspecified",
        "?",
        "`",
        "rounded",
        "conical",
        "oval",
        "vase",
        "irregular",
        "single crown",
        "soli",
        "mult",
        "ss",
        "c+sc",
        "nd",
        "various",
    }

    # Check if it's a number or clearly not a growth form
    if growth_form in non_growth_forms:
        return None

    # Try to parse as float - if successful, it's not a growth form
    try:
        float(growth_form)
        return None
    except ValueError:
        pass

    # First try exact match (already lowercase)
    for pft, terms in search_terms.items():
        if growth_form in terms:
            return pft

    # Clean and tokenize the growth form
    # Split by common delimiters: underscore, slash, pipe, comma, space
    tokens = re.split(r"[_/|,\s]+", growth_form.strip('"').strip())

    # Priority order
    priority_order = ["Tree", "Shrub", "Grass"]

    # Check each token against search terms
    found_pfts = set()
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        for pft, terms in search_terms.items():
            if token in terms:
                found_pfts.add(pft)
                break

    # Return highest priority match
    for pft in priority_order:
        if pft in found_pfts:
            return pft

    # If no match, try substring matching (contains)
    for pft in priority_order:
        for term in search_terms[pft]:
            if term in growth_form and len(term) > 2:  # avoid short false positives
                return pft

    return None


def main() -> None:
    log = logging.getLogger(__name__)

    log.info("Reading data...")
    fp = Path("./data/try/try_growth_forms.parquet")

    df = pd.read_parquet(fp)

    log.info("Processing data...")
    df = df.assign(AccSpeciesName=lambda _df: _df["AccSpeciesName"].str.lower())

    # Define the search terms (updated with additional terms)
    search_terms_tree = [
        # Basic tree terms
        "tree",
        "Tree",
        "TREE",
        "trees",
        "Trees",
        "t",
        "seedling",
        "seedlings",
        "sapling",
        "saplings",
        # Tree size/age
        "small tree",
        "Small_Tree",
        "small_tree",
        "large tree",
        "tree-like",
        "tre",
        "treen",
        # Canopy position
        "canopy tree",
        "canopy_tree",
        "top.canopy.tree",
        "top canopy",
        "mid canopy",
        "mid.canopy.tree",
        "midtree",
        "mid.canopy.tree",
        "understory tree",
        "understory",
        "midstory",
        # Treelet
        "treelet",
        "Tree/Treelet",
        "tree/treelet",
        # Conifer/gymnosperm
        "conifer",
        "conifers",
        "gymnosperm",
        "gymn",
        # Angiosperm
        "angiosperm",
        "angwood",
        # Palms
        "palm",
        "pam",
        "palm tree",
        "palmtree",
        "canopy_palm",
        "understory_palm",
        "palm (p)",
        "palm resp. palm",
        # Deciduous/evergreen
        "tree (deciduous)",
        "tree (evergreen)",
        "woody deciduous",
        "woody evergreen",
        "deciduous shrub or tree",
        "evergreen shrub or tree",
        "semi deciduous tree or shrub",
        # Ferns
        "tree fern",
        "treefern",
        "Tree_Fern",
        "arborescent_fern",
        "Tree_",
        "t (treefern)",
        # Woody
        "hardwood",
        "softwood",
        "Hardwood",
        "Softwood",
        "woody plant",
        "woody species",
        "woody plants",
        "woody",
        "w",
        # Mangrove
        "mangrove",
        "tree/mangrove/woody",
        "shrub/tree/mangrove/woody",
        # Hemiepiphytes
        "tree/hemiepiphyte",
        "hemiepiphyte",
        "hemiepiphite",
        "hemi-epiphyte",
        "hemi-epipjyte",
        "tree/hemiepiphyte/woody",
        # Tree forms/shapes
        "columnar",
        "conical",
        "pachicaul",
        # Crops
        "tree crop",
        # Spanish/other
        "arbol",
        # Tree combinations
        "tree/shrub",
        "tree / shrub",
        "tree shrub",
        "shrub/tree",
        "shrub / tree",
        "tree-shrub",
        "tree_shrub",
        "shrub_tree",
        "t/s",
        "t/tree",
        "tree/tree",
        "tree | shrub",
        "shrub | tree",
        "tree | tree",
        "tree|shrub",
        "tree shrub intermediate",
        "tree/large shrub",
        "tree/shrub/climber",
        "smtree",
        "tree/ /woody",
        "tree/woody",
        # TRY database patterns
        "trees/T/Tree",
        "Tree/Tree",
        "T/Tree",
        "T/tree/Tree",
        "tree/Tree",
        "trees/Shrub",
        "trees/T/tree/Tree",
        "trees/tree",
        "trees/tree/tree",
        "t/tree/tree",
        "Tree/Shrub",
        "trees/T",
        "trees/t",
        "trees/t/tree",
        # Roman numerals
        "Tree V",
        "Tree VII",
        "Tree IX",
        "Tree IV",
        "Tree III",
        "Tree II",
        "Tree I",
        # Abbreviations
        "tree (t)",
        "t resp. t",
        "a t",
        "st",
        "lt",
        "slt",
        # Forest types
        "USforestTrees",
        # Successional stages
        "early-successional",
        "mid-successional",
        "late-successional",
    ]

    search_terms_grass = [
        # Herb terms
        "herb",
        "Herb",
        "HERB",
        "herbs",
        "herb.",
        "herb (h)",
        "h",
        "herbaceous",
        "herbaceous monocot",
        "herbaceous dicot",
        "herbaceous dicotyl",
        "herbaceous monocotyl",
        "herbaceous forb",
        "herbaceous legume",
        "herbaceous plant",
        "herbaceous/terrestrial herb",
        "terrestrial herb",
        "terrestrial_herb",
        "angherb",
        "herb_erect",
        "hierba_",
        # Forb
        "forb",
        "forbs",
        "Forb",
        "forb/herb",
        "frobs",
        "annual forb",
        "perennial forb",
        "variable forb",
        "leguminous forb",
        "forb-annual",
        "forb-biennial",
        # Graminoid/grass
        "graminoid",
        "Graminoid",
        "GRAMINOID",
        "graminoids",
        "gram",
        "gras",
        "grass",
        "Grass",
        "GRASS",
        "grasses",
        "g",
        "grass (poaceae only)",
        "c3 grass",
        "c4 grass",
        "c3.sedges",
        "grass (clonal)",
        "grass (tussock)",
        "tuss",
        "forage grass",
        "pasture grass",
        "prairie grass",
        "woody grass",
        "annual grass",
        "perennial grass",
        "Annual Grass",
        "Perennial Grass",
        "annual graminoid",
        "perennial graminoid",
        "variable graminoid",
        "graminoid-annual",
        "bunchgrasses",
        "rhizome grass",
        # Sedges
        "sedge",
        "SEDGE",
        "seges",
        # Grasses&sedges combined
        "Grasses&Sedges",
        "grasses&sedges",
        "g&s",
        "g&s resp. g&s",
        # Ferns
        "fern",
        "Fern",
        "FERN",
        "ferns",
        "fern ally",
        "fernally",
        "pteridophyte",
        "ferns and allies (lycophytes)",
        "fern or fern ally",
        "fern/non-woody",
        "fern/aquatic",
        "fern/aquatic/non-woody",
        "fern/woody",
        "fern/palmoid/woody",
        "fern/epiphyte/non-woody",
        "terrestrial_fern",
        "terrestrial fern",
        # Allies
        "clubmoss",
        "club moss",
        "horsetail",
        "lycopodiophyta",
        "selaginella",
        # Moss/bryophyte
        "moss",
        "turf moss",
        "bryophyte",
        "nonvascular",
        # Lichen
        "lichen",
        "lichenous",
        "lichen/non-woody",
        "foliose lichen",
        "fruticose lichen",
        # Life cycles
        "annual",
        "perennial",
        "therophyte",
        "hemicryptophyte",
        "annual-biennial",
        "perennial herb",
        "perennial herb/hemicryptophyte",
        "herbaceous perennial",
        "annual herb",
        "herbaceous annual",
        "herbaceous annual-biennial",
        "perennial graminoid",
        "perennial leguminous herb",
        "perennial grass/hemicryptophyte",
        # Geophyte
        "geophyte",
        "geop",
        "bulb",
        "rhizomatous",
        "rhiz",
        "rhizimatous",
        "rhizomatous/bulbs",
        "perennial, rhizomatous",
        # Bamboo
        "bamboo",
        # Aquatic herbs
        "aquatic",
        "aquatic forb",
        "hydrophyte",
        "hydrophytes",
        "waterplant",
        "aquatic fresh water",
        "aquatic, fresh water, floating",
        "submerged",
        "amphibiousubmerged",
        "emergent attached to the substrate",
        "submerged attached to the substrate",
        "floating leaves attached to the substrate",
        "free-floating plants",
        "herb/aquatic",
        "herb/aquatic/non-woody",
        "hydrophyte-annual",
        "hyd",
        "n hyd",
        "macrophyte",
        "aquativ",  # Added macrophyte
        # Rosette
        "rosette",
        "rosette plant",
        "semi-rosette",
        "rosette forb",
        # Rush
        "rush",
        # Grasslike
        "grasslike",
        # Legumes
        "legume",
        "legumes",
        "annual legume",
        "perennial legume",  # Added annual/perennial legume
        # Cereals
        "cereal",
        # Weeds
        "weed",
        "weedy",
        "weed, sedge",
        # Grassland
        "Grassland",
        "grassland",
        # Abbreviations
        "herb resp. h",
        "b h",
        "hel",
        "m hel",
        "n",
        "a",
        "hs",
        "hl",
        "ha",
        "hsl",
        "hst",
        "hslt",
        "hsa",
        "el",
        # Crop
        "crops",
        "crop",
        # Other
        "extensive-stemmed herb",
        "small_herb_",
        "annuals",  # Added
    ]

    search_terms_shrub = [
        # Basic shrub terms
        "shrub",
        "Shrub",
        "SHRUB",
        "shrubs",
        "Shrubs",
        "s",
        "sh",
        "shru",
        "shrub (s)",
        "s resp. s",
        "srub",
        "shurb",
        "arbusto",
        # Subshrub
        "subshrub",
        "sub-shrub",
        "sub shrub",
        "subshrub (woody <1m)",
        "sub-shrub (chamaephyte)",
        "suffrutescent",
        "subshurb",
        # Dwarf shrub
        "dwarf shrub",
        "erect dwarf shrub",
        "prostrate dwarf shrub",
        "evergreen dwarf shrub",
        "drwarf shrub",
        "Dwarf Shrub community",
        "dwarf semishrub",  # Added
        # Shrub size
        "small shrub",
        "small_shrub",
        "large shrub",
        "low to high shrub",
        # Arborescent
        "arborescent shrubs",
        # Chamaephyte
        "chamaephyte",
        "chaemaephyte",
        "nano-chamaephyte",
        "shrub (chamaephyte)",
        "chaemaephyte | shrub",
        "shrub | chaemaephyte",
        "chaemaephyte | nano-chamaephyte",
        "chaemaephyte | vine",
        "chasmophyte",  # Added chasmophyte
        # Woody shrub
        "woody shrub",
        "shrub/woody",
        "shrub (woody 1-4m)",
        # Mallee
        "mallee",
        # Vine terms
        "vine",
        "Vine",
        "VINE",
        "v",
        "vines",
        "woody vine",
        "herbaceous vine",
        "liana/woody vine",
        "liana/woody vine|shrub",
        "climbing vine",
        "climbing_vine",
        "scandent_vine",
        "trailing_vine",
        "herbaceous vine|herb",
        "vine|herb",
        "herbaceous vine|liana/woody vine",
        "vine resp. climb",
        "shrub/vine",  # FIXED - added this term that was missing due to syntax error
        "w climb resp. v",
        "climb resp. v",
        "climb resp. climb",
        # Liana
        "liana",
        "Liana",
        "LIANA",
        "lianas",
        "l",
        "lian",
        "lianna",
        "woody liana",
        "l/woody liana",
        "lianas/lianna/woody liana",
        "lianas/woody liana",
        "lianas and climbers",
        "lianas/climber",
        "lianas/lianna",
        "lianas (wody climbers)",
        # Climber/creeper
        "climber",
        "climb",
        "climbing",
        "climber or creeper",
        "creeper",
        "climber/vine",
        "climber/liana",
        "climber/non-woody",
        "climber/woody",
        "climber/non-woody/woody",
        "twiner/climber.",
        "twiner/climber",
        "climber/palmoid/woody",  # Added
        # Epiphyte
        "epiphyte",
        "epiphytic",
        "epiphytes",
        "ep",
        "e",
        "epiphyte (e)",
        "epiphyte (mistletoe)",
        "epiphytic herb",
        "epiphytic_herb",
        "epiphyte_herb",
        "hemiepiphyte",
        "hemiepiphytes",
        "hemi-epiphyte",
        "epiphyte/hemiepiphyte",
        # Parasite
        "parasite",
        "hemi-parasite",
        "hemiparasite",
        "hemi-parasitic",
        "stem parasite",
        "root parasite",
        "woody parasite",
        "parasitic climber",
        "holoparasitic",
        "mistletoe",
        "parasite_epiphyte",
        # Succulent
        "succulent",
        "succulents",
        "succulent leaves",
        "succulent stems",
        "stem succulent",
        "leaf succulent",
        "rosette leaf succulent",
        "tall stem succulent",
        "stem and leaf succulent",
        "forb-succulent",
        "l succ",
        "i succ",
        "caudiciform",  # Added
        # Cactus
        "cactus",
        "cacti",
        "cact",
        "cacti (c)",
        "agaves&cacti",
        "agaves&cacti resp. agaves&cacti",
        # Carnivore
        "carnivore",
        "carnivorous",
        "carnivorous plant",
        "carnivorous plant resp. carnivorous plant",
        # Cushion
        "cushion",
        "cushion plant",
        "cushion forming",
        # Mat forming
        "mat forming",
        "mat-forming",
        # Aquatic
        "aquatic",
        "Shrub/Aquatic",
        "shrub/aquatic",
        "aquatic plants, submerged",
        "aquatic plants, floating",
        "semi-aquatic",
        "subaquatic",
        "aquatic/semi-aquatic",  # Added
        # Growth forms
        "stem erect",
        "erect",
        "stem ascending to prostrate",
        "ascending",
        "stem prostrate",
        "prostrate",
        "decumbent",
        "trailing",
        "semi-erect",
        "always climbing using tendrils",
        "always trailing",
        "always spread climbing",
        "always climbing using adhesive roots",
        "sometimes spread climbing",
        "trailing_herb",
        "trailing_herbaceous_vine",
        "trailing_plant",
        "terrestrial_trailing_plant",
        # Thicket
        "thicket forming",
        # Branching
        "bunch",
        "colonizing",
        "multiple stems",
        "multi-stem",
        "multiple stem",
        "single stem",
        "single stem or multi stemed",
        "branchy",
        # Rosette
        "ros",
        "rosette",
        # Scapose
        "scap",
        "scapose",
        # Caespitose
        "caesp",
        "caespitose",
        # Reptant
        "rept",
        "reptant",
        # Stoloniferous
        "stoloniferous",
        # Rhizomatous
        "rhizomatous",
        "rhiz",
        # Shrub combinations
        "shrub/tree",
        "tree/shrub",
        "tree / shrub",
        "shrub / tree",
        "shrub_tree",
        "tree_shrub",
        "shrub/tree intermediate",
        "shrub | tree",
        "tree | shrub",
        "shrub|tree",
        "tree|shrub",
        "Terrestrial_Shrub",
        "terrestrial_shrub",
        # Shrub land
        "Shrubland",
        "shrubland",
        "Shrub forest belt",
        # Shrub, Subshrub
        "Shrub, Subshrub",
        "shrubs and sub-shrubs",
        # Shrub/Parasite
        "Shrub/Parasite",
        # Abbreviations
        "halfshrubs resp. s",
        "shrub-like_clumps",
        "shrub-like_herb",
        "se",
        "rus",
        # Desert
        "desert sub-shrubs",
        # Free
        "free",
        "free-standing",
        # Climber variations
        "climber/epiphyte",
        "climber/epiphyte/parasitic",
        "climber/epiphyte/succulent",
        "climber/fern",
        "climber/hemiepiphyte",
        "climber/herb",
        "climber/succulent",
        "climber/free/liana",
        "climber/free/shrub",
        "climber/free/understory",
        "climber/free/vine",
        "climber/parasitic",
        "climbing_epiphyte",
        "climibing_herb",  # Added (typo in original data)
    ]

    # Convert all terms to lowercase and create a dictionary
    search_terms = {
        "Tree": set([t.lower() for t in search_terms_tree]),
        "Shrub": set([t.lower() for t in search_terms_shrub]),
        "Grass": set([t.lower() for t in search_terms_grass]),
    }

    log.info("Assigning PFTs to species using improved matching...")
    # Assign PFTs using the improved priority-based matching
    df["pft"] = df["X42"].apply(lambda x: assign_pft_priority(x, search_terms))

    # Drop rows without PFT assignment
    df = df.dropna(subset=["pft"])

    log.info("Grouping by species and selecting majority PFT...")
    # Group by species and select the most common PFT (majority count)
    df = (
        df.groupby("AccSpeciesName")
        .agg(
            pft=("pft", lambda x: x.mode()[0]),
        )
        .reset_index()
    )

    log.info("Saving data...")
    # Save to file
    df.to_parquet("./data/try/try_pfts.parquet")

    log.info(f"Saved {len(df):,} species with PFT assignments")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()


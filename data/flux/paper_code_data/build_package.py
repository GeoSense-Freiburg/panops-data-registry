#!/usr/bin/env python
"""
Assemble the code-and-data package for the paper (Nature Ecology & Evolution submission) from the project.

The package MIRRORS the project's folder layout (scripts/, AutoGluon/, notebooks/, derived_tables/, plots/), because the
scripts find each other and their inputs through these relative paths. All absolute paths of the analysis server are
rewritten to a package root that is read at run time from the environment variable PAPER_ROOT (default: the current
working directory, i.e. run everything from the package root). Interpreters are called as plain `Rscript` / `python`.
Notebook outputs are stripped. Afterwards the package is checked for leftover server paths.

    /home/nk1125/miniconda3/envs/autogluon_env/bin/python paper_code_data/build_package.py
"""
import json, os, re, shutil, subprocess, sys
import pandas as pd

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
PKG = os.path.join(ROOT, "paper_code_data")
RSCRIPT = "/home/nk1125/miniconda3/envs/clean_r_env/bin/Rscript"
PYTHON = "/home/nk1125/miniconda3/envs/autogluon_env/bin/python"

CODE = [  # paths relative to the project root = paths inside the package
    "notebooks/12_FLUXNET_shuttle.ipynb", "notebooks/14_calc_EFPs.ipynb",
    "scripts/paper_v41/02_nepmax90_gs_monthly_meteo.R",
    "scripts/paper_v41/01_mortality_metrics_v41.py",
    "scripts/paper_v41/paper_common.R", "scripts/paper_v41/03_build_paper_dataset.R", "scripts/paper_v41/04_traits_lusk_1km.R",
    "AutoGluon/ag_common.py", "AutoGluon/01_fit_full.py", "AutoGluon/02_loso_repcv.py", "AutoGluon/03_shap.py",
    "AutoGluon/run_efp.sh", "AutoGluon/run_efpclim.sh", "AutoGluon/05_figures.sh", "AutoGluon/05b_figures_ag_only.sh",
    "scripts/optuna_tune.py", "scripts/paper_v41/10_optuna_objective.R", "scripts/paper_v41/13_collect_optuna.py",
    "scripts/paper_v41/11_train_repcv.R", "scripts/paper_v41/12_shap_M4.R", "scripts/paper_v41/run_variant.sh",
    "scripts/paper_v41/31_table_fig2_summary.R", "scripts/paper_v41/37_mortality_dropcol.R",
    "scripts/paper_v41/38_mortality_block_summary.R", "scripts/paper_v41/39_paired_bootstrap.R",
    "scripts/paper_v41/40_table_bootstrap.R", "scripts/paper_v41/41_mortality_dose_response.R",
    "scripts/paper_v41/42_dose_response_robustness.R", "scripts/paper_v41/43_residual_by_mortality_bin.R",
    "scripts/paper_v41/20_fig1_site_map.R", "scripts/paper_v41/21_fig2_M3M4.R", "scripts/paper_v41/23_fig4_signed_shap.R",
    "scripts/paper_v41/24_fig5_igbp_board.R", "scripts/paper_v41/25_fig3b_site_signed_shap.R",
    "scripts/paper_v41/26_D_block_composition.R", "scripts/paper_v41/29_site_delta_rmse.R",
    "scripts/paper_v41/32_fig3_landscape.R",
    # Nature-style figures of the current draft (2026-10-21) and the respiration check
    "scripts/paper_v41/fig_style.R", "scripts/paper_v41/50_figures_nature.R", "scripts/paper_v41/51_appendix_figures_nature.R",
    "scripts/paper_v41/52_skill_bars.R", "scripts/paper_v41/53_reco_mortality_check.R",
    "scripts/paper_v41/55_appendix_shap_composition.R",
]

DATA = [  # analysis-ready inputs and results, at the paths the scripts use
    "derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
    "derived_tables/paper_v5/paper_dataset_counts_minrun2_buf100-200-300-400-500.txt",
    "AutoGluon/outputs/efp_v5_nothresh/AG_predictions_repCV.csv", "AutoGluon/outputs/efp_v5_nothresh/AG_metrics_repCV.csv",
    "AutoGluon/outputs/efp_v5_nothresh/AG_site_signed_shap_M4.csv", "AutoGluon/outputs/efp_v5_nothresh/AG_site_shap_M4.csv",
    "AutoGluon/outputs/efp_v5_nothresh/ensemble_weights_loso.csv",
    "AutoGluon/outputs/efp_v5_nothresh_efpclim/AG_predictions_repCV.csv", "AutoGluon/outputs/efp_v5_nothresh_efpclim/AG_metrics_repCV.csv",
    "derived_tables/paper_v5_nothresh/models/XGB_predictions_repCV.csv", "derived_tables/paper_v5_nothresh/models/LGB_predictions_repCV.csv",
    "derived_tables/paper_v5_nothresh/models/XGB_metrics_repCV.csv", "derived_tables/paper_v5_nothresh/models/LGB_metrics_repCV.csv",
    "derived_tables/paper_v5_nothresh/models/XGB_site_signed_shap_M4.csv", "derived_tables/paper_v5_nothresh/models/LGB_site_signed_shap_M4.csv",
    "derived_tables/paper_v5_nothresh/models/XGB_site_shap_M4.csv", "derived_tables/paper_v5_nothresh/models/LGB_site_shap_M4.csv",
    "plots/paper_v5_nothresh/Optuna/12m/best_configs.csv", "plots/paper_v5_nothresh/Optuna/24m/best_configs.csv",
    "plots/paper_v5_nothresh/paired_bootstrap_statistics.csv",
    "plots/paper_v5_nothresh/paired_bootstrap_statistics_efp_v5_nothresh_efpclim.csv",
    "plots/paper_v5_nothresh/table_fig2_dMAE_total_mort_permlog_statistics.csv",
    "plots/paper_v5_nothresh/dose_response/residual_by_mortality_AG_24m.csv",
    "plots/paper_v5_nothresh/dose_response/residual_correction_by_mortality_AG_24m.csv",
    "plots/paper_v5_nothresh/dose_response/residual_correction_trend_AG_24m.csv",
    "plots/paper_v5_nothresh/dose_response/dose_response_robustness_AG.csv",
    "plots/paper_v5_nothresh/mortality_split/mortality_block_dropcol.csv",
    # inputs of 50-53 (2026-10-21)
    "fluxnet_2017_2025_V02/fluxnet_shuttle_snapshot_20260512T115423.csv",
    "plots/paper_v5_nothresh/dose_response/dose_response_AG.csv",
    "plots/paper_v5_nothresh/dose_response/residual_by_mortality_AG_12m.csv",
    "plots/paper_v5_nothresh/dose_response/residual_correction_by_mortality_AG_12m.csv",
    "plots/paper_v5_nothresh/dose_response/residual_correction_trend_AG_12m.csv",
    "plots/paper_v5_nothresh/dose_response/residual_by_mortality_AG_efpclim_12m.csv",
    "plots/paper_v5_nothresh/dose_response/residual_by_mortality_AG_efpclim_24m.csv",
    "plots/paper_v5_nothresh/dose_response/residual_correction_by_mortality_AG_efpclim_12m.csv",
    "plots/paper_v5_nothresh/dose_response/residual_correction_by_mortality_AG_efpclim_24m.csv",
    "AutoGluon/plots/efp_v5_nothresh/ag_only/fig2_MAE_perm_statistics.csv",
    "AutoGluon/plots/efp_v5_nothresh/ag_only/fig2_high_total_mort_MAE_perm_statistics.csv",
    "plots/paper_v5_nothresh/nature/AG_24m/skill_r2_bootstrap_AG.csv",
    "plots/paper_v5_nothresh/reco_check/reco_gpp_nep_gs_by_site_year.csv",
    "plots/paper_v5_nothresh/reco_check/reco_mortality_check.csv",
]

R_ROOT = 'Sys.getenv("PAPER_ROOT", ".")'
PY_ROOT = 'os.environ.get("PAPER_ROOT", ".")'
SH_ROOT = '${PAPER_ROOT:-$(pwd)}'


def portable(text, lang):
    """Rewrite server-specific paths; lang in {'R', 'py', 'sh'}."""
    text = re.sub(r"/home/nk1125/miniconda3/envs/[A-Za-z0-9_]+/bin/(Rscript|python3?)", r"\1", text)
    text = text.replace("/home/nk1125/miniconda3/bin/python3", "python3")
    if lang == "sh":
        text = text.replace("/mnt/ssds/nk1125/AutoGluon", "${AG_WORK:-$PAPER_ROOT/AutoGluon}")
        text = text.replace("/mnt/gsdata/projects/panops/panops-data-registry/data/flux", SH_ROOT)
        return text
    # string literals that start with the project root -> root at run time + rest of the string
    rt = R_ROOT if lang == "R" else PY_ROOT
    text = re.sub(r'(["\'])/mnt/gsdata/projects/panops/panops-data-registry/data/flux/', lambda m: f"{('paste0' if lang == 'R' else '')}"
                  f"{'(' if lang == 'R' else ''}{rt}{', ' if lang == 'R' else ' + '}{m.group(1)}/", text)
    text = re.sub(r'(["\'])/mnt/gsdata/projects/panops/panops-data-registry/data/flux\1', rt, text)
    tm = 'Sys.getenv("TRAIT_MAPS_DIR", "raw/trait_maps")' if lang == "R" else 'os.environ.get("TRAIT_MAPS_DIR", "raw/trait_maps")'
    text = re.sub(r'(["\'])/mnt/gsdata/projects/panops/panops-data-registry/data/trait_maps/', lambda m: (f"paste0({tm}, " if lang == "R" else f"{tm} + ") + m.group(1) + "/", text)
    text = re.sub(r'(["\'])/home/nk1125/miniconda3/envs/[A-Za-z0-9_]+/share/proj\1', '""', text)
    if lang == "R":   # close the paste0( opened above: a paste0(root, "...") call ends where its string literal ends
        text = re.sub(r'paste0\((Sys\.getenv\("[A-Z_]+", "[^"]*"\)), "([^"]*)"', r'paste0(\1, "\2")', text)
    if lang == "py" and "os.environ" in text and not re.search(r"^import .*\bos\b|^import os", text, re.M):
        text = "import os\n" + text
    return text


def copy_code(rel):
    src, dst = os.path.join(ROOT, rel), os.path.join(PKG, rel)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if rel.endswith(".ipynb"):
        nb = json.load(open(src))
        lang = "R" if nb.get("metadata", {}).get("kernelspec", {}).get("language", "").lower() == "r" else "py"
        for c in nb["cells"]:
            if c["cell_type"] == "code":
                c["outputs"], c["execution_count"] = [], None
                c["source"] = portable("".join(c["source"]), lang).splitlines(keepends=True)
        json.dump(nb, open(dst, "w"), indent=1)
        return
    lang = {".R": "R", ".py": "py", ".sh": "sh"}[os.path.splitext(rel)[1]]
    open(dst, "w").write(portable(open(src).read(), lang))


def site_metadata():
    ds = pd.read_csv(os.path.join(PKG, DATA[0]), usecols=["SITE_ID", "YEAR"])
    n = ds.groupby("SITE_ID").agg(n_site_years=("YEAR", "size"), first_year=("YEAR", "min"), last_year=("YEAR", "max")).reset_index()
    snap = pd.read_csv(os.path.join(ROOT, "fluxnet_2017_2025_V02/fluxnet_shuttle_snapshot_20260512T115423.csv"))
    snap = snap.drop_duplicates("site_id")[["site_id", "site_name", "location_lat", "location_long", "igbp",
                                            "product_source_network", "oneflux_code_version", "product_citation"]]
    m = n.merge(snap, left_on="SITE_ID", right_on="site_id", how="left").drop(columns="site_id")
    m.to_csv(os.path.join(PKG, "site_metadata.csv"), index=False)
    return len(m), int(m.product_citation.isna().sum())


def environment():
    env = os.path.join(PKG, "environment"); os.makedirs(env, exist_ok=True)
    with open(os.path.join(env, "python_packages.txt"), "w") as fh:
        subprocess.run([PYTHON, "-m", "pip", "freeze"], stdout=fh, check=True)
    pk = ["data.table", "xgboost", "lightgbm", "nlme", "ggplot2", "ggh4x", "patchwork", "terra", "sf", "bigleaf", "writexl", "scales"]
    subprocess.run([RSCRIPT, "-e", f"pk <- c({','.join(repr(p) for p in pk)}); v <- sapply(pk, function(p) tryCatch(as.character(packageVersion(p)), error = function(e) NA));"
                    f"writeLines(c(R.version.string, paste(pk, v)), '{env}/R_packages.txt')"], check=True)


def main():
    for sub in ("code", "data", "source_data", "environment", "scripts", "AutoGluon", "notebooks", "derived_tables", "plots"):
        shutil.rmtree(os.path.join(PKG, sub), ignore_errors=True)
    for f in ("site_metadata.csv",):
        if os.path.exists(os.path.join(PKG, f)): os.remove(os.path.join(PKG, f))
    for rel in CODE: copy_code(rel)
    missing = []
    for rel in DATA:
        s = os.path.join(ROOT, rel)
        if not os.path.exists(s): missing.append(rel); continue
        os.makedirs(os.path.dirname(os.path.join(PKG, rel)), exist_ok=True); shutil.copyfile(s, os.path.join(PKG, rel))
    n, nocite = site_metadata()
    environment()
    r = subprocess.run([RSCRIPT, os.path.join(PKG, "make_source_data.R")], capture_output=True, text=True, cwd=PKG)
    print(r.stdout[-600:], r.stderr[-1500:] if r.returncode else "")
    left = subprocess.run(["grep", "-rlE", "/mnt/gsdata|/mnt/ssds|/home/nk1125", "scripts", "AutoGluon", "notebooks"],
                          capture_output=True, text=True, cwd=PKG).stdout.split()
    print(f"code files: {len(CODE)}, data files: {len(DATA) - len(missing)}, sites: {n} (without citation: {nocite})")
    print("files with server paths left:", left or "none")
    if missing: print("MISSING:", *missing, sep="\n  "); sys.exit(1)


if __name__ == "__main__":
    main()

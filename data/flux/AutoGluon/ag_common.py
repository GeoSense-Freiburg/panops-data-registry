"""Shared definitions for the AutoGluon runs on the paper_v41 datasets.

Python port of scripts/paper_v41/paper_common.R: same datasets, same predictor blocks, same
windows and the same two models, so AutoGluon sees exactly what XGB / LGB saw.
  response set  efp  -> GPPsat, NEPmax, ETmax, WUE          (106 sites / 388 site-years)
                flux -> GPP_gs, NEP_gs, ET_gs, WUE_gs       (102 sites / 362 site-years)
  windows       12m = climate lag1 + mortality current and lag1
                24m = climate lag1 + lag2 + mortality current, lag1 and lag2
  models        M3 = C+T (climate + traits), M4 = C+T+D (+ mortality / disturbance)
"""
import os
import re
import numpy as np
import pandas as pd

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
# everything the runs write (models, cache, outputs, RESULTS_*.md) goes under AG_ROOT; set AG_WORK to a local
# disk for long runs (the network drive is slow for many small files), then copy the results back
AG_ROOT = os.environ.get("AG_WORK", os.path.join(ROOT, "AutoGluon"))
SEED = 42
N_BAG = 5            # site-grouped folds inside every AutoGluon fit (as the Optuna objective)
N_REPS = 3           # repeated LOSO: reps per held-out site (as 11_train_repcv.R)
SUBSAMP = 0.8        # share of the training sites drawn in each rep
WINDOWS = ["12m", "24m"]
MODELS = {"M3": "C+T", "M4": "C+T+D"}

SETS = {
    "efp": dict(
        data=f"{ROOT}/derived_tables/paper_v41/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
        responses=["GPPsat", "NEPmax", "ETmax", "WUE"], ref=f"{ROOT}/derived_tables/paper_v41/models"),
    "efp_v5": dict(   # same EFP design on the deadtrees v5 maps (2026-10-06)
        data=f"{ROOT}/derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
        responses=["GPPsat", "NEPmax", "ETmax", "WUE"], ref=f"{ROOT}/derived_tables/paper_v5/models"),
    # without the thresholded mortality metrics (user 2026-10-07; as PAPER_NO_THRESH=1 in paper_common.R):
    # load() drops new_deadwood_gain_pp_thresh and relative_tree_loss_pct_thresh, so M4 has 193 / 338
    # predictors instead of 213 / 368; compared against the XGB / LGB runs in paper_*_nothresh
    "efp_nothresh": dict(
        data=f"{ROOT}/derived_tables/paper_v41/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
        responses=["GPPsat", "NEPmax", "ETmax", "WUE"], ref=f"{ROOT}/derived_tables/paper_v41_nothresh/models", nothresh=True),
    "efp_v5_nothresh": dict(
        data=f"{ROOT}/derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
        responses=["GPPsat", "NEPmax", "ETmax", "WUE"], ref=f"{ROOT}/derived_tables/paper_v5_nothresh/models", nothresh=True),
    # EFP-specific climate block (user 2026-10-12, after Miguel's comment on circularity): the climate variables that
    # appear in the FORMULA of an EFP are removed for that EFP only - GPPsat: light-response curve on PPFD = SW_IN * 2.11
    # -> no SW_IN; ETmax and WUE: ET = LE / lambda(TA) -> no TA; NEPmax (90th percentile of NEP): nothing removed.
    # Filters (SW_IN > 50 / 200, rain) are not in the formula and stay. Same data and design as efp_v5_nothresh.
    "efp_v5_nothresh_efpclim": dict(
        data=f"{ROOT}/derived_tables/paper_v5/paper_dataset_full_lags_monthly_tc30_minrun2_buf100-200-300-400-500.csv",
        responses=["GPPsat", "NEPmax", "ETmax", "WUE"], ref=f"{ROOT}/derived_tables/paper_v5_nothresh/models", nothresh=True,
        clim_drop={"GPPsat": ["SW_IN"], "ETmax": ["TA"], "WUE": ["TA"]}),
    "flux": dict(
        data=f"{ROOT}/derived_tables/paper_v41_fluxes/paper_dataset_fluxes.csv",
        responses=["GPP_gs", "NEP_gs", "ET_gs", "WUE_gs"], ref=f"{ROOT}/derived_tables/paper_v41_fluxes/models"),
}

TRAITS = ["LeafC", "LeafN_mass", "LeafWidth", "LeafCN_ratio", "LeafP", "StemConduitDensity",
          "StemConduitDiameter", "LeafArea_3112", "LeafArea_3113", "LeafArea_3114", "SLA", "SSD",
          "LeafThickness", "LeafN_area", "LeafDryMass", "RootingDepth", "LeafDelta15N",
          "gsmax", "P12", "P50", "P88", "rdmax", "WUE_trait"]
MORT_RE = (r"^(absolute_mortality|relative_mortality|new_deadwood_gain_pp(_thresh)?|"
           r"relative_tree_loss_pct(_thresh)?|relative_disturbance)_[0-9]+m")


def out_dir(resp_set):
    d = os.path.join(AG_ROOT, "outputs", resp_set)
    os.makedirs(d, exist_ok=True)
    return d


def load(resp_set):
    df = pd.read_csv(SETS[resp_set]["data"])
    if SETS[resp_set].get("nothresh"):
        df = df.drop(columns=[c for c in df.columns if re.search(r"_thresh_[0-9]+m(_lag[12])?$", c)])
    return df


def clim_drop(resp_set, resp):
    """Climate variables removed for this EFP (EFP-specific climate block), e.g. ["SW_IN"]; [] for the standard sets."""
    return SETS[resp_set].get("clim_drop", {}).get(resp, [])


def paper_cols(df, window, spec, drop=()):
    """Predictor columns, in the order of paper_cols() in paper_common.R. drop = climate variables to leave out
    (e.g. ["SW_IN"] removes lag*_M*_SW_IN_mean/p05/p95)."""
    ac = list(df.columns)
    clim_re = r"^lag1_M[0-9]{2}_" if window == "12m" else r"^lag[12]_M[0-9]{2}_"
    clim = [c for c in ac if re.search(clim_re, c)]
    if drop:
        clim = [c for c in clim if not re.search(r"^lag[12]_M[0-9]{2}_(" + "|".join(drop) + r")_", c)]
    mort = [c for c in ac if re.search(MORT_RE + "$", c)]
    mort += [c for c in ac if re.search(MORT_RE + "_lag1$", c)]
    if window == "24m":
        mort += [c for c in ac if re.search(MORT_RE + "_lag2$", c)]
    missing = [t for t in TRAITS if t not in ac]
    assert not missing, f"traits missing: {missing}"
    out = clim + TRAITS
    if spec == "C+T+D":
        out += mort
    return out


def paper_group(v):
    if re.search(r"^lag[12]_M[0-9]{2}_", v):
        return "Climate"
    if v in TRAITS:
        return "Traits"
    if re.search(MORT_RE, v):
        return "Disturbance"
    return "Other"


def site_folds(site_ids, seed=SEED, k=N_BAG):
    """Balanced random assignment of SITES to k folds (all years of a site share a fold)."""
    sites = np.sort(pd.unique(site_ids))
    rng = np.random.default_rng(seed)
    f = dict(zip(sites, rng.permutation(np.arange(len(sites)) % k)))
    return pd.Series(site_ids).map(f).to_numpy()


def tasks(resp_set):
    """(window, response, model) in the order of 11_train_repcv.R."""
    return [(w, r, m) for w in WINDOWS for r in SETS[resp_set]["responses"] for m in MODELS]


def fit_autogluon(train, label, path, num_cpus):
    """One AutoGluon fit: default model zoo, 5-fold bagging with site-disjoint folds, weighted ensemble.

    `train` holds the predictors, the label and a `_fold` column (site-grouped fold id). Every base
    model is therefore scored on sites it has not seen, and the ensemble weights are chosen on those
    out-of-site predictions. No stacking (388 site-years), no time limit, CPU only.
    """
    from autogluon.tabular import TabularPredictor
    return TabularPredictor(label=label, groups="_fold", problem_type="regression", eval_metric="rmse",
                            path=path, verbosity=0).fit(
        train, num_stack_levels=0, num_cpus=num_cpus, num_gpus=0,
        ag_args_ensemble=dict(fold_fitting_strategy="sequential_local"))


def ensemble_weights(predictor):
    try:
        info = predictor.model_info(predictor.model_best)
        return info["children_info"]["S1F1"]["model_weights"]
    except Exception:
        return {}


def run_pool(work, jobs, cache_file, n_workers, label, tasks_per_child=None, stall_min=60, max_restarts=30):
    """Run `work` over `jobs` in a spawn process pool that survives killed workers.

    Jobs whose cache file exists are skipped. If a worker is killed (e.g. out of memory) or nothing
    finishes for `stall_min` minutes, the pool is torn down and rebuilt for the jobs still missing.
    """
    import time
    import multiprocessing as mp
    from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
    t0 = time.time()
    for attempt in range(max_restarts + 1):
        todo = [j for j in jobs if not os.path.exists(cache_file(j))]
        print(f"{label}: {len(jobs) - len(todo)}/{len(jobs)} done, {len(todo)} to run"
              f"{'' if attempt == 0 else f' (pool restart {attempt})'}  [{(time.time() - t0) / 60:.1f} min]", flush=True)
        if not todo:
            return
        ex = ProcessPoolExecutor(max_workers=n_workers, mp_context=mp.get_context("spawn"),
                                 max_tasks_per_child=tasks_per_child)
        try:
            pending = {ex.submit(work, j) for j in todo}
            n = 0
            while pending:
                fin, pending = wait(pending, timeout=stall_min * 60, return_when=FIRST_COMPLETED)
                if not fin:
                    raise TimeoutError(f"no job finished in {stall_min} min")
                for fu in fin:
                    fu.result(); n += 1
                    if n % 50 == 0:
                        print(f"{label}: {len(jobs) - len(todo) + n}/{len(jobs)}  [{(time.time() - t0) / 60:.1f} min]", flush=True)
            ex.shutdown()
        except Exception as e:
            print(f"{label}: pool stopped ({e!r}); rebuilding", flush=True)
            for pr in list(getattr(ex, "_processes", {}).values()):
                try:
                    pr.kill()
                except Exception:
                    pass
            ex.shutdown(wait=False, cancel_futures=True)
            time.sleep(60)
    raise RuntimeError(f"{label}: gave up after {max_restarts} pool restarts")

#!/usr/bin/env python
"""Step 1 - AutoGluon on ALL site-years, one fit per window x response x model.

This is the counterpart of the Optuna step: each candidate learner is scored by 5-fold CV with
site-disjoint folds (seed 42), and AutoGluon builds a weighted ensemble on those out-of-site
predictions. The fitted predictors are kept (AutoGluon/models/<set>/) and are the ones explained
by 03_shap.py, as 12_shap_M4.R explains a model fit on all site-years.
Outputs (AutoGluon/outputs/<set>/):
  AG_leaderboard_groupCV.csv   every learner per task: grouped-CV RMSE, RMSE/sd(y), R2, ensemble weight
  AG_best_groupCV.csv          the winner per task and the best single learner
    python 01_fit_full.py <efp|flux> [n_parallel] [cpus_per_fit]
"""
import os, sys, shutil, time, warnings
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

RESP_SET = sys.argv[1]
N_PAR = int(sys.argv[2]) if len(sys.argv) > 2 else 16
N_CPU = int(sys.argv[3]) if len(sys.argv) > 3 else 6
os.environ["OMP_NUM_THREADS"] = str(N_CPU)
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
import ag_common as C


def run(task):
    win, resp, mod = task
    warnings.filterwarnings("ignore")
    df = C.load(RESP_SET)
    xv = C.paper_cols(df, win, C.MODELS[mod], C.clim_drop(RESP_SET, resp))
    d = df[xv + [resp]].copy()
    d["_fold"] = C.site_folds(df["SITE_ID"].to_numpy())
    path = os.path.join(C.AG_ROOT, "models", RESP_SET, f"{mod}_{win}_{resp}")
    shutil.rmtree(path, ignore_errors=True)
    t0 = time.time()
    p = C.fit_autogluon(d, resp, path, N_CPU)
    lb = p.leaderboard()[["model", "score_val", "fit_time", "pred_time_val"]].copy()
    y = d[resp].to_numpy()
    lb["rmse_cv"] = -lb["score_val"]
    lb["rmse_cv_over_sd"] = lb["rmse_cv"] / np.std(y, ddof=1)
    lb["r2_cv"] = 1 - lb["rmse_cv"] ** 2 * len(y) / np.sum((y - y.mean()) ** 2)
    w = C.ensemble_weights(p)
    lb["ensemble_weight"] = lb["model"].map(w).fillna(0.0)
    lb.insert(0, "n_predictors", len(xv))
    lb.insert(0, "response", resp)
    lb.insert(0, "model_id", f"{mod}_{win}")
    lb = lb.rename(columns={"model": "ag_model"}).drop(columns="score_val")
    print(f"{RESP_SET} {win} {resp:7s} {mod}: {len(xv):3d} preds, best {p.model_best} "
          f"RMSE/sd {lb['rmse_cv_over_sd'].min():.3f}  [{(time.time() - t0) / 60:.1f} min]", flush=True)
    return lb


if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=N_PAR, mp_context=mp.get_context("spawn")) as ex:
        res = list(ex.map(run, C.tasks(RESP_SET)))
    lb = pd.concat(res, ignore_index=True)
    out = C.out_dir(RESP_SET)
    lb.to_csv(os.path.join(out, "AG_leaderboard_groupCV.csv"), index=False)
    single = lb[~lb["ag_model"].str.startswith("WeightedEnsemble")]
    best = (lb.sort_values("rmse_cv").groupby(["model_id", "response"], sort=False).head(1)
            .merge(single.sort_values("rmse_cv").groupby(["model_id", "response"], sort=False).head(1)
                   [["model_id", "response", "ag_model", "rmse_cv", "r2_cv"]],
                   on=["model_id", "response"], suffixes=("", "_best_single")))
    best = best[["model_id", "response", "n_predictors", "ag_model", "rmse_cv", "rmse_cv_over_sd", "r2_cv",
                 "ag_model_best_single", "rmse_cv_best_single", "r2_cv_best_single"]]
    best.to_csv(os.path.join(out, "AG_best_groupCV.csv"), index=False)
    print(best.to_string(index=False))
    print("FIT_FULL_DONE")

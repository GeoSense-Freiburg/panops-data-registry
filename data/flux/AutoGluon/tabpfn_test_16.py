#!/usr/bin/env python
"""
TabPFN v2 screening test (2026-10-07): does TabPFN earn a place in the AutoGluon ensemble?
One AutoGluon fit per task (4 EFPs x 2 windows x C+T / C+T+M = 16) on ALL site-years of the paper dataset
(V5, no thresholds by default), with the default model zoo plus TabPFN v2 (REALTABPFN-V2), 5-fold bagging on
site-disjoint folds - the same design as 01_fit_full.py. Grouped-CV scores only (no leave-one-site-out, no SHAP).
Run on ONE GPU:  CUDA_VISIBLE_DEVICES=0 <autogluon_gpu_env>/python tabpfn_test_16.py [set=efp_v5_nothresh]
Output: AutoGluon/outputs/tabpfn_test/<set>/leaderboards.csv, summary.csv, SUMMARY.md
"""
import os, sys, time, shutil, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, torch
import ag_common as C
from autogluon.tabular import TabularPredictor
from autogluon.tabular.configs.hyperparameter_configs import get_hyperparameter_config

SET = sys.argv[1] if len(sys.argv) > 1 else "efp_v5_nothresh"
OUT = os.path.join(C.AG_ROOT, "outputs", "tabpfn_test", SET); os.makedirs(OUT, exist_ok=True)
TPFN = "RealTabPFN-v2_BAG_L1"
df = C.load(SET)
hp = get_hyperparameter_config("default"); hp["REALTABPFN-V2"] = {}
print(f"{SET}: {df.SITE_ID.nunique()} sites / {len(df)} site-years | GPU {torch.cuda.get_device_name(0)}", flush=True)
lbs, rows, t0 = [], [], time.time()
for win, resp, mod in C.tasks(SET):
    xv = C.paper_cols(df, win, C.MODELS[mod])
    d = df[xv + [resp]].copy(); d["_fold"] = C.site_folds(df["SITE_ID"].to_numpy())
    path = f"/dev/shm/tabpfn16_{os.getpid()}"; shutil.rmtree(path, ignore_errors=True)
    t = time.time()
    p = TabularPredictor(label=resp, groups="_fold", problem_type="regression", eval_metric="rmse", path=path, verbosity=0).fit(
        d, hyperparameters=hp, num_stack_levels=0, num_cpus=8, num_gpus=1, ag_args_ensemble=dict(fold_fitting_strategy="sequential_local"))
    lb = p.leaderboard()[["model", "score_val", "fit_time", "pred_time_val"]].copy()
    lb["rmse_over_sd"] = -lb.score_val / d[resp].std(ddof=1)
    w = C.ensemble_weights(p); lb["ensemble_weight"] = lb.model.map(w).fillna(0.0)
    lb.insert(0, "model_id", f"{mod}_{win}"); lb.insert(0, "response", resp); lbs.append(lb)
    single = lb[~lb.model.str.startswith("WeightedEnsemble")].sort_values("rmse_over_sd").reset_index(drop=True)
    rank = int(single.index[single.model == TPFN][0]) + 1 if TPFN in set(single.model) else np.nan
    tp = single[single.model == TPFN]
    rows.append(dict(response=resp, model_id=f"{mod}_{win}", n_predictors=len(xv),
                     best_single=single.model[0].replace("_BAG_L1", ""), best_single_rmse_sd=single.rmse_over_sd[0],
                     tabpfn_rmse_sd=float(tp.rmse_over_sd.iloc[0]) if len(tp) else np.nan, tabpfn_rank=rank, n_models=len(single),
                     tabpfn_weight=w.get(TPFN, 0.0), ensemble_rmse_sd=float(lb[lb.model == "WeightedEnsemble_L2"].rmse_over_sd.iloc[0]),
                     fit_s=time.time() - t))
    r = rows[-1]
    print(f"{resp:7s} {mod}_{win}: TabPFN rank {r['tabpfn_rank']}/{r['n_models']}, RMSE/sd {r['tabpfn_rmse_sd']:.3f} "
          f"(best {r['best_single']} {r['best_single_rmse_sd']:.3f}), weight {r['tabpfn_weight']:.2f}  [{(time.time()-t0)/60:.1f} min]", flush=True)
    shutil.rmtree(path, ignore_errors=True)
L = pd.concat(lbs, ignore_index=True); L.to_csv(f"{OUT}/leaderboards.csv", index=False)
S = pd.DataFrame(rows); S.to_csv(f"{OUT}/summary.csv", index=False)
md = [f"# TabPFN v2 screening test - {SET}", "",
      f"{df.SITE_ID.nunique()} sites / {len(df)} site-years; AutoGluon default zoo + TabPFN v2; grouped 5-fold site CV; one GPU.", "",
      f"- TabPFN gets ensemble weight > 0 in {(S.tabpfn_weight > 0).sum()} of {len(S)} tasks (mean weight {S.tabpfn_weight.mean():.2f})",
      f"- median rank among single models: {S.tabpfn_rank.median():.0f} of {S.n_models.max()}; best single model in {(S.tabpfn_rank == 1).sum()} tasks",
      f"- total time {(time.time()-t0)/60:.0f} min", "", S.round(3).to_markdown(index=False), ""]
open(f"{OUT}/SUMMARY.md", "w").write("\n".join(md)); print("\n".join(md)); print("TABPFN_TEST_DONE")

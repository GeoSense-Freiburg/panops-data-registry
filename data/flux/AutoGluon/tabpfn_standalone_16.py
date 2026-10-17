#!/usr/bin/env python
"""
TabPFN v2 on its own (outside AutoGluon), 2026-10-08: is TabPFN weak on this data, or only weak inside AutoGluon?
Same 16 tasks, same data (V5, no thresholds) and the SAME site-disjoint 5 folds as tabpfn_test_16.py / AutoGluon
(ag_common.site_folds, seed 42): out-of-fold RMSE / sd(y), directly comparable with the AutoGluon leaderboards.
Configurations: TabPFN v2 package defaults, and a larger ensemble (n_estimators = 32). Weights: the v2 regressor checkpoint.
Run on ONE GPU:  CUDA_VISIBLE_DEVICES=0 <autogluon_gpu_env>/python tabpfn_standalone_16.py [set=efp_v5_nothresh]
Output: AutoGluon/outputs/tabpfn_test/<set>/standalone.csv, STANDALONE.md
"""
import os, sys, time, warnings
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, torch
import ag_common as C
from tabpfn import TabPFNRegressor
from tabpfn.constants import ModelVersion

SET = sys.argv[1] if len(sys.argv) > 1 else "efp_v5_nothresh"
OUT = os.path.join(C.AG_ROOT, "outputs", "tabpfn_test", SET); os.makedirs(OUT, exist_ok=True)
CONFIGS = {"default": {}, "n_estimators_32": {"n_estimators": 32}}
df = C.load(SET)
folds = C.site_folds(df["SITE_ID"].to_numpy())
print(f"{SET}: {df.SITE_ID.nunique()} sites / {len(df)} site-years | {torch.cuda.get_device_name(0)}", flush=True)


def make(cfg):
    m = TabPFNRegressor.create_default_for_version(ModelVersion.V2)
    m.set_params(device="cuda", random_state=42, ignore_pretraining_limits=False, **cfg)
    return m


rows, t0 = [], time.time()
for win, resp, mod in C.tasks(SET):
    xv = C.paper_cols(df, win, C.MODELS[mod]); X = df[xv].to_numpy(np.float32); y = df[resp].to_numpy(float)
    for name, cfg in CONFIGS.items():
        oof = np.full(len(y), np.nan); t = time.time()
        for k in np.unique(folds):
            te = folds == k
            m = make(cfg).fit(X[~te], y[~te]); oof[te] = m.predict(X[te])
        rmse = np.sqrt(np.mean((oof - y) ** 2))
        rows.append(dict(response=resp, model_id=f"{mod}_{win}", config=name, rmse_over_sd=rmse / np.std(y, ddof=1), seconds=time.time() - t))
        print(f"{resp:7s} {mod}_{win} {name:16s} RMSE/sd {rows[-1]['rmse_over_sd']:.3f}  [{(time.time()-t0)/60:.1f} min]", flush=True)
S = pd.DataFrame(rows).pivot_table(index=["response", "model_id"], columns="config", values="rmse_over_sd").reset_index()
ag = pd.read_csv(f"{OUT}/summary.csv")[["response", "model_id", "tabpfn_rmse_sd", "best_single", "best_single_rmse_sd", "ensemble_rmse_sd"]]
S = S.merge(ag, on=["response", "model_id"])
S.rename(columns={"default": "standalone_default", "n_estimators_32": "standalone_32", "tabpfn_rmse_sd": "tabpfn_in_autogluon"}, inplace=True)
S["gap_to_best_single"] = S[["standalone_default", "standalone_32"]].min(axis=1) - S.best_single_rmse_sd
S.to_csv(f"{OUT}/standalone.csv", index=False)
md = [f"# TabPFN v2 standalone vs inside AutoGluon - {SET}", "",
      "Out-of-fold RMSE / sd(y) on the same site-disjoint 5 folds. Lower is better. best_single = best AutoGluon base model.", "",
      f"- standalone default: median {S.standalone_default.median():.3f}; n_estimators 32: median {S.standalone_32.median():.3f}; "
      f"inside AutoGluon: median {S.tabpfn_in_autogluon.median():.3f}; best single model: median {S.best_single_rmse_sd.median():.3f}",
      f"- standalone TabPFN beats the best AutoGluon base model in {(S.gap_to_best_single < 0).sum()} of {len(S)} tasks",
      f"- total time {(time.time()-t0)/60:.0f} min", "", S.round(3).to_markdown(index=False), ""]
open(f"{OUT}/STANDALONE.md", "w").write("\n".join(md)); print("\n".join(md)); print("STANDALONE_DONE")

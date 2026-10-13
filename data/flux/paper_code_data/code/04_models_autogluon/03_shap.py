#!/usr/bin/env python
"""Step 3 - SHAP for M4 (C+T+D), 12m and 24m, on the AutoGluon ensembles fit on all site-years.

Counterpart of 12_shap_M4.R. The AutoGluon winner is a weighted ensemble that mixes neural networks
with tree models, so TreeSHAP does not apply; SHAP values come from the model-agnostic permutation
explainer (shap.PermutationExplainer): N_PERM antithetic permutations per site-year, features
"switched off" by replacing them with their values in N_BG background site-years (seed 42). The values
are exactly additive: sum(SHAP) + base value = ensemble prediction. Averaged per site as before.
Outputs (AutoGluon/outputs/<set>/), same formats as the XGB / LGB files:
  AG_site_shap_M4.csv         variable, mean_abs_shap, model, response, test_site, group
  AG_site_signed_shap_M4.csv  group, mean_signed_shap, mean_abs_shap, model, response, test_site
  shap_values/M4_<window>_<response>.csv.gz   SHAP per site-year (SITE_ID, YEAR, base_value, prediction, <features>)
Each site is cached (AutoGluon/cache/shap/...), so an interrupted run resumes where it stopped.
    python 03_shap.py <efp|flux> [n_workers]
"""
import os, sys, time, warnings

RESP_SET = sys.argv[1]
N_WORK = int(sys.argv[2]) if len(sys.argv) > 2 else 24
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
import ag_common as C

N_PERM = int(os.environ.get("AG_SHAP_NPERM", "10"))
N_BG = int(os.environ.get("AG_SHAP_NBG", "100"))
CACHE = os.path.join(C.AG_ROOT, "cache", "shap", RESP_SET)
_S = {}


def explainer(win, resp):
    """Load the predictor once per worker and task; everything single-threaded."""
    if _S.get("key") != (win, resp):
        import torch, shap
        from autogluon.tabular import TabularPredictor
        torch.set_num_threads(1)
        df = C.load(RESP_SET)
        p = TabularPredictor.load(os.path.join(C.AG_ROOT, "models", RESP_SET, f"M4_{win}_{resp}"), verbosity=0)
        p.persist()
        for name in p.model_names():                             # forests were fit with n_jobs > 1
            try:
                for ch in p._trainer.load_model(name).models:
                    if hasattr(getattr(ch, "model", None), "n_jobs"):
                        ch.model.n_jobs = 1
            except Exception:
                pass
        xv = C.paper_cols(df, win, "C+T+D")
        assert list(p.features()) == xv
        X = df[xv].to_numpy(dtype=float)
        bg = X[np.random.default_rng(C.SEED).choice(len(X), size=min(N_BG, len(X)), replace=False)]
        import gc, ctypes
        libc = ctypes.CDLL("libc.so.6")

        def f(a):
            # the FastAI net leaves reference cycles behind on every predict call (about 1 GB per
            # permutation with 368 predictors); collect them and hand the memory back to the OS
            r = p.predict(pd.DataFrame(a, columns=xv), as_pandas=False)
            gc.collect(); libc.malloc_trim(0)
            return r
        ex = shap.PermutationExplainer(f, shap.maskers.Independent(bg, max_samples=len(bg)), seed=C.SEED)
        _S.update(key=(win, resp), df=df, xv=xv, X=X, f=f, ex=ex)
    return _S


def work(job):
    win, resp, si = job
    warnings.filterwarnings("ignore")
    out = os.path.join(CACHE, f"M4_{win}_{resp}")
    s = explainer(win, resp)
    df, xv, X = s["df"], s["xv"], s["X"]
    site = np.sort(df["SITE_ID"].unique())[si]
    f = os.path.join(out, f"{site}.csv")
    if os.path.exists(f):
        return f
    idx = np.where(df["SITE_ID"].to_numpy() == site)[0]
    sv = s["ex"](X[idx], max_evals=N_PERM * (2 * len(xv) + 1), batch_size=50, silent=True)
    pred = s["f"](X[idx])
    base = np.asarray(sv.base_values, dtype=float).reshape(-1)
    err = np.max(np.abs(sv.values.sum(1) + base - pred))
    assert err < 1e-3 * max(1.0, np.max(np.abs(pred))), f"SHAP not additive ({err})"
    o = pd.DataFrame(sv.values, columns=xv)
    o.insert(0, "prediction", pred); o.insert(0, "base_value", base)
    o.insert(0, "YEAR", df["YEAR"].to_numpy()[idx]); o.insert(0, "SITE_ID", site)
    os.makedirs(out, exist_ok=True)
    o.to_csv(f + ".tmp", index=False); os.replace(f + ".tmp", f)
    return f


if __name__ == "__main__":
    df = C.load(RESP_SET)
    n_sites = df["SITE_ID"].nunique()
    tasks = [(w, r) for w in C.WINDOWS for r in C.SETS[RESP_SET]["responses"]]
    jobs = [(w, r, si) for (w, r) in tasks for si in range(n_sites)]
    sites = np.sort(df["SITE_ID"].unique())
    C.run_pool(work, jobs, lambda j: os.path.join(CACHE, f"M4_{j[0]}_{j[1]}", f"{sites[j[2]]}.csv"),
               N_WORK, f"{RESP_SET}: sites")
    out = C.out_dir(RESP_SET)
    os.makedirs(os.path.join(out, "shap_values"), exist_ok=True)
    per_var, per_grp = [], []
    for win, resp in tasks:
        d = os.path.join(CACHE, f"M4_{win}_{resp}")
        sv = pd.concat([pd.read_csv(os.path.join(d, f)) for f in sorted(os.listdir(d)) if f.endswith(".csv")],
                       ignore_index=True)
        sv.to_csv(os.path.join(out, "shap_values", f"M4_{win}_{resp}.csv.gz"), index=False)
        xv = [c for c in sv.columns if c not in ("SITE_ID", "YEAR", "base_value", "prediction")]
        grp = np.array([C.paper_group(v) for v in xv])
        for site, s in sv.groupby("SITE_ID", sort=False):
            m = s[xv].to_numpy()
            per_var.append(pd.DataFrame(dict(variable=xv, mean_abs_shap=np.abs(m).mean(0), model=f"M4_{win}",
                                             response=resp, test_site=site, group=grp)))
            for g in pd.unique(grp):
                per_grp.append(dict(group=g, mean_signed_shap=m[:, grp == g].sum(1).mean(),
                                    mean_abs_shap=np.abs(m[:, grp == g]).sum(1).mean(),
                                    model=f"M4_{win}", response=resp, test_site=site))
    pd.concat(per_var, ignore_index=True).to_csv(os.path.join(out, "AG_site_shap_M4.csv"), index=False)
    pd.DataFrame(per_grp).to_csv(os.path.join(out, "AG_site_signed_shap_M4.csv"), index=False)
    print("SHAP_DONE", flush=True)
    os._exit(0)                      # abandoned pools from restarts can block a normal exit

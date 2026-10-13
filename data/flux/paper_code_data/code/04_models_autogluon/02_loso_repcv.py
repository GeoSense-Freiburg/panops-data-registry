#!/usr/bin/env python
"""Step 2 - repeated leave-one-site-out CV with AutoGluon (the scheme of 11_train_repcv.R).

For every held-out site: 3 reps, each on a random 80 % of the remaining sites (seed 42 + 1000 r + site
index), predictions averaged over reps. Inside every rep AutoGluon runs from scratch on the training
sites only (site-grouped 5-fold bagging + weighted ensemble), so model selection never sees the
held-out site. Besides the ensemble, the held-out predictions of every base learner are kept, which
gives a like-for-like LOSO score per learner family.
Outputs (AutoGluon/outputs/<set>/), same columns as the XGB / LGB files:
  AG_predictions_repCV.csv, AG_metrics_repCV.csv                    the AutoGluon ensemble
  AG_predictions_repCV_by_model.csv, AG_metrics_repCV_by_model.csv  every base learner
Each site is cached (AutoGluon/cache/loso/...), so an interrupted run resumes where it stopped.
    python 02_loso_repcv.py <efp|flux> [n_workers] [cpus_per_fit]
"""
import os, sys, shutil, time, uuid, warnings

RESP_SET = sys.argv[1]
N_WORK = int(sys.argv[2]) if len(sys.argv) > 2 else 48
N_CPU = int(sys.argv[3]) if len(sys.argv) > 3 else 2
os.environ["OMP_NUM_THREADS"] = str(N_CPU)
warnings.filterwarnings("ignore")
import numpy as np, pandas as pd
import ag_common as C

CACHE = os.path.join(C.AG_ROOT, "cache", "loso", RESP_SET)
TMP = "/dev/shm/ag_loso"
ENS = "AutoGluon"
_DF = None


def work(job):
    global _DF
    win, resp, mod, si = job
    warnings.filterwarnings("ignore")
    if _DF is None:
        _DF = C.load(RESP_SET)
    df = _DF
    sites = np.sort(df["SITE_ID"].unique())
    ts = sites[si]
    f = os.path.join(CACHE, f"{mod}_{win}_{resp}", f"{ts}.csv")
    if os.path.exists(f):
        return f
    xv = C.paper_cols(df, win, C.MODELS[mod], C.clim_drop(RESP_SET, resp))
    te = df[df["SITE_ID"] == ts]
    tr_sites = sites[sites != ts]
    k = max(5, int(np.floor(C.SUBSAMP * len(tr_sites))))
    rows, wts = [], []
    for r in range(1, C.N_REPS + 1):
        rng = np.random.default_rng(C.SEED + r * 1000 + si + 1)
        keep = rng.choice(tr_sites, size=k, replace=False)
        tr = df[df["SITE_ID"].isin(keep)]
        d = tr[xv + [resp]].copy()
        d["_fold"] = C.site_folds(tr["SITE_ID"].to_numpy(), seed=C.SEED + r)
        path = os.path.join(TMP, f"{os.getpid()}_{uuid.uuid4().hex}")
        try:
            p = C.fit_autogluon(d, resp, path, N_CPU)
            pm = p.predict_multi(te[xv])
            pm[ENS] = pm[p.model_best]                           # what AutoGluon itself would deploy
            for m, v in pm.items():
                rows.append(pd.DataFrame(dict(
                    SITE_ID=ts, YEAR=te["YEAR"].to_numpy(), observed=te[resp].to_numpy(), rep=r,
                    ag_model=m, predicted=np.asarray(v, dtype=float))))
            wts.append(dict(rep=r, selected=p.model_best, **C.ensemble_weights(p)))
        except Exception as e:                                   # a failed rep is skipped, as in the R script
            print(f"FAILED {win} {resp} {mod} {ts} rep {r}: {e!r}", flush=True)
        finally:
            shutil.rmtree(path, ignore_errors=True)
    if not rows:
        return None
    os.makedirs(os.path.dirname(f), exist_ok=True)
    pd.DataFrame(wts).assign(SITE_ID=ts).to_json(f[:-4] + "_weights.json", orient="records")
    pd.concat(rows, ignore_index=True).to_csv(f + ".tmp", index=False)
    os.replace(f + ".tmp", f)
    return f


def collect(win, resp, mod):
    d = os.path.join(CACHE, f"{mod}_{win}_{resp}")
    x = pd.concat([pd.read_csv(os.path.join(d, f)) for f in sorted(os.listdir(d)) if f.endswith(".csv")])
    g = x.groupby(["ag_model", "SITE_ID", "YEAR"], sort=False)
    a = g.agg(observed=("observed", "first"), predicted=("predicted", "mean"),
              pred_sd=("predicted", lambda v: v.std(ddof=1)), n_reps_used=("predicted", "size")).reset_index()
    a.insert(0, "response", resp)
    a.insert(0, "model", f"{mod}_{win}")
    return a


def metrics(a, n_pred):
    out = []
    for (am, mo, rs), s in a.groupby(["ag_model", "model", "response"], sort=False):
        ob, pr = s["observed"].to_numpy(), s["predicted"].to_numpy()
        out.append(dict(learner="AG", ag_model=am, model=mo, response=rs, n_predictors=n_pred[(mo, rs)],
                        n_pairs=len(s), n_sites=s["SITE_ID"].nunique(), RMSE=np.sqrt(np.mean((pr - ob) ** 2)),
                        MAE=np.mean(np.abs(pr - ob)), R2=1 - np.sum((ob - pr) ** 2) / np.sum((ob - ob.mean()) ** 2)))
    return pd.DataFrame(out)


if __name__ == "__main__":
    df = C.load(RESP_SET)
    n_sites = df["SITE_ID"].nunique()
    tasks = C.tasks(RESP_SET)
    jobs = [(w, r, m, si) for (w, r, m) in tasks for si in range(n_sites)]
    os.makedirs(TMP, exist_ok=True)
    sites = np.sort(df["SITE_ID"].unique())
    C.run_pool(work, jobs, lambda j: os.path.join(CACHE, f"{j[2]}_{j[0]}_{j[1]}", f"{sites[j[3]]}.csv"),
               N_WORK, f"{RESP_SET}: site folds")   # no max_tasks_per_child: python 3.11 deadlocks when all workers retire at once
    allp = pd.concat([collect(*t) for t in tasks], ignore_index=True)
    n_pred = {(f"{m}_{w}", r): len(C.paper_cols(df, w, C.MODELS[m], C.clim_drop(RESP_SET, r))) for (w, r, m) in tasks}
    out = C.out_dir(RESP_SET)
    allp.to_csv(os.path.join(out, "AG_predictions_repCV_by_model.csv"), index=False)
    mm = metrics(allp, n_pred)
    mm.to_csv(os.path.join(out, "AG_metrics_repCV_by_model.csv"), index=False)
    ens = allp[allp["ag_model"] == ENS].drop(columns="ag_model")
    ens.to_csv(os.path.join(out, "AG_predictions_repCV.csv"), index=False)
    me = mm[mm["ag_model"] == ENS].drop(columns="ag_model")
    me.to_csv(os.path.join(out, "AG_metrics_repCV.csv"), index=False)
    print(me.to_string(index=False))
    print("LOSO_DONE", flush=True)
    os._exit(0)                      # abandoned pools from restarts can block a normal exit

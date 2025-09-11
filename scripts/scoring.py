# scripts/scoring.py
# Compute 3/5-day covering scores from history (ASIC).

import os, glob
import pandas as pd

def _load_last_n_history(n=5, path="data/history/asic"):
    files = sorted(glob.glob(os.path.join(path, "*.csv")))[-n:]
    frames = []
    for p in files:
        try:
            frames.append(pd.read_csv(p))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    for c in ("DeltaShares_num", "Delta_pp_num", "PctShort_pp_num"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def covering_scores(window=3, path="data/history/asic"):
    df = _load_last_n_history(window, path)
    if df.empty or "Code" not in df.columns:
        return pd.DataFrame(columns=["Code","CovNegShares","CovNegPP","NegDays","CoverScore"])
    df = df.groupby("Code").agg({
        "DeltaShares_num": lambda s: float(s.clip(upper=0).sum() if s.notna().any() else 0.0),
        "Delta_pp_num":    lambda s: float(s.clip(upper=0).sum() if s.notna().any() else 0.0),
        "PctShort_pp_num": "last"
    }).reset_index()
    df.rename(columns={"DeltaShares_num":"CovNegShares","Delta_pp_num":"CovNegPP","PctShort_pp_num":"PctShort_pp"}, inplace=True)

    # Neg day count (approximate: count negatives across the window by re-loading with marks)
    hist = _load_last_n_history(window, path)
    if not hist.empty and "Code" in hist.columns:
        negdays = hist.assign(Neg=(hist["DeltaShares_num"] < 0) | (hist["Delta_pp_num"] < 0)).groupby("Code")["Neg"].sum().rename("NegDays")
        df = df.merge(negdays, on="Code", how="left").fillna({"NegDays":0})
    else:
        df["NegDays"] = 0

    # Combine: more negative sums and more negative days => higher score
    # Use absolute magnitude for sums
    df["CoverScore"] = (df["NegDays"] * 1.0) + (df["CovNegPP"].abs() * 0.5) + (df["CovNegShares"].abs() / 200000.0)
    return df.sort_values(["CoverScore","CovNegShares","CovNegPP"], ascending=[False, False, False])

# scripts/history.py
import os, json, glob
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DIR_DOCS = "docs"
DIR_API  = os.path.join(DIR_DOCS, "api")
DIR_API_DAILY = os.path.join(DIR_API, "daily")
DIR_CHARTS = os.path.join(DIR_DOCS, "charts")
DIR_HIST  = "data/history"
DIR_HIST_GROSS = os.path.join(DIR_HIST, "gross")
DIR_HIST_ASIC  = os.path.join(DIR_HIST, "asic")

def ensure_dirs():
    for d in [DIR_DOCS, DIR_API, DIR_API_DAILY, DIR_CHARTS, DIR_HIST_GROSS, DIR_HIST_ASIC]:
        os.makedirs(d, exist_ok=True)

def _to_records(df, cols):
    if df is None or df.empty: return []
    keep = [c for c in cols if c in df.columns]
    return df[keep].to_dict(orient="records")

def export_api(gross_sig, pos_sig, gross_date, asic_date):
    ensure_dirs()
    gross_cols = ["Code", "Sector", "Gross_num", "PctGrossVsIssuedPct_num", "Date"]
    pos_cols   = ["Code", "Sector", "PctShort_pp_num", "Delta_pp_num", "DeltaShares_num", "DaysToCover", "ADV", "Date"]
    payload = {
        "meta": {"gross_date": str(gross_date), "asic_date": str(asic_date)},
        "gross": _to_records(gross_sig, gross_cols),
        "asic":  _to_records(pos_sig,   pos_cols),
    }
    with open(os.path.join(DIR_API, "latest.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DIR_API_DAILY, f"{gross_date}.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def append_history(gross_sig, pos_sig, gross_date, asic_date):
    ensure_dirs()
    if gross_sig is not None and not gross_sig.empty:
        gcols = [c for c in ["Date","Code","Gross_num","PctGrossVsIssuedPct_num"] if c in gross_sig.columns]
        gd = gross_sig[gcols].copy(); gd["Date"] = str(gross_date)
        gd.to_csv(os.path.join(DIR_HIST_GROSS, f"{gross_date}.csv"), index=False)

    if pos_sig is not None and not pos_sig.empty:
        pcols = [c for c in ["Date","Code","PctShort_pp_num","Delta_pp_num","DeltaShares_num","DaysToCover","ADV"] if c in pos_sig.columns]
        pdx = pos_sig[pcols].copy(); pdx["Date"] = str(asic_date)
        pdx.to_csv(os.path.join(DIR_HIST_ASIC, f"{asic_date}.csv"), index=False)

def _concat_hist(pattern):
    files = sorted(glob.glob(pattern))
    if not files: return pd.DataFrame()
    frames = [pd.read_csv(p) for p in files]
    out = pd.concat(frames, ignore_index=True)
    dedup_cols = [c for c in ["Date","Code"] if c in out.columns]
    return out.drop_duplicates(subset=dedup_cols + [c for c in out.columns if c not in dedup_cols], keep="last")

def build_charts():
    ensure_dirs()
    charts = []

    # Gross: top 5 by 30d cumulative
    gh = _concat_hist(os.path.join(DIR_HIST_GROSS, "*.csv"))
    if not gh.empty and "Gross_num" in gh.columns:
        gh["Date"] = pd.to_datetime(gh["Date"])
        cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=30)
        gh30 = gh[gh["Date"] >= cutoff]
        if not gh30.empty:
            top5 = gh30.groupby("Code")["Gross_num"].sum().nlargest(5).index.tolist()
            plot = gh30[gh30["Code"].isin(top5)].pivot_table(index="Date", columns="Code", values="Gross_num", aggfunc="sum").fillna(0)
            if not plot.empty:
                plt.figure(figsize=(8,4.5)); plot.plot(); plt.title("Gross short sales – top 5 (30d)"); plt.ylabel("Shares"); plt.xlabel("Date"); plt.tight_layout()
                p = os.path.join(DIR_CHARTS, "gross_top5_30d.png"); plt.savefig(p); plt.close()
                charts.append("charts/gross_top5_30d.png")

    # ASIC % short leaders (60d)
    ah = _concat_hist(os.path.join(DIR_HIST_ASIC, "*.csv"))
    if not ah.empty and "PctShort_pp_num" in ah.columns:
        ah["Date"] = pd.to_datetime(ah["Date"])
        cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=60)
        ah60 = ah[ah["Date"] >= cutoff]
        if not ah60.empty:
            latest = ah60.sort_values("Date").dropna(subset=["PctShort_pp_num"])
            latest_codes = (latest.groupby("Code").last()["PctShort_pp_num"].nlargest(5).index.tolist())
            plot = ah60[ah60["Code"].isin(latest_codes)].pivot_table(index="Date", columns="Code", values="PctShort_pp_num", aggfunc="last")
            if not plot.empty:
                plt.figure(figsize=(8,4.5)); plot.plot(); plt.title("% short on issue – leaders (60d)"); plt.ylabel("Percent"); plt.xlabel("Date"); plt.tight_layout()
                p = os.path.join(DIR_CHARTS, "asic_pctshort_top5_60d.png"); plt.savefig(p); plt.close()
                charts.append("charts/asic_pctshort_top5_60d.png")

    return charts

def update_history_and_charts(gross_sig, pos_sig, gross_date, asic_date):
    if gross_sig is not None and not gross_sig.empty:
        if "Gross_num" not in gross_sig.columns:
            gross_sig["Gross_num"] = pd.to_numeric(gross_sig.get("Gross"), errors="coerce").fillna(0.0)
        if "PctGrossVsIssuedPct_num" not in gross_sig.columns:
            gross_sig["PctGrossVsIssuedPct_num"] = pd.to_numeric(gross_sig.get("PctGrossVsIssuedPct"), errors="coerce").fillna(0.0)

    if pos_sig is not None and not pos_sig.empty:
        pos_sig["PctShort_pp_num"] = pd.to_numeric(pos_sig.get("PctShort_pp"), errors="coerce").fillna(0.0)
        pos_sig["Delta_pp_num"]    = pd.to_numeric(pos_sig.get("Delta_pp"),    errors="coerce").fillna(0.0)
        pos_sig["DeltaShares_num"] = pd.to_numeric(pos_sig.get("DeltaShares"), errors="coerce").fillna(0.0)

    export_api(gross_sig if gross_sig is not None else pd.DataFrame(),
               pos_sig if pos_sig is not None else pd.DataFrame(),
               gross_date, asic_date)
    append_history(gross_sig, pos_sig, gross_date, asic_date)
    return build_charts()

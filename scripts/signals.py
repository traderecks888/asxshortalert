# scripts/signals.py
import pandas as pd

_GROSS_BASE_COLS = ["Code", "Gross", "Issued", "PctGrossVsIssuedPct", "Date"]
_POS_BASE_COLS   = ["Code", "ReportedShort", "Issued", "PctShort", "Date"]

def _empty_with(cols):
    return pd.DataFrame(columns=cols)

def compute_short_position_signals(df_today, df_prev=None, cfg=None):
    """ASIC short positions (T+4) with covering flags; DTC computed in pipeline."""
    cfg = cfg or {}
    t = cfg.get("short_positions", {})
    pct_ge   = float(t.get("pct_short_ge", 5.0))
    dpp_ge   = float(t.get("pct_short_change_ge", 0.50))
    dshr_ge  = float(t.get("abs_shares_cover_ge", 200_000))

    if df_today is None or len(df_today) == 0 or "PctShort" not in df_today.columns:
        out = _empty_with(_POS_BASE_COLS + [
            "PctShort_pp","PctShort_prev","Delta_pp",
            "ShortedShares","PrevShortedShares","DeltaShares",
            "PctShort_pp_num","Delta_pp_num","DeltaShares_num",
            "FLAG_high_pct","FLAG_delta","FLAG_cover_pp","FLAG_cover_shares","CoverScore"
        ])
        return out

    out = df_today.copy()
    out["PctShort"] = pd.to_numeric(out["PctShort"], errors="coerce")
    out["Issued"]   = pd.to_numeric(out.get("Issued"), errors="coerce")

    maxv = out["PctShort"].max()
    out["PctShort_pp"] = (out["PctShort"] * 100.0) if (pd.notna(maxv) and maxv <= 1) else out["PctShort"]

    if df_prev is not None and {"Code","PctShort"}.issubset(df_prev.columns):
        prev = df_prev[["Code","PctShort","Issued"]].rename(columns={"PctShort":"PctShort_prev","Issued":"Issued_prev"})
        prev["PctShort_prev"] = pd.to_numeric(prev["PctShort_prev"], errors="coerce")
        prev["Issued_prev"]   = pd.to_numeric(prev.get("Issued_prev"), errors="coerce")
        out = out.merge(prev, on="Code", how="left")
        if pd.notna(maxv) and maxv <= 1:
            out["Delta_pp"] = (out["PctShort"] - out["PctShort_prev"]) * 100.0
        else:
            out["Delta_pp"] = (out["PctShort"] - out["PctShort_prev"])
    else:
        out["PctShort_prev"] = pd.NA
        out["Issued_prev"]   = pd.NA
        out["Delta_pp"]      = pd.NA

    out["ShortedShares"]      = (out["PctShort_pp"] / 100.0) * out["Issued"]
    out["PrevShortedShares"]  = (pd.to_numeric(out.get("PctShort_prev"), errors="coerce") / 100.0) * out.get("Issued_prev")
    out["DeltaShares"]        = out["ShortedShares"] - out["PrevShortedShares"]

    out["PctShort_pp_num"] = pd.to_numeric(out["PctShort_pp"], errors="coerce").fillna(0.0)
    out["Delta_pp_num"]    = pd.to_numeric(out["Delta_pp"],    errors="coerce").fillna(0.0)
    out["DeltaShares_num"] = pd.to_numeric(out["DeltaShares"], errors="coerce").fillna(0.0)

    out["FLAG_high_pct"] = out["PctShort_pp_num"] >= pct_ge
    out["FLAG_delta"]    = out["Delta_pp_num"]    >= dpp_ge

    out["FLAG_cover_pp"]     = out["Delta_pp_num"]    <= (-dpp_ge)
    out["FLAG_cover_shares"] = out["DeltaShares_num"] <= (-dshr_ge)
    out["CoverScore"]        = out[["FLAG_cover_pp","FLAG_cover_shares"]].sum(axis=1, min_count=1)

    return out.sort_values(["FLAG_high_pct","FLAG_delta","PctShort_pp_num"], ascending=[False, False, False])

def compute_gross_shorts_signals(df_asx_cboe, cfg=None):
    cfg = cfg or {}
    t = cfg.get("gross_shorts", {})
    if (df_asx_cboe is None) or (len(df_asx_cboe) == 0) or ("Gross" not in df_asx_cboe.columns):
        out = _empty_with(_GROSS_BASE_COLS + ["FLAG_big_qty","FLAG_big_pct","Score","Gross_num","PctGrossVsIssuedPct_num"])
        return out

    out = df_asx_cboe.copy()
    out["Gross"] = pd.to_numeric(out.get("Gross"), errors="coerce")
    out["Issued"] = pd.to_numeric(out.get("Issued"), errors="coerce")
    out["PctGrossVsIssuedPct"] = pd.to_numeric(out.get("PctGrossVsIssuedPct"), errors="coerce").fillna(0.0)
    out["Gross_num"] = out["Gross"].fillna(0.0)
    out["PctGrossVsIssuedPct_num"] = out["PctGrossVsIssuedPct"].fillna(0.0)

    out["FLAG_big_qty"] = out["Gross_num"] >= float(t.get("absolute_qty_ge", 200_000))
    out["FLAG_big_pct"] = out["PctGrossVsIssuedPct_num"] >= float(t.get("as_percent_issued_ge", 0.10))
    out["Score"] = out[["FLAG_big_qty","FLAG_big_pct"]].sum(axis=1, min_count=1)

    return out.sort_values(["Score","Gross_num","PctGrossVsIssuedPct_num"], ascending=[False,False,False])

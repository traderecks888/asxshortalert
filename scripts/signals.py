# scripts/signals.py
import pandas as pd

_GROSS_BASE_COLS = ["Code", "Gross", "Issued", "PctGrossVsIssued", "Date"]
_POS_BASE_COLS   = ["Code", "ReportedShort", "Issued", "PctShort", "Date"]

def _empty_with(cols):
    return pd.DataFrame(columns=cols)

def compute_short_position_signals(df_today, df_prev=None, cfg=None):
    """Defensive: tolerates empty inputs, coerces numerics, avoids FutureWarnings."""
    cfg = cfg or {}
    t = cfg.get("short_positions", {})

    if df_today is None or len(df_today) == 0 or "PctShort" not in df_today.columns:
        out = _empty_with(_POS_BASE_COLS)
        out["PctShort_pp"]   = pd.Series(dtype="float64")
        out["PctShort_prev"] = pd.Series(dtype="float64")
        out["Delta_pp"]      = pd.Series(dtype="float64")
        out["FLAG_high_pct"] = pd.Series(dtype="bool")
        out["FLAG_delta"]    = pd.Series(dtype="bool")
        return out

    out = df_today.copy()
    out["PctShort"] = pd.to_numeric(out["PctShort"], errors="coerce")

    maxv = out["PctShort"].max()
    out["PctShort_pp"] = (out["PctShort"] * 100.0) if (pd.notna(maxv) and maxv <= 1) else out["PctShort"]

    if df_prev is not None and {"Code","PctShort"} <= set(df_prev.columns):
        prev = df_prev[["Code","PctShort"]].rename(columns={"PctShort":"PctShort_prev"})
        prev["PctShort_prev"] = pd.to_numeric(prev["PctShort_prev"], errors="coerce")
        out = out.merge(prev, on="Code", how="left")
        out["Delta_pp"] = (out["PctShort"] - out["PctShort_prev"]) * 100.0
    else:
        out["PctShort_prev"] = pd.NA
        out["Delta_pp"] = pd.NA

    # Numeric views for comparisons (prevents FutureWarning on fillna)
    out["PctShort_pp_num"] = pd.to_numeric(out["PctShort_pp"], errors="coerce").fillna(0.0)
    out["Delta_pp_num"]    = pd.to_numeric(out["Delta_pp"],    errors="coerce").fillna(0.0)

    out["FLAG_high_pct"] = out["PctShort_pp_num"] >= float(t.get("pct_short_ge", 5.0))
    out["FLAG_delta"]    = out["Delta_pp_num"]    >= float(t.get("pct_short_change_ge", 0.50))

    return out.sort_values(["FLAG_high_pct","FLAG_delta","PctShort_pp_num"], ascending=[False,False,False])

def compute_gross_shorts_signals(df_asx_cboe, cfg=None):
    """Defensive: tolerates empty schema changes; coerces numerics; stable sort."""
    cfg = cfg or {}
    t = cfg.get("gross_shorts", {})

    if (df_asx_cboe is None) or (len(df_asx_cboe) == 0) or ("Gross" not in df_asx_cboe.columns):
        out = _empty_with(_GROSS_BASE_COLS)
        out["FLAG_big_qty"] = pd.Series(dtype="bool")
        out["FLAG_big_pct"] = pd.Series(dtype="bool")
        out["Score"]        = pd.Series(dtype="float64")
        return out

    out = df_asx_cboe.copy()
    for c in ("Gross","Issued","PctGrossVsIssued"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out["Gross_num"]             = pd.to_numeric(out.get("Gross"), errors="coerce").fillna(0.0)
    out["PctGrossVsIssued_num"]  = pd.to_numeric(out.get("PctGrossVsIssued"), errors="coerce").fillna(0.0)

    out["FLAG_big_qty"] = out["Gross_num"] >= float(t.get("absolute_qty_ge", 200_000))
    out["FLAG_big_pct"] = (out["PctGrossVsIssued_num"]*100.0) >= float(t.get("as_percent_issued_ge", 0.10))

    out["Score"] = out[["FLAG_big_qty","FLAG_big_pct"]].sum(axis=1, min_count=1)

    return out.sort_values(["Score","Gross_num","PctGrossVsIssued_num"], ascending=[False,False,False])

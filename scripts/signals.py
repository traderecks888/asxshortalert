# scripts/signals.py
import pandas as pd

_GROSS_BASE_COLS = ["Code", "Gross", "Issued", "PctGrossVsIssued", "Date"]
_POS_BASE_COLS   = ["Code", "ReportedShort", "Issued", "PctShort", "Date"]

def _empty_with(cols):
    return pd.DataFrame(columns=cols)

def compute_short_position_signals(df_today, df_prev=None, cfg=None):
    """Robust to empty inputs and missing columns."""
    cfg = cfg or {}
    t = cfg.get("short_positions", {})
    # If empty or missing expected cols, return shaped empty
    if df_today is None or len(df_today) == 0 or "PctShort" not in df_today.columns:
        out = _empty_with(_POS_BASE_COLS + ["PctShort_pp", "PctShort_prev", "Delta_pp",
                                            "FLAG_high_pct", "FLAG_delta"])
        return out

    out = df_today.copy()

    # Normalize % short to percentage points (pp)
    # ASIC sometimes publishes as fraction 0.xx; detect and scale.
    try:
        maxv = pd.to_numeric(out["PctShort"], errors="coerce").max()
    except Exception:
        maxv = None
    out["PctShort"] = pd.to_numeric(out["PctShort"], errors="coerce")
    out["PctShort_pp"] = (out["PctShort"] * 100) if (maxv is not None and maxv <= 1) else out["PctShort"]

    # Day-over-day delta (if previous available)
    if df_prev is not None and "PctShort" in df_prev.columns and "Code" in df_prev.columns:
        prev = df_prev[["Code", "PctShort"]].rename(columns={"PctShort": "PctShort_prev"})
        out = out.merge(prev, on="Code", how="left")
        out["Delta_pp"] = (out["PctShort"] - out["PctShort_prev"]) * 100
    else:
        out["PctShort_prev"] = pd.NA
        out["Delta_pp"] = pd.NA

    out["FLAG_high_pct"] = out["PctShort_pp"] >= t.get("pct_short_ge", 5.0)
    out["FLAG_delta"]    = out["Delta_pp"].fillna(0) >= t.get("pct_short_change_ge", 0.50)

    return out.sort_values(
        ["FLAG_high_pct", "FLAG_delta", "PctShort_pp"],
        ascending=[False, False, False]
    )

def compute_gross_shorts_signals(df_asx_cboe, cfg=None):
    """Robust to empty inputs and missing columns."""
    cfg = cfg or {}
    t = cfg.get("gross_shorts", {})

    # If empty or missing required columns, return shaped empty
    if (df_asx_cboe is None) or (len(df_asx_cboe) == 0) or ("Gross" not in df_asx_cboe.columns):
        return _empty_with(_GROSS_BASE_COLS + ["FLAG_big_qty", "FLAG_big_pct", "Score"])

    out = df_asx_cboe.copy()

    # Coerce numerics; tolerate strings/NaNs
    for c in ("Gross", "Issued", "PctGrossVsIssued"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out["FLAG_big_qty"] = out["Gross"].fillna(0) >= t.get("absolute_qty_ge", 200_000)
    out["FLAG_big_pct"] = (out["PctGrossVsIssued"].fillna(0) * 100) >= t.get("as_percent_issued_ge", 0.10)
    # min_count=1 avoids NaN when both flags are NaN (older pandas compat)
    out["Score"] = out[["FLAG_big_qty", "FLAG_big_pct"]].sum(axis=1, min_count=1)

    return out.sort_values(
        ["Score", "Gross", "PctGrossVsIssued"],
        ascending=[False, False, False]
    )

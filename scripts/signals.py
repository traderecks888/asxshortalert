import pandas as pd

def compute_short_position_signals(df_today, df_prev=None, cfg=None):
    out = df_today.copy()
    out["PctShort_pp"] = out["PctShort"]*100 if out["PctShort"].max()<=1 else out["PctShort"]
    if df_prev is not None:
        prev = df_prev[["Code","PctShort"]].rename(columns={"PctShort":"PctShort_prev"})
        out = out.merge(prev, on="Code", how="left")
        out["Delta_pp"] = (out["PctShort"] - out["PctShort_prev"]) * 100
    t = (cfg or {}).get("short_positions", {})
    out["FLAG_high_pct"] = out["PctShort_pp"] >= t.get("pct_short_ge", 5.0)
    out["FLAG_delta"]    = out["Delta_pp"].fillna(0) >= t.get("pct_short_change_ge", 0.5)
    return out.sort_values(["FLAG_high_pct","FLAG_delta","PctShort_pp"], ascending=[False,False,False])

def compute_gross_shorts_signals(df_asx_cboe, cfg=None):
    out = df_asx_cboe.copy()
    t = (cfg or {}).get("gross_shorts", {})
    out["FLAG_big_qty"]  = out["Gross"].fillna(0) >= t.get("absolute_qty_ge", 200_000)
    out["FLAG_big_pct"]  = (out["PctGrossVsIssued"].fillna(0)*100) >= t.get("as_percent_issued_ge", 0.10)
    out["Score"] = out[["FLAG_big_qty","FLAG_big_pct"]].sum(axis=1)
    return out.sort_values(["Score","Gross","PctGrossVsIssued"], ascending=[False,False,False])

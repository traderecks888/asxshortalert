# scripts/sectors_static.py

"""
Static sector mapping loader/attacher.
- Reads config/sectors_static.csv (columns: Code,Sector)
- Attaches Sector for any DataFrame with a 'Code' column
- Logs unknown codes to data/sectors_unknown_today.csv for quarterly updates
No network calls. Safe for private repos and reproducibility.
"""
from __future__ import annotations
import os
import pandas as pd
from datetime import datetime, timezone, timedelta

AWST_OFFSET_HOURS = 8
UTC = timezone.utc

def today_awst_date():
    return (datetime.now(UTC) + timedelta(hours=AWST_OFFSET_HOURS)).date()

def load_static_map(path: str = "config/sectors_static.csv") -> dict:
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path)
        if "Code" in df.columns and "Sector" in df.columns:
            return {str(c).strip().upper(): str(s).strip() for c, s in zip(df["Code"], df["Sector"]) if str(c).strip() and str(s).strip()}
    except Exception:
        pass
    return {}

def attach_sectors_static(df: pd.DataFrame, static_map: dict, log_path: str | None = None) -> pd.DataFrame:
    if df is None or df.empty or "Code" not in df.columns:
        return df
    out = df.copy()
    codes = out["Code"].astype(str).str.upper()
    out["Sector"] = codes.map(static_map).fillna("Unknown")
    if log_path is not None:
        missing = sorted(set(codes[codes.map(lambda x: x not in static_map)].tolist()))
        if missing:
            os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
            pd.DataFrame({"Date":[str(today_awst_date())]*len(missing), "Code": missing}).to_csv(log_path, index=False)
    return out

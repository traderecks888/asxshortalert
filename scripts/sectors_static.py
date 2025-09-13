# scripts/sectors_static.py
"""
Static sector mapping loader/attacher with normalization.
- Reads config/sectors_static.csv (Code,Sector)
- Normalizes any sector/industry-group labels to the 11 GICS sectors
- Attaches Sector to dataframes by 'Code'
- Logs unknown codes to data/sectors_unknown_today.csv

Drop-in replacement for your current scripts/sectors_static.py
"""

from __future__ import annotations
import os
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict

UTC = timezone.utc
AWST_OFFSET_HOURS = 8

# Canonical 11 sectors
GICS_11 = {
    "Energy","Materials","Industrials","Consumer Discretionary","Consumer Staples",
    "Health Care","Financials","Information Technology","Communication Services",
    "Utilities","Real Estate"
}

# Substring-based mapping from messy labels (including Industry Groups) -> 11 sectors
def _normalize_sector(s: str) -> str:
    if not s: return "Unknown"
    t = str(s).strip()
    low = t.lower()

    # Already a canonical sector?
    for canon in GICS_11:
        if low == canon.lower():
            return canon

    # Real Estate (REITs, management & development, etc.)
    if ("real estate" in low) or ("reit" in low):
        return "Real Estate"

    # Communication Services (media, telco)
    if ("telecommunication" in low) or ("media" in low) or ("communication" in low):
        return "Communication Services"

    # Information Technology
    if ("software" in low) or ("technology" in low) or ("semiconductor" in low) or ("hardware" in low) or ("it " in low) or low.startswith("it"):
        return "Information Technology"

    # Consumer Staples
    if ("food" in low) or ("beverage" in low) or ("tobacco" in low) or ("staples" in low) or ("household" in low) or ("personal products" in low):
        return "Consumer Staples"

    # Consumer Discretionary
    if ("retail" in low) or ("automobile" in low) or ("durables" in low) or ("apparel" in low) or ("discretionary" in low) or ("consumer services" in low):
        return "Consumer Discretionary"

    # Health Care
    if ("health" in low) or ("biotech" in low) or ("pharma" in low) or ("life sciences" in low):
        return "Health Care"

    # Financials
    if ("bank" in low) or ("insurance" in low) or ("financial" in low) or ("diversified financial" in low) or ("capital markets" in low):
        return "Financials"

    # Industrials
    if ("capital goods" in low) or ("commercial & professional services" in low) or ("transportation" in low) or ("industrial" in low):
        return "Industrials"

    # Materials
    if ("materials" in low) or ("metals" in low) or ("mining" in low) or ("chemicals" in low) or ("construction materials" in low) or ("paper" in low) or ("forest" in low):
        return "Materials"

    # Energy
    if ("energy" in low) or ("oil" in low) or ("gas" in low) or ("coal" in low) or ("uranium" in low):
        return "Energy"

    # Utilities
    if "utilities" in low or "utility" in low:
        return "Utilities"

    # Fallback
    return "Unknown"

def today_awst_date():
    return (datetime.now(UTC) + timedelta(hours=AWST_OFFSET_HOURS)).date()

def load_static_map(path: str = "config/sectors_static.csv") -> Dict[str, str]:
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path)
        if "Code" in df.columns and "Sector" in df.columns:
            # Normalize any sector labels from the CSV itself
            df["Sector"] = df["Sector"].astype(str).map(_normalize_sector)
            return {str(c).strip().upper(): str(s).strip() for c, s in zip(df["Code"], df["Sector"]) if str(c).strip()}
    except Exception:
        pass
    return {}

def attach_sectors_static(df: pd.DataFrame, static_map: Dict[str, str], log_path: str | None = None) -> pd.DataFrame:
    if df is None or df.empty or "Code" not in df.columns:
        return df
    out = df.copy()
    codes = out["Code"].astype(str).str.upper()
    out["Sector"] = codes.map(static_map).fillna("Unknown")
    # Normalize again, in case map contained non-canonical labels
    out["Sector"] = out["Sector"].astype(str).map(_normalize_sector)

    if log_path is not None:
        missing = sorted(set(codes[codes.map(lambda x: x not in static_map)].tolist()))
        if missing:
            os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
            pd.DataFrame({"Date":[str(today_awst_date())]*len(missing), "Code": missing}).to_csv(log_path, index=False)
    return out

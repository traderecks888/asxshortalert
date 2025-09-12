# scripts/sectors.py
"""
Auto-resolve Sector for ASX codes with this priority:
1) config/sectors.csv (your manual overrides)
2) data/sectors_cache.csv (remembered from prior runs)
3) Yahoo Finance "assetProfile"/"summaryProfile" (live lookup) via {CODE}.AX
4) Fallback: "Unknown"

All sectors are normalized to standard GICS names.
Safe to run in GitHub Actions; rate-limited; caches results.
"""

from __future__ import annotations
import csv
import os
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import requests
import pandas as pd

OVERRIDES_CSV = "config/sectors.csv"
CACHE_CSV     = "data/sectors_cache.csv"

# Normalize Yahoo/various names -> GICS 11 sectors
SECTOR_NORMALIZE = {
    "basic materials": "Materials",
    "materials": "Materials",
    "consumer defensive": "Consumer Staples",
    "consumer staples": "Consumer Staples",
    "consumer cyclical": "Consumer Discretionary",
    "consumer discretionary": "Consumer Discretionary",
    "communication services": "Communication Services",
    "communications": "Communication Services",
    "energy": "Energy",
    "financial services": "Financials",
    "financials": "Financials",
    "healthcare": "Health Care",
    "health care": "Health Care",
    "industrials": "Industrials",
    "technology": "Information Technology",
    "information technology": "Information Technology",
    "real estate": "Real Estate",
    "utilities": "Utilities",
    # Funds/ETFs and oddballs
    "etf": "ETF/Listed Fund",
    "fund": "ETF/Listed Fund",
    "trust": "ETF/Listed Fund",
}

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

def _norm_sector(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    k = str(name).strip().lower()
    return SECTOR_NORMALIZE.get(k, name.strip())

def _read_overrides(path: str = OVERRIDES_CSV) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path)
        # Expect columns: Code,Sector
        if "Code" in df.columns and "Sector" in df.columns:
            out = {}
            for _, r in df.iterrows():
                code = str(r["Code"]).strip().upper()
                if not code:
                    continue
                sec = _norm_sector(r["Sector"])
                if sec:
                    out[code] = sec
            return out
    except Exception:
        pass
    return {}

def _read_cache(path: str = CACHE_CSV) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path)
        if "Code" in df.columns and "Sector" in df.columns:
            return {str(r["Code"]).strip().upper(): str(r["Sector"]).strip() for _, r in df.iterrows() if str(r.get("Sector","")).strip()}
    except Exception:
        pass
    return {}

def _write_cache(mapping: Dict[str, str], path: str = CACHE_CSV) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    rows = [{"Code": k, "Sector": v} for k, v in sorted(mapping.items())]
    pd.DataFrame(rows).to_csv(path, index=False)

def _yf_profile_sector(code: str, timeout: int = 15) -> Optional[str]:
    """Fetch sector from Yahoo Finance quoteSummary modules=assetProfile/summaryProfile"""
    sym = f"{code}.AX"
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Accept": "application/json"})
    for module in ("assetProfile", "summaryProfile"):
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}?modules={module}"
        try:
            r = sess.get(url, timeout=timeout)
            r.raise_for_status()
            js = r.json()
            result = (js or {}).get("quoteSummary", {}).get("result", [])
            if not result:
                continue
            prof = result[0].get(module, {})
            sec = prof.get("sector")
            sec = _norm_sector(sec)
            if sec:
                return sec
        except Exception:
            continue
    # Some ETFs won't have sector; leave None
    return None

def resolve_sectors(codes: Iterable[str], sleep_sec: float = 0.8) -> pd.DataFrame:
    """
    Resolve sectors for a set of codes and update cache.
    Returns DataFrame: Code, Sector, Source (override/cache/yahoo/unknown)
    """
    codes = [str(c).strip().upper() for c in codes if str(c).strip()]
    if not codes:
        return pd.DataFrame(columns=["Code","Sector","Source"])

    overrides = _read_overrides()
    cache = _read_cache()
    out = {}
    src = {}

    # 1) overrides
    for c in codes:
        if c in overrides:
            out[c] = overrides[c]; src[c] = "override"

    # 2) cache
    for c in codes:
        if c in out:
            continue
        if c in cache:
            out[c] = cache[c]; src[c] = "cache"

    # 3) yahoo
    for c in codes:
        if c in out:
            continue
        sec = _yf_profile_sector(c)
        if sec:
            out[c] = sec; src[c] = "yahoo"
            cache[c] = sec
            # be polite to Yahoo
            time.sleep(sleep_sec)

    # 4) unknown
    for c in codes:
        if c not in out:
            out[c] = "Unknown"; src[c] = "unknown"

    # persist cache
    try:
        _write_cache(cache)
    except Exception:
        pass

    rows = [{"Code": c, "Sector": out[c], "Source": src[c]} for c in sorted(out.keys())]
    return pd.DataFrame(rows)

def attach_sectors(df: pd.DataFrame, sectors_df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with 'Sector' column attached by Code."""
    if df is None or df.empty:
        return df
    if sectors_df is None or sectors_df.empty:
        out = df.copy()
        out["Sector"] = out.get("Sector", "Unknown")
        return out
    m = sectors_df.set_index("Code")["Sector"].to_dict()
    out = df.copy()
    out["Sector"] = out["Code"].astype(str).str.upper().map(m).fillna("Unknown")
    return out

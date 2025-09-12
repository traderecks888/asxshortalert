# scripts/make_sectors_from_asx_csv.py
"""
Robust static sectors builder from the official ASX "ASXListedCompanies.csv".
- Handles banner/header lines and odd encodings
- Tries both the primary ASX URL and a MarkitDigital fallback
- Accepts --universe to restrict to config/universe_asx20.csv + config/universe_asx200.csv
- Writes config/sectors_static.csv with columns: Code,Sector

Usage:
  python scripts/make_sectors_from_asx_csv.py
  python scripts/make_sectors_from_asx_csv.py --universe

If the CSV has only "GICS industry group", we map it to the 11 GICS sectors via a built-in map.
"""

import os, io, argparse, re
import pandas as pd
import requests

PRIMARY  = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
FALLBACK = "https://asx.api.markitdigital.com/asx-research/1.0/companies/list.csv"

# Map common GICS Industry Group names -> GICS Sector
INDUSTRY_GROUP_TO_SECTOR = {
    # Energy
    "energy": "Energy",
    # Materials
    "materials": "Materials",
    "metals & mining": "Materials",
    "chemicals": "Materials",
    "paper & forest products": "Materials",
    "construction materials": "Materials",
    # Industrials
    "capital goods": "Industrials",
    "commercial & professional services": "Industrials",
    "transportation": "Industrials",
    # Consumer Discretionary
    "automobiles & components": "Consumer Discretionary",
    "consumer durables & apparel": "Consumer Discretionary",
    "consumer services": "Consumer Discretionary",
    "retailing": "Consumer Discretionary",
    # Consumer Staples
    "food & staples retailing": "Consumer Staples",
    "food beverage & tobacco": "Consumer Staples",
    "household & personal products": "Consumer Staples",
    # Health Care
    "health care equipment & services": "Health Care",
    "pharmaceuticals, biotechnology & life sciences": "Health Care",
    # Financials
    "banks": "Financials",
    "diversified financials": "Financials",
    "insurance": "Financials",
    # Information Technology
    "software & services": "Information Technology",
    "technology hardware & equipment": "Information Technology",
    "semiconductors & semiconductor equipment": "Information Technology",
    # Communication Services
    "telecommunication services": "Communication Services",
    "media & entertainment": "Communication Services",
    # Utilities
    "utilities": "Utilities",
    # Real Estate
    "real estate": "Real Estate",
}

def _download_bytes():
    headers = {"User-Agent":"Mozilla/5.0","Accept":"text/csv, */*;q=0.9"}
    for url in (PRIMARY, FALLBACK):
        try:
            print(f"Downloading: {url}")
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.content, url
        except Exception as e:
            print(f"  WARN: failed: {e}")
            continue
    raise SystemExit("ERROR: Unable to download the ASX list from both endpoints.")

def _decode_best(raw):
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16le","utf-16be","cp1252"):
        try:
            return raw.decode(enc), enc
        except Exception:
            continue
    # last resort
    return raw.decode("latin-1", errors="ignore"), "latin-1"

def _extract_table_text(text):
    """
    Find the header line that contains 'ASX' and 'code' plus either 'GICS' or 'Sector'.
    Return the CSV text starting from that line.
    """
    lines = text.splitlines()
    header_idx = None
    for i, ln in enumerate(lines[:100]):  # look at first 100 lines for a header
        low = ln.lower()
        if ("asx" in low and "code" in low) and ("gics" in low or "sector" in low or "industry" in low):
            header_idx = i
            break
    if header_idx is None:
        return None
    return "\n".join(lines[header_idx:])

def _read_csv_robust(raw):
    # Try straight to pandas with sniffed separator
    try:
        df = pd.read_csv(io.BytesIO(raw), engine="python", sep=None)
        if df.shape[1] >= 2:
            return df
    except Exception:
        pass
    # Decode and search for header inside text (banner above table)
    txt, enc = _decode_best(raw)
    body = _extract_table_text(txt)
    if body:
        try:
            return pd.read_csv(io.StringIO(body), engine="python", sep=None)
        except Exception:
            pass
    # Fallback: try common separators on the decoded text
    for sep in [",",";","\t","|"]:
        try:
            df = pd.read_csv(io.StringIO(txt), sep=sep)
            if df.shape[1] >= 2:
                return df
        except Exception:
            continue
    return None

def _prefer_series(*cands):
    for s in cands:
        if s is not None:
            return s
    return None

def _map_industry_group_to_sector(s):
    if pd.isna(s):
        return None
    key = str(s).strip().lower()
    return INDUSTRY_GROUP_TO_SECTOR.get(key, None)

def _read_universe_csv(path):
    if not os.path.exists(path): return []
    try:
        df = pd.read_csv(path)
        if "Code" in df.columns:
            return df["Code"].dropna().astype(str).str.upper().str.strip().tolist()
    except Exception:
        pass
    return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", action="store_true", help="Filter to config/universe_asx20.csv + config/universe_asx200.csv if present")
    args = ap.parse_args()

    raw, used = _download_bytes()
    df = _read_csv_robust(raw)
    if df is None:
        raise SystemExit("ERROR: Could not parse the downloaded CSV (layout/encoding changed).")

    # Normalize columns
    cols = {c.lower().strip(): c for c in df.columns}

    # Find code column
    code_col = None
    for key in cols:
        if "asx" in key and "code" in key:
            code_col = cols[key]; break
    if code_col is None:
        for key in cols:
            if key in ("asx code","code","ticker"):
                code_col = cols[key]; break
    if code_col is None:
        # Try any column that looks like a 2-4 letter code in most rows
        cand = None
        for c in df.columns:
            sample = df[c].astype(str).str.strip().dropna().head(30)
            if len(sample) and (sample.str.fullmatch(r"[A-Z0-9]{2,5}").mean() > 0.6):
                cand = c; break
        code_col = cand
    if code_col is None:
        raise SystemExit(f"ERROR: Could not find an ASX code column. Columns seen: {list(df.columns)}")

    # Sector/Industry fields
    sector_col = None
    for key in cols:
        if "gics sector" in key or key == "sector":
            sector_col = cols[key]; break
    industry_col = None
    for key in cols:
        if "gics industry group" in key or "industry group" in key:
            industry_col = cols[key]; break

    # Build final table
    out = pd.DataFrame()
    out["Code"] = df[code_col].astype(str).str.upper().str.strip()

    if sector_col:
        out["Sector"] = df[sector_col].astype(str).str.strip()
    elif industry_col:
        mapped = df[industry_col].map(_map_industry_group_to_sector)
        # if mapping failed, keep the original group label (better than Unknown)
        out["Sector"] = mapped.where(mapped.notna(), df[industry_col].astype(str).str.strip())
    else:
        # last resort: create Unknown
        out["Sector"] = "Unknown"

    # Clean
    out = out[out["Code"].str.len() > 0]
    out = out.drop_duplicates(subset=["Code"], keep="first")

    if args.universe:
        uni = set(_read_universe_csv("config/universe_asx20.csv") + _read_universe_csv("config/universe_asx200.csv"))
        if uni:
            out = out[out["Code"].isin(sorted(uni))]
            print(f"Filtered to universe: {len(out)} rows")
        else:
            print("WARN: --universe given but no universe files found; writing full list.")

    os.makedirs("config", exist_ok=True)
    out_path = "config/sectors_static.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(out)} rows (source: {used}).")

if __name__ == "__main__":
    main()

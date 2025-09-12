# scripts/build_static_sectors_fast.py
"""
FAST static sector builder.
- Default: **no network calls**. Uses existing static CSV + cache + overrides; logs unknowns.
- Optional: enable Yahoo fetch for missing tickers with --yahoo (parallel, capped).

Usage examples:
  python scripts/build_static_sectors_fast.py                         # quick, offline
  python scripts/build_static_sectors_fast.py --yahoo                 # try to fill missing via Yahoo
  python scripts/build_static_sectors_fast.py --universe-only         # ignore data/*.csv tickers
  python scripts/build_static_sectors_fast.py --days 2 --yahoo        # include last 2 days data

Outputs:
  - config/sectors_static.csv (merged & updated)
  - data/sectors_unknown_today.csv (tickers you still need to fill)
"""

import os, glob, argparse, concurrent.futures as cf
import pandas as pd
from datetime import datetime, timezone, timedelta

# Optional Yahoo fetcher (only used if --yahoo flag)
try:
    from yahoo_sector import fetch_sector_yahoo
except Exception:
    fetch_sector_yahoo = None

AWST_OFFSET_HOURS = 8
def today_awst_date():
    return (datetime.now(timezone.utc) + timedelta(hours=AWST_OFFSET_HOURS)).date()

def _read_codes_from_universe_csv(path):
    if not os.path.exists(path): return []
    try:
        df=pd.read_csv(path)
        if "Code" in df.columns:
            return df["Code"].dropna().astype(str).str.upper().tolist()
    except Exception: pass
    return []

def _read_codes_from_universe_yml(path="config/universe.yml"):
    try:
        import yaml
    except Exception:
        return []
    if not os.path.exists(path): return []
    try:
        with open(path,"r",encoding="utf-8") as f:
            y=yaml.safe_load(f) or {}
        codes=set()
        for _, arr in (y.get("lists") or {}).items():
            for c in (arr or []):
                if c: codes.add(str(c).strip().upper())
        return sorted(codes)
    except Exception:
        return []

def _read_codes_from_data(days=3):
    codes=set()
    files=sorted(glob.glob("data/asic_*.csv")+glob.glob("data/gross_*.csv"))[-max(days,0):]
    for p in files:
        try:
            df=pd.read_csv(p)
            if "Code" in df.columns:
                codes.update(df["Code"].dropna().astype(str).str.upper().tolist())
        except Exception:
            pass
    return sorted(codes)

def _read_static_map(path="config/sectors_static.csv"):
    if not os.path.exists(path): return {}
    try:
        df=pd.read_csv(path)
        if "Code" in df.columns and "Sector" in df.columns:
            return {str(c).strip().upper(): str(s).strip() for c,s in zip(df["Code"], df["Sector"]) if str(c).strip() and str(s).strip()}
    except Exception: pass
    return {}

def _read_cache(path="data/sectors_cache.csv"):
    if not os.path.exists(path): return {}
    try:
        df=pd.read_csv(path)
        if "Code" in df.columns and "Sector" in df.columns:
            return {str(c).strip().upper(): str(s).strip() for c,s in zip(df["Code"], df["Sector"]) if str(c).strip() and str(s).strip()}
    except Exception: pass
    return {}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3, help="include codes from last N data files")
    ap.add_argument("--universe-only", action="store_true", help="ignore data/*.csv tickers")
    ap.add_argument("--yahoo", action="store_true", help="attempt to fill missing via Yahoo (parallel)")
    ap.add_argument("--max-workers", type=int, default=12, help="parallel workers for Yahoo fetch")
    args=ap.parse_args()

    codes=set()
    codes.update(_read_codes_from_universe_yml())
    codes.update(_read_codes_from_universe_csv("config/universe_asx20.csv"))
    codes.update(_read_codes_from_universe_csv("config/universe_asx200.csv"))
    if not args.universe_only:
        codes.update(_read_codes_from_data(args.days))

    codes=sorted(codes)
    if not codes:
        print("No codes found. Populate universe files or run after first daily job.")
        return

    static=_read_static_map()
    cache=_read_cache()

    # Start with what we already know
    mapping=dict(static)
    # Fill from cache (fast, offline)
    for c in codes:
        if c not in mapping and c in cache:
            mapping[c]=cache[c]

    missing=[c for c in codes if c not in mapping]

    print(f"Codes total: {len(codes)} | prefilled: {len(mapping)} | missing: {len(missing)}")

    if args.yahoo and missing:
        if fetch_sector_yahoo is None:
            print("Yahoo fetcher not available. Install requests or include yahoo_sector.py.")
        else:
            def task(code):
                sec=fetch_sector_yahoo(code)
                return code, sec
            with cf.ThreadPoolExecutor(max_workers=max(1,args.max_workers)) as ex:
                for code, sec in ex.map(task, missing):
                    if sec:
                        mapping[code]=sec
        # recompute missing
        missing=[c for c in codes if c not in mapping]

    # Write static CSV (only for the codes in our set)
    rows=[{"Code":c,"Sector":mapping.get(c, "Unknown")} for c in codes]
    os.makedirs("config", exist_ok=True)
    pd.DataFrame(rows).to_csv("config/sectors_static.csv", index=False)
    print(f"Wrote config/sectors_static.csv with {len(rows)} rows. Unknowns: {len([r for r in rows if r['Sector']=='Unknown'])}")

    # Log unknowns to help quarterly updates
    if missing:
        os.makedirs("data", exist_ok=True)
        pd.DataFrame({"Date":[str(today_awst_date())]*len(missing), "Code": missing}).to_csv("data/sectors_unknown_today.csv", index=False)
        print(f"Logged {len(missing)} unknowns to data/sectors_unknown_today.csv")

if __name__=="__main__":
    main()

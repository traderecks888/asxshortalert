# scripts/build_static_sectors.py

"""
Build a static sector map once (or quarterly) and save to config/sectors_static.csv.

It uses the resolver logic from 'scripts/sectors.py' (overrides -> cache -> Yahoo) to fetch
sectors for your current universes and recent data, then writes a frozen CSV.

Usage:
  python scripts/build_static_sectors.py              # local
(Optionally add a one-off Actions step to run it once, then remove.)

After generation, your daily pipeline will read only config/sectors_static.csv,
so there are no network calls for sector info at run time.
"""
import os, glob, yaml, pandas as pd

# Import resolver if present (from earlier bundles). If missing, user can add it.
try:
    from sectors import resolve_sectors  # type: ignore
except Exception as e:
    raise SystemExit("Missing scripts/sectors.py (resolver). Please add it from the previous bundle, or ask me to include one.") from e

def _codes_from_universe_csv(path):
    if not os.path.exists(path): return []
    try:
        df=pd.read_csv(path)
        if "Code" in df.columns:
            return df["Code"].dropna().astype(str).str.upper().tolist()
    except Exception: pass
    return []

def _codes_from_universe_yml(path="config/universe.yml"):
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

def _codes_from_recent_data(n=7):
    codes=set()
    for pat in ["data/asic_*.csv","data/gross_*.csv"]:
        files=sorted(glob.glob(pat))[-n:]
        for p in files:
            try:
                df=pd.read_csv(p)
                if "Code" in df.columns:
                    codes.update(df["Code"].dropna().astype(str).str.upper().tolist())
            except Exception:
                pass
    return sorted(codes)

def main():
    codes=set()
    codes.update(_codes_from_universe_yml())
    codes.update(_codes_from_universe_csv("config/universe_asx20.csv"))
    codes.update(_codes_from_universe_csv("config/universe_asx200.csv"))
    codes.update(_codes_from_recent_data())

    if not codes:
        print("No codes found — populate universe files or run once after first daily job.")
        return

    print(f"Building static sectors for {len(codes)} codes…")
    df=resolve_sectors(sorted(codes))
    out=df[["Code","Sector"]].drop_duplicates().sort_values("Code")
    os.makedirs("config", exist_ok=True)
    out.to_csv("config/sectors_static.csv", index=False)
    print("Wrote config/sectors_static.csv with", len(out), "rows.")

if __name__=="__main__":
    main()

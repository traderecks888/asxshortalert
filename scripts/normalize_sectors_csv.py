# scripts/normalize_sectors_csv.py
"""
One-off tool: normalize your existing config/sectors_static.csv so it only uses the canonical 11 GICS sectors.
Run locally once, commit the cleaned file, and your dropdown will stop showing 'industry group' variations.

Usage:
  python scripts/normalize_sectors_csv.py
"""
import os, pandas as pd
from sectors_static import _normalize_sector  # reuse logic

SRC = "config/sectors_static.csv"

def main():
    if not os.path.exists(SRC):
        print("No config/sectors_static.csv found.")
        return
    df = pd.read_csv(SRC)
    if "Code" not in df.columns or "Sector" not in df.columns:
        print("config/sectors_static.csv must have columns: Code,Sector")
        return
    df["Code"] = df["Code"].astype(str).str.upper().str.strip()
    df["Sector"] = df["Sector"].astype(str).map(_normalize_sector)
    df = df[df["Code"] != ""].drop_duplicates(subset=["Code"], keep="first").sort_values("Code")
    df.to_csv(SRC, index=False)
    print(f"Normalized {len(df)} rows and rewrote {SRC}")

if __name__ == "__main__":
    main()

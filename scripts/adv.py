# scripts/adv.py
# Fetch ADV (average daily volume) via Yahoo Finance CSV download for ASX codes.

import time
import io
from datetime import datetime, timedelta
import requests
import pandas as pd

def _yf_symbol(code: str) -> str:
    code = str(code).strip().upper()
    return f"{code}.AX"

def _yf_download_csv(symbol: str, lookback_days: int = 120) -> pd.DataFrame:
    now = datetime.utcnow()
    period2 = int(now.timestamp())
    period1 = int((now - timedelta(days=lookback_days)).timestamp())
    url = (
        f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
        f"?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def fetch_adv(codes, window_days: int = 30, sleep_sec: float = 1.0) -> pd.DataFrame:
    """Return DataFrame: Code, ADV (shares/day). Missing symbols are skipped."""
    out = []
    seen = set()
    for code in codes:
        c = str(code).strip().upper()
        if not c or c in seen: continue
        seen.add(c)
        sym = _yf_symbol(c)
        try:
            df = _yf_download_csv(sym)
            if "Volume" not in df.columns: continue
            vol = df.dropna(subset=["Volume"]).tail(window_days)["Volume"].astype(float)
            if len(vol) == 0: continue
            adv = float(vol.mean())
            out.append({"Code": c, "ADV": adv})
        except Exception:
            continue
        time.sleep(sleep_sec)
    return pd.DataFrame(out)

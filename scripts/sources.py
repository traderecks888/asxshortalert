# scripts/sources.py
# Robust: built-in retry, compute % Issued = Gross/Issued*100, ignore vendor % fields.

import io, re, calendar, time
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

UTC = timezone.utc

def _dstr(d): return d.strftime("%Y%m%d")
def _mon3(d): return calendar.month_abbr[d.month].lower()

def _get(url, retries=6, backoff_sec=10, timeout=30):
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt == retries - 1:
                raise
            time.sleep(backoff_sec * (attempt + 1))
    raise last_err

def fetch_asic_short_positions(today_utc=None, max_lookback_days=8):
    """Find latest ASIC Short Positions CSV (T+4). Keep '% short' as numeric percent."""
    if today_utc is None: today_utc = datetime.now(UTC)
    for extra in range(0, max_lookback_days):
        d = (today_utc - timedelta(days=4 + extra))
        url = f"https://download.asic.gov.au/short-selling/RR{_dstr(d)}-001-SSDailyAggShortPos.csv"
        try:
            csv = _get(url).text
            df = pd.read_csv(io.StringIO(csv))
            df.columns = [c.strip() for c in df.columns]
            df.rename(columns={
                "Product Code": "Code",
                "Reported Short Positions": "ReportedShort",
                "Total Product in Issue": "Issued",
                "% of Total Product in Issue Reported as Short Positions": "PctShort",  # percent units
            }, inplace=True)
            df["Date"] = d.date()
            # Coerce numerics
            for c in ("ReportedShort","Issued","PctShort"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            return df, url, d.date()
        except Exception:
            continue
    raise RuntimeError("ASIC CSV not found in expected window.")

def fetch_asx_gross_shorts(target_date):
    """ASX text (T+1). Compute PctGrossVsIssuedPct = Gross/Issued*100."""
    y, mon3 = target_date.year, _mon3(target_date)
    url = f"https://asxonline.com/content/dam/asxonline/public/reports/{y}/{mon3}/shortsell_gross_{_dstr(target_date)}.txt"
    txt = _get(url).text
    lines = [ln.strip() for ln in txt.splitlines()]
    pat = re.compile(
        r"^(?P<Code>[A-Z0-9]{2,4})\s+(?P<Name>.+?)\s+"
        r"(?P<Class>ETF UNITS|CDI 1:1|CDI 3:1|FPO|STAPLED|FPO NZX|FPO NZ)\s+"
        r"(?P<Gross>[\d,]+)\s+(?P<Issued>[\d,]+)\s+(?P<Pct>[\.\d]+)$"
    )
    rows = []
    for ln in lines:
        m = pat.match(ln)
        if m: rows.append(m.groupdict())
    df = pd.DataFrame(rows)
    if df.empty:
        return df, url
    df["Gross"] = pd.to_numeric(df["Gross"].str.replace(",",""), errors="coerce")
    df["Issued"] = pd.to_numeric(df["Issued"].str.replace(",",""), errors="coerce")
    # Our own % calc (ignore file's Pct)
    df["PctGrossVsIssuedPct"] = (df["Gross"] / df["Issued"]) * 100.0
    df.drop(columns=[c for c in ["Pct"] if c in df.columns], inplace=True)
    df["Date"] = target_date
    return df, url

def _find_col(df, key_snippets):
    lower_map = {c.lower(): c for c in df.columns}
    for snip in key_snippets:
        for low, orig in lower_map.items():
            if snip in low:
                return orig
    return None

def fetch_cboe_gross_shorts(target_date):
    """Cboe AU CSV (T+1). Compute PctGrossVsIssuedPct = Gross/Issued*100; ignore vendor %."""
    url = f"https://cdn.cboe.com/data/au/equities/short_sale_reports/Short_Sell_{_dstr(target_date)}.csv"
    csv = _get(url).text
    header_idx = None
    lines = csv.splitlines()
    for i, ln in enumerate(lines):
        if ln.strip().startswith("Code,"):
            header_idx = i; break
    if header_idx is None:
        raise ValueError("Cboe CSV header not found")

    df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))
    code_col   = _find_col(df, ["code"])
    gross_col  = _find_col(df, ["reported gross short sales", "gross short"])
    issued_col = _find_col(df, ["issued capital"])
    # We will ignore vendor % column entirely.
    if not all([code_col, gross_col, issued_col]):
        raise KeyError("Cboe CSV column names changed; could not map")

    df = df.rename(columns={code_col:"Code", gross_col:"Gross", issued_col:"Issued"})
    df["Gross"] = pd.to_numeric(df["Gross"], errors="coerce")
    df["Issued"] = pd.to_numeric(df["Issued"], errors="coerce")
    df["PctGrossVsIssuedPct"] = (df["Gross"] / df["Issued"]) * 100.0
    df["Date"] = target_date
    return df, url

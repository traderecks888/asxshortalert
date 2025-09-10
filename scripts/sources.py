import io, re, calendar
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt

UTC = timezone.utc

def _dstr(d): return d.strftime("%Y%m%d")
def _mon3(d): return calendar.month_abbr[d.month].lower()  # 'nov'

@retry(wait=wait_fixed(10), stop=stop_after_attempt(6))
def _get(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r

def fetch_asic_short_positions(today_utc=None, max_lookback_days=8):
    """
    Find the most recent ASIC Short Positions CSV. T+4 lag is typical.
    We try today-4, then 5,6,... until we hit an existing CSV.
    Example URL:
      https://download.asic.gov.au/short-selling/RR20250725-001-SSDailyAggShortPos.csv
    """
    if today_utc is None: today_utc = datetime.now(UTC)
    for extra in range(0, max_lookback_days):
        d = (today_utc - timedelta(days=4+extra))
        url = f"https://download.asic.gov.au/short-selling/RR{_dstr(d)}-001-SSDailyAggShortPos.csv"
        try:
            csv = _get(url).text
            df = pd.read_csv(io.StringIO(csv))
            df.columns = [c.strip() for c in df.columns]
            df.rename(columns={
                "Product Code":"Code",
                "Reported Short Positions":"ReportedShort",
                "Total Product in Issue":"Issued",
                "% of Total Product in Issue Reported as Short Positions":"PctShort"
            }, inplace=True)
            df["Date"] = d.date()
            df["PctShort"] = pd.to_numeric(df["PctShort"], errors="coerce")
            return df, url, d.date()
        except Exception:
            continue
    raise RuntimeError("ASIC CSV not found in expected window.")

def fetch_asx_gross_shorts(target_date):
    """
    ASX text file (per prior trading day), e.g.:
    https://asxonline.com/content/dam/asxonline/public/reports/2024/nov/shortsell_gross_20241126.txt
    """
    y, mon3 = target_date.year, _mon3(target_date)
    url = f"https://asxonline.com/content/dam/asxonline/public/reports/{y}/{mon3}/shortsell_gross_{_dstr(target_date)}.txt"
    txt = _get(url).text
    lines = [ln.strip() for ln in txt.splitlines()]
    pat = re.compile(r"^(?P<Code>[A-Z0-9]{2,4})\s+(?P<Name>.+?)\s+(?P<Class>ETF UNITS|CDI 1:1|CDI 3:1|FPO|STAPLED|FPO NZX|FPO NZ)\s+(?P<Gross>[\d,]+)\s+(?P<Issued>[\d,]+)\s+(?P<Pct>\.?\d+)$")
    rows = []
    for ln in lines:
        m = pat.match(ln)
        if m:
            rows.append(m.groupdict())
    df = pd.DataFrame(rows)
    for c in ["Gross","Issued"]: df[c] = df[c].str.replace(",","", regex=False).astype("Int64")
    df["PctGrossVsIssued"] = pd.to_numeric(df["Pct"], errors="coerce") # fraction (0.xx)
    df.drop(columns=["Pct"], inplace=True)
    df["Date"] = target_date
    return df, url

def fetch_cboe_gross_shorts(target_date):
    """
    Cboe AU mirror CSV:
    https://cdn.cboe.com/data/au/equities/short_sale_reports/Short_Sell_YYYYMMDD.csv
    """
    url = f"https://cdn.cboe.com/data/au/equities/short_sale_reports/Short_Sell_{_dstr(target_date)}.csv"
    csv = _get(url).text
    header_idx = None
    lines = csv.splitlines()
    for i, ln in enumerate(lines):
        if ln.startswith("Code,"):
            header_idx = i; break
    df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))
    df.rename(columns={
        "Code":"Code",
        "Reported Gross Short Sales (a)":"Gross",
        "Issued Capital (b)":"Issued",
        "% of issued capital reported as short sold (a)/(b)":"PctGrossVsIssued"
    }, inplace=True)
    df["Gross"] = pd.to_numeric(df["Gross"], errors="coerce").astype("Int64")
    df["Issued"] = pd.to_numeric(df["Issued"], errors="coerce").astype("Int64")
    df["PctGrossVsIssued"] = pd.to_numeric(df["PctGrossVsIssued"], errors="coerce")
    df["Date"] = target_date
    return df, url

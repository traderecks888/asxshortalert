# scripts/yahoo_sector.py
import requests, time, random

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
SECTOR_NORMALIZE = {
    "basic materials":"Materials","materials":"Materials",
    "consumer defensive":"Consumer Staples","consumer staples":"Consumer Staples",
    "consumer cyclical":"Consumer Discretionary","consumer discretionary":"Consumer Discretionary",
    "communication services":"Communication Services","communications":"Communication Services",
    "energy":"Energy",
    "financial services":"Financials","financials":"Financials",
    "healthcare":"Health Care","health care":"Health Care",
    "industrials":"Industrials",
    "technology":"Information Technology","information technology":"Information Technology",
    "real estate":"Real Estate",
    "utilities":"Utilities",
    "etf":"ETF/Listed Fund","fund":"ETF/Listed Fund","trust":"ETF/Listed Fund"
}
def _norm_sector(name):
    if not name: return None
    k=str(name).strip().lower()
    return SECTOR_NORMALIZE.get(k, str(name).strip())

def fetch_sector_yahoo(code, timeout=12):
    """Return normalized sector or None; single request; polite headers."""
    sym=f"{code}.AX"
    sess=requests.Session()
    sess.headers.update({"User-Agent": UA, "Accept":"application/json"})
    for module in ("assetProfile","summaryProfile"):
        url=f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}?modules={module}"
        try:
            r=sess.get(url, timeout=timeout)
            r.raise_for_status()
            js=r.json()
            res=(js or {}).get("quoteSummary",{}).get("result",[])
            if not res: continue
            prof=res[0].get(module,{}) or {}
            sec=_norm_sector(prof.get("sector"))
            if sec: return sec
        except Exception:
            # backoff a touch on transient errors
            time.sleep(0.1 + random.random()*0.2)
            continue
    return None

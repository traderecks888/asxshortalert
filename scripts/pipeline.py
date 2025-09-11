# scripts/pipeline.py
import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import yaml

from sources import fetch_asic_short_positions, fetch_asx_gross_shorts, fetch_cboe_gross_shorts
from signals import compute_short_position_signals, compute_gross_shorts_signals
from render import render_dashboard
from history import update_history_and_charts
from adv import fetch_adv
from scoring import covering_scores

UTC = timezone.utc
AWST_OFFSET_HOURS = 8  # Perth is UTC+8 (no DST)

def today_awst():
    return datetime.now(UTC) + timedelta(hours=AWST_OFFSET_HOURS)

def read_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def save_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)

def _safe_top(df, col, n):
    if df is None or len(df) == 0 or col not in df.columns or n <= 0:
        return []
    df2 = df.copy()
    df2[col] = pd.to_numeric(df2[col], errors="coerce")
    df2 = df2[df2[col].notna()]
    if df2.empty:
        return []
    return df2.nlargest(n, col).to_dict(orient="records")

def _load_sectors(path="config/sectors.csv"):
    if not os.path.exists(path): return {}
    try:
        df = pd.read_csv(path)
        if "Code" in df.columns and "Sector" in df.columns:
            return {str(r["Code"]).strip().upper(): str(r["Sector"]).strip() for _, r in df.iterrows()}
    except Exception:
        pass
    return {}

if __name__ == "__main__":
    cfg = read_yaml("config/alerts.yml") if os.path.exists("config/alerts.yml") else {}

    # 1) Gross shorts (T+1)
    yday_awst = (today_awst() - timedelta(days=1)).date()
    try:
        asx_df = cboe_df = None
        try:
            asx_df, _ = fetch_asx_gross_shorts(yday_awst); print(f"INFO: ASX gross {yday_awst}")
        except Exception as e:
            print(f"WARN: ASX gross unavailable {yday_awst}: {e}")
        try:
            cboe_df, _ = fetch_cboe_gross_shorts(yday_awst); print(f"INFO: Cboe gross {yday_awst}")
        except Exception as e:
            print(f"WARN: Cboe gross unavailable {yday_awst}: {e}")
        frames = [x for x in (asx_df, cboe_df) if x is not None and len(x)]
        if frames:
            gross_all = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Code","Gross","Issued"], keep="last")
            save_csv(gross_all, f"data/gross_{yday_awst}.csv")
        else:
            gross_all = pd.DataFrame(); print(f"WARN: No gross rows {yday_awst}.")
    except Exception as e:
        print(f"ERROR: gross aggregation: {e}"); gross_all = pd.DataFrame()

    gross_sig = compute_gross_shorts_signals(gross_all, cfg=cfg)

    # Sector mapping
    sectors = _load_sectors()
    if sectors and not gross_sig.empty:
        gross_sig["Sector"] = gross_sig["Code"].map(sectors).fillna("Unknown")
    else:
        gross_sig["Sector"] = "Unknown"

    gross_top_qty = _safe_top(gross_sig, "Gross_num", cfg.get("gross_shorts", {}).get("top_n", 25))
    gross_top_pct = _safe_top(gross_sig, "PctGrossVsIssuedPct_num", cfg.get("gross_shorts", {}).get("top_n", 25))

    # 2) Short positions (T+4)
    try:
        asic_df, _, asic_date = fetch_asic_short_positions(); save_csv(asic_df, f"data/asic_{asic_date}.csv")
    except Exception as e:
        print(f"ERROR: ASIC fetch: {e}"); asic_df = pd.DataFrame(columns=["Code","ReportedShort","Issued","PctShort","Date"]); asic_date = today_awst().date()

    prev_df = None
    try:
        candidates = sorted([p for p in os.listdir("data") if p.startswith("asic_")])
        if len(candidates) >= 2: prev_df = pd.read_csv(os.path.join("data", candidates[-2]))
    except Exception as e:
        print(f"WARN: prev ASIC not loaded: {e}")

    pos_sig = compute_short_position_signals(asic_df, df_prev=prev_df, cfg=cfg)

    # ADV & Days-to-Cover
    adv_days = int(cfg.get("short_positions", {}).get("adv_window_days", 30))
    if not pos_sig.empty:
        codes = sorted(set(pos_sig["Code"].dropna().astype(str).str.upper().tolist()))
        adv_df = fetch_adv(codes, window_days=adv_days)
        if not adv_df.empty:
            pos_sig = pos_sig.merge(adv_df, on="Code", how="left")
            pos_sig["ADV"] = pd.to_numeric(pos_sig["ADV"], errors="coerce").fillna(0.0)
            pos_sig["DaysToCover"] = (pos_sig["ShortedShares"] / pos_sig["ADV"]).replace([float("inf"), -float("inf")], 0.0).fillna(0.0)
        else:
            pos_sig["ADV"] = 0.0
            pos_sig["DaysToCover"] = 0.0

    # Sector on pos
    if sectors and not pos_sig.empty:
        pos_sig["Sector"] = pos_sig["Code"].map(sectors).fillna("Unknown")
    else:
        pos_sig["Sector"] = "Unknown"

    # Numeric helpers
    if not pos_sig.empty:
        for c in ("PctShort_pp","Delta_pp","DeltaShares","DaysToCover","ADV"):
            if c in pos_sig.columns:
                pos_sig[c + ("_num" if not c.endswith("_num") else "")] = pd.to_numeric(pos_sig[c], errors="coerce").fillna(0.0)

    top_n_pos = cfg.get("short_positions", {}).get("top_n", 25)
    pos_high  = pos_sig.sort_values("PctShort_pp_num", ascending=False).head(top_n_pos).to_dict(orient="records")
    pos_delta = pos_sig.sort_values("Delta_pp_num",    ascending=False).head(top_n_pos).to_dict(orient="records")
    pos_cover = pos_sig.sort_values("DeltaShares_num", ascending=True ).head(top_n_pos).to_dict(orient="records")
    pos_dtc   = pos_sig.sort_values("DaysToCover_num", ascending=False).head(top_n_pos).to_dict(orient="records")

    # API, history & charts (this also appends today's pos to history)
    charts = update_history_and_charts(gross_sig, pos_sig, yday_awst, asic_date)

    # 3/5-day covering scores from history (now that today is appended)
    cov3 = covering_scores(3)
    cov5 = covering_scores(5)
    cov3_list = cov3.head(top_n_pos).to_dict(orient="records") if not cov3.empty else []
    cov5_list = cov5.head(top_n_pos).to_dict(orient="records") if not cov5.empty else []

    # Render site
    # Collect all sectors present for filter
    sectors_on_page = sorted(set([r.get("Sector","Unknown") for r in (gross_top_qty + gross_top_pct + pos_high + pos_delta + pos_cover + pos_dtc) if r.get("Sector")]))
    ctx = {
        "generated_at": today_awst().strftime("%Y-%m-%d %H:%M AWST"),
        "gross_date": yday_awst.strftime("%Y-%m-%d"),
        "asic_date":  asic_date.strftime("%Y-%m-%d"),
        "sectors": ["All"] + sectors_on_page,
        "gross_top_qty": gross_top_qty,
        "gross_top_pct": gross_top_pct,
        "pos_high":  pos_high,
        "pos_delta": pos_delta,
        "pos_cover": pos_cover,
        "pos_dtc":   pos_dtc,
        "cov3": cov3_list,
        "cov5": cov5_list,
        "charts": charts,
    }
    render_dashboard("docs", ctx)

    # Notifications (optional; skipped if no secrets)
    try:
        from notify import maybe_notify
        maybe_notify(ctx, gross_sig, pos_sig)
    except Exception as e:
        print(f"WARN: notification skipped/failed: {e}")

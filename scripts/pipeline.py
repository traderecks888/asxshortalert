import os
from datetime import datetime, timedelta, timezone
import yaml, pandas as pd
from sources import fetch_asic_short_positions, fetch_asx_gross_shorts, fetch_cboe_gross_shorts
from signals import compute_short_position_signals, compute_gross_shorts_signals
from render import render_dashboard

UTC = timezone.utc

def read_yaml(p): 
    with open(p, "r", encoding="utf-8") as f: return yaml.safe_load(f)

def save_csv(df, path): 
    os.makedirs(os.path.dirname(path), exist_ok=True); df.to_csv(path, index=False)

def today_awst():
    # GitHub runners are UTC; AWST = UTC+8 (no DST)
    return datetime.now(UTC) + timedelta(hours=8)

if __name__ == "__main__":
    cfg = read_yaml("config/alerts.yml")

    # 1) GROSS SHORTS (T+1)
    yday_awst = (today_awst() - timedelta(days=1)).date()
    try:
        asx_df, asx_url = None, None
        try:
            asx_df, asx_url = fetch_asx_gross_shorts(yday_awst)
        except Exception:
            pass
        cboe_df, cboe_url = None, None
        try:
            cboe_df, cboe_url = fetch_cboe_gross_shorts(yday_awst)
        except Exception:
            pass

        frames = [x for x in [asx_df, cboe_df] if x is not None]
        gross_all = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Code","Gross","Issued"], keep="last")
        gross_all.to_csv(f"data/gross_{yday_awst}.csv", index=False)
    except Exception:
        gross_all = pd.DataFrame()

    gross_sig = compute_gross_shorts_signals(gross_all, cfg=cfg)
    gross_top_qty = gross_sig.nlargest(cfg["gross_shorts"]["top_n"], "Gross").to_dict(orient="records")
    gross_top_pct = gross_sig.nlargest(cfg["gross_shorts"]["top_n"], "PctGrossVsIssued").to_dict(orient="records")

    # 2) SHORT POSITIONS (T+4)
    asic_df, asic_url, asic_date = fetch_asic_short_positions()
    save_csv(asic_df, f"data/asic_{asic_date}.csv")
    prev_candidates = sorted([p for p in os.listdir("data") if p.startswith("asic_")])
    prev_df = None
    if len(prev_candidates) >= 2:
        prev_df = pd.read_csv(os.path.join("data", prev_candidates[-2]))
    pos_sig = compute_short_position_signals(asic_df, df_prev=prev_df, cfg=cfg)
    pos_high = pos_sig.sort_values("PctShort_pp", ascending=False).head(cfg["short_positions"]["top_n"]).to_dict(orient="records")
    pos_delta = pos_sig.sort_values("Delta_pp", ascending=False).head(cfg["short_positions"]["top_n"]).to_dict(orient="records")

    # 3) Render site
    ctx = {
        "generated_at": today_awst().strftime("%Y-%m-%d %H:%M AWST"),
        "gross_date": yday_awst.strftime("%Y-%m-%d"),
        "asic_date": asic_date.strftime("%Y-%m-%d"),
        "gross_top_qty": gross_top_qty,
        "gross_top_pct": gross_top_pct,
        "pos_high": pos_high,
        "pos_delta": pos_delta,
    }
    render_dashboard("docs", ctx)

    # 4) Notifications (optional; use secrets)
    from notify import maybe_notify
    maybe_notify(ctx, gross_sig, pos_sig)

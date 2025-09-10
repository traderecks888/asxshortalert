# scripts/pipeline.py
import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import yaml

from sources import (
    fetch_asic_short_positions,
    fetch_asx_gross_shorts,
    fetch_cboe_gross_shorts,
)
from signals import (
    compute_short_position_signals,
    compute_gross_shorts_signals,
)
from render import render_dashboard

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
    """
    Return top-N rows by numeric column `col`, robust to empty inputs
    and non-numeric dtypes. Returns a list[dict] for templating.
    """
    if df is None or len(df) == 0 or col not in df.columns or n <= 0:
        return []
    df2 = df.copy()
    df2[col] = pd.to_numeric(df2[col], errors="coerce")
    df2 = df2[df2[col].notna()]
    if df2.empty:
        return []
    return df2.nlargest(n, col).to_dict(orient="records")


if __name__ == "__main__":
    cfg = read_yaml("config/alerts.yml") if os.path.exists("config/alerts.yml") else {}

    # ---------------------------
    # 1) GROSS SHORTS (T+1)
    # ---------------------------
    yday_awst = (today_awst() - timedelta(days=1)).date()
    try:
        asx_df = cboe_df = None
        try:
            asx_df, _ = fetch_asx_gross_shorts(yday_awst)
            print(f"INFO: fetched ASX gross shorts for {yday_awst}")
        except Exception as e:
            print(f"WARN: ASX gross short file not available for {yday_awst}: {e}")

        try:
            cboe_df, _ = fetch_cboe_gross_shorts(yday_awst)
            print(f"INFO: fetched Cboe gross shorts for {yday_awst}")
        except Exception as e:
            print(f"WARN: Cboe gross short CSV not available for {yday_awst}: {e}")

        frames = [x for x in (asx_df, cboe_df) if x is not None and len(x)]
        if frames:
            gross_all = (
                pd.concat(frames, ignore_index=True)
                .drop_duplicates(subset=["Code", "Gross", "Issued"], keep="last")
            )
            save_csv(gross_all, f"data/gross_{yday_awst}.csv")
        else:
            gross_all = pd.DataFrame()
            print(f"WARN: No gross short rows for {yday_awst}. Continuing.")
    except Exception as e:
        print(f"ERROR: Unexpected gross short aggregation failure: {e}")
        gross_all = pd.DataFrame()

    # Score signals (function is defensive to empties)
    gross_sig = compute_gross_shorts_signals(gross_all, cfg=cfg)

    # Use safe top extractors (coerce-to-numeric + guard empties)
    gross_top_qty = _safe_top(
        gross_sig, "Gross", cfg.get("gross_shorts", {}).get("top_n", 25)
    )
    gross_top_pct = _safe_top(
        gross_sig, "PctGrossVsIssued", cfg.get("gross_shorts", {}).get("top_n", 25)
    )

    # ---------------------------
    # 2) SHORT POSITIONS (T+4)
    # ---------------------------
    try:
        asic_df, _, asic_date = fetch_asic_short_positions()
        save_csv(asic_df, f"data/asic_{asic_date}.csv")
    except Exception as e:
        print(f"ERROR: ASIC short positions fetch failed: {e}")
        asic_df = pd.DataFrame(columns=["Code", "ReportedShort", "Issued", "PctShort", "Date"])
        asic_date = today_awst().date()

    # Try to load previous ASIC day for Δ pp
    prev_df = None
    try:
        candidates = sorted([p for p in os.listdir("data") if p.startswith("asic_")])
        if len(candidates) >= 2:
            prev_df = pd.read_csv(os.path.join("data", candidates[-2]))
    except Exception as e:
        print(f"WARN: Could not load previous ASIC day for delta: {e}")

    pos_sig = compute_short_position_signals(asic_df, df_prev=prev_df, cfg=cfg)

    top_n_pos = cfg.get("short_positions", {}).get("top_n", 25)
    pos_high = _safe_top(pos_sig, "PctShort_pp", top_n_pos)
    pos_delta = _safe_top(pos_sig, "Delta_pp", top_n_pos)

    # ---------------------------
    # 3) Render site
    # ---------------------------
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

    # ---------------------------
    # 4) Notifications (optional)
    # ---------------------------
    try:
        from notify import maybe_notify
        maybe_notify(ctx, gross_sig, pos_sig)
    except Exception as e:
        print(f"WARN: Notification step failed: {e}")

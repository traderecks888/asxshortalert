# scripts/notify.py
# Safe to keep or remove. Pipeline will skip if env secrets are unset.

import os, requests

def _fmt_rows(rows, cols):
    def _fmt(v):
        if isinstance(v, float): return f"{v:.2f}"
        if isinstance(v, int):   return f"{v:,}"
        return "" if v is None else str(v)
    return "\n".join([" • " + " | ".join(_fmt(r.get(c)) for c in cols) for r in rows])

def notify_slack(webhook_url, title, text):
    if not webhook_url: return
    payload = {"text": f"*{title}*\n{text}"}
    requests.post(webhook_url, json=payload, timeout=15).raise_for_status()

def notify_ntfy(topic_url, title, text):
    if not topic_url: return
    requests.post(topic_url, data=text.encode("utf-8"),
                  headers={"Title": title}, timeout=15).raise_for_status()

def notify_telegram(bot_token, chat_id, title, text):
    if not (bot_token and chat_id): return
    base = f"https://api.telegram.org/bot{bot_token}"
    payload = {"chat_id": chat_id, "text": f"{title}\n{text}", "parse_mode": "HTML", "disable_web_page_preview": True}
    requests.post(f"{base}/sendMessage", json=payload, timeout=20).raise_for_status()

def maybe_notify(ctx, gross_df, pos_df):
    # Skip entirely if no secrets set
    if not (os.getenv("SLACK_WEBHOOK") or os.getenv("NTFY_TOPIC_URL") or (os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))):
        return

    gross_qty = []
    if gross_df is not None and hasattr(gross_df, "nlargest") and "Gross_num" in gross_df:
        gross_qty = gross_df.nlargest(5, "Gross_num")[["Code","Gross_num","PctGrossVsIssuedPct_num"]].to_dict(orient="records")

    pos_spike = []
    if pos_df is not None and hasattr(pos_df, "sort_values") and "Delta_pp_num" in pos_df:
        pos_spike = pos_df.sort_values("Delta_pp_num", ascending=False).head(5)[["Code","PctShort_pp_num","Delta_pp_num"]].to_dict(orient="records")

    pos_cover = []
    if pos_df is not None and "DeltaShares_num" in pos_df:
        pos_cover = pos_df.sort_values("DeltaShares_num", ascending=True).head(5)[["Code","PctShort_pp_num","DeltaShares_num"]].to_dict(orient="records")

    pos_dtc = []
    if pos_df is not None and "DaysToCover" in pos_df.columns:
        pos_dtc = pos_df.sort_values("DaysToCover", ascending=False).head(5)[["Code","PctShort_pp_num","DaysToCover"]].to_dict(orient="records")

    def fmt(rows, cols): return _fmt_rows(rows, cols) if rows else ""

    msg = "\n\n".join(filter(None, [
        f"Gross {ctx['gross_date']} (Top QTY)\n" + fmt(gross_qty, ["Code","Gross_num","PctGrossVsIssuedPct_num"]),
        f"ASIC {ctx['asic_date']} (Δ pp)\n" + fmt(pos_spike, ["Code","PctShort_pp_num","Delta_pp_num"]),
        f"ASIC {ctx['asic_date']} (Likely covering)\n" + fmt(pos_cover, ["Code","PctShort_pp_num","DeltaShares_num"]),
        f"ASIC {ctx['asic_date']} (High DTC)\n" + fmt(pos_dtc, ["Code","PctShort_pp_num","DaysToCover"]),
    ]))

    notify_slack(os.getenv("SLACK_WEBHOOK"), "ASX Shorts – Daily", msg)
    notify_ntfy(os.getenv("NTFY_TOPIC_URL"), "ASX Shorts – Daily", msg)
    notify_telegram(os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID"),
                    "ASX Shorts – Daily", msg)

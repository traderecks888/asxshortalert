import os, requests

def _fmt_rows(rows, cols):
    def _fmt(v):
        if isinstance(v,float): return f"{v:.2f}"
        if isinstance(v,int):   return f"{v:,}"
        return str(v)
    return "\n".join([" • " + " | ".join(_fmt(r.get(c)) for c in cols) for r in rows])

def notify_slack(webhook_url, title, text):
    payload = {"text": f"*{title}*\n{text}"}
    requests.post(webhook_url, json=payload, timeout=15).raise_for_status()

def notify_ntfy(topic_url, title, text):
    requests.post(topic_url, data=text.encode("utf-8"), headers={"Title": title}, timeout=15).raise_for_status()

def maybe_notify(ctx, gross_df, pos_df):
    gross_qty = gross_df.nlargest(5,"Gross")[
        ["Code","Gross","PctGrossVsIssued"]].to_dict(orient="records")
    pos_spike = pos_df.sort_values("Delta_pp", ascending=False).head(5)[
        ["Code","PctShort_pp","Delta_pp"]].to_dict(orient="records")
    msg = (
        f"Gross {ctx['gross_date']} (Top QTY)\n" +
        _fmt_rows(gross_qty, ["Code","Gross","PctGrossVsIssued"]) + "\n\n" +
        f"ASIC {ctx['asic_date']} (Δ pp)\n" +
        _fmt_rows(pos_spike, ["Code","PctShort_pp","Delta_pp"]) )
    slack = os.getenv("SLACK_WEBHOOK")
    ntfy  = os.getenv("NTFY_TOPIC_URL")
    if slack: notify_slack(slack, "ASX Shorts – Daily", msg)
    if ntfy:  notify_ntfy(ntfy, "ASX Shorts – Daily", msg)

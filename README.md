# ASX Shorts Alerts – MAX (DTC bands + Sector filters + Covering scores)

**What you get**
- Gross (T+1) with true % Issued.
- ASIC (T+4) with % short, Δpp, ShortedShares, ΔShares.
- **Days-to-Cover (DTC)** via Yahoo ADV; color bands in table.
- **Sector filter** + code search (client-side) across all tables.
- **Covering score (3d/5d)** from history.
- JSON API, history CSVs, charts.
- Notifications (Slack/Telegram/ntfy) are optional; skipped if secrets unset.

**Run**
1) Enable Pages (main/docs). 2) Run the workflow. 3) Extend `config/sectors.csv` as needed.

**Config**
`config/alerts.yml` tunes thresholds: pct_short_ge, pct_short_change_ge, abs_shares_cover_ge, adv_window_days, dtc_ge, etc.

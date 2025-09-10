# ASX Shorts Alerts (Serverless)

**Starter kit** to fetch ASIC Short Positions (T+4) + ASX/Cboe Gross Short Sales (T+1), score signals, publish a static dashboard, and push alerts via Slack/ntfy.

- Timezone: AWST (UTC+8). GH Actions cron set to 10:05 AWST.
- Config thresholds in `config/alerts.yml`.
- Optional universe lists in `config/universe_asx20.csv` and `config/universe_asx200.csv` (not required to run).

> Heads-up: ASX20/ASX200 membership changes quarterly. Verify lists each rebalance.
> This starter includes a **best-effort** ASX20 list as of 2025-09-10.
> ASX200 file is a scaffold — fill as needed for filtering/panels.

## Quickstart
1. Enable GitHub Pages to serve from `/docs`.
2. Add (optional) repo secrets for Slack or ntfy.
3. Run: `workflow_dispatch` or wait for the cron.

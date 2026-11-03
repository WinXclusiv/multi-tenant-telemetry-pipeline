# Postmortem: <short title>

<!--
Blameless: describe what systems and processes allowed this, not who did what wrong.
Use UTC times. Use real numbers from `scripts/slo_report.py` and the dashboard, not estimates.
Copy this file to docs/postmortems/YYYY-MM-DD-<slug>.md.
-->

| | |
| --- | --- |
| **Date** | YYYY-MM-DD |
| **Status** | Draft / In review / Final |
| **Severity** | Page / Ticket / Game day |
| **Authors** | |
| **Duration** | Start HH:MM UTC to end HH:MM UTC (N minutes) |
| **Detection** | Which alarm fired, and when, relative to the start |

## Summary

Two or three sentences: what broke, who was affected, how it was fixed.

## Impact

| Measure | Value |
| --- | --- |
| Events affected (late or failed) | |
| Events lost (never written) | |
| Tenants affected | |
| Freshness SLI during the incident | |
| Error budget consumed | % of the 28-day budget |
| Freshness p99 / max | |

## Timeline (UTC)

| Time | Event |
| --- | --- |
| HH:MM | Cause begins (e.g. deploy, injected fault) |
| HH:MM | First symptom visible on the dashboard |
| HH:MM | Alarm fires: `<alarm name>` |
| HH:MM | Responder acknowledges |
| HH:MM | Mitigation applied |
| HH:MM | Metrics back to normal |
| HH:MM | Replay or cleanup complete |

**Time to detect:** N min · **Time to mitigate:** N min · **Time to recover:** N min

## Root cause

What happened technically, and why. Use the "five whys" until you reach a cause in a system or process.

## What went well

-

## What went poorly

-

## Where we got lucky

-

## Action items

| Action | Type (prevent / detect / mitigate) | Owner | Ticket | Due |
| --- | --- | --- | --- | --- |
| | | | | |

## Supporting evidence

- SLO report table (paste from `scripts/slo_report.py`)
- Dashboard screenshot for the incident window
- Relevant log lines

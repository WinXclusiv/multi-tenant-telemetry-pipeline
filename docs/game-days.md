# Game days

A game day is a planned failure in the sandbox to prove three things: the alarm fires,
the runbook works, and the system recovers. Each one ends with a short write-up using the
[postmortem template](postmortems/TEMPLATE.md), filled in with **real numbers** from `scripts/slo_report.py`.

**Rules:** sandbox account only. Error budget above 50% (see [SLO.md](SLO.md)). One experiment at a time.
Note start and end times in UTC.

## Before every experiment

```bash
gh workflow run deploy.yml            # approve it, wait for the smoke test to pass
# Terminal 1: steady background traffic for the whole experiment
python scripts/load_test.py --stream fde-telemetry-stream --rate 100 --duration 1800
# Browser: the dashboard (terraform output -raw dashboard_url)
```

## Experiment 1: S3 outage (the important one)

| | |
| --- | --- |
| **Hypothesis** | When S3 writes fail, no data is lost. Failed writes burn the SLO, batches reach the DLQ after 3 retries, `PAGE-dlq-not-empty` and `PAGE-slo-fast-burn` fire, and replay recovers every record. |
| **Inject** | `python scripts/chaos.py s3-outage --minutes 10` (reverts automatically) |
| **Watch** | Errors graph: failed writes. DLQ depth. Alarm state widget. Time from injection to each alarm. |
| **Recover** | Follow [dlq-not-empty.md](runbooks/dlq-not-empty.md): dry run, then `--execute` |
| **Verify** | Athena: `count(DISTINCT id)` for the window equals the load test's `sent` count |
| **Record** | Time to detect, time to page, records replayed, records lost (should be 0), budget consumed |

## Experiment 2: Poison records

| | |
| --- | --- |
| **Hypothesis** | Malformed records are skipped without blocking the shard. Only `TICKET-malformed-records` fires, and the SLI is unaffected. |
| **Inject** | `python scripts/chaos.py poison --stream fde-telemetry-stream --count 25` |
| **Watch** | Malformed records > 0, consumer lag stays flat, no DLQ messages |
| **Record** | Whether good records around the poison ones were delayed (freshness p99) |

## Experiment 3: Over capacity

| | |
| --- | --- |
| **Hypothesis** | At 1.5× one shard's limit, producers are throttled, the load test's retries absorb it, no records are dropped, and `TICKET-producer-throttled` fires. |
| **Inject** | Stop the background load, then `python scripts/load_test.py --stream fde-telemetry-stream --rate 1500 --duration 180` |
| **Watch** | Saturation graph: producer throttles. Load test output: `throttled_attempts`, `dropped`, achieved rate. |
| **Then** | Reshard to 2 (see [producer-throttling.md](runbooks/producer-throttling.md)), rerun, and compare achieved rate. |
| **Record** | Achieved throughput at 1 and 2 shards: your real numbers for "how does it scale?" |

## Experiment 4: Hot tenant

| | |
| --- | --- |
| **Hypothesis** | With 2 shards, 90% of traffic from one tenant saturates one shard while the other sits idle. |
| **Inject** | `python scripts/load_test.py --stream fde-telemetry-stream --rate 1200 --duration 180 --hot-tenant tenant_A --hot-share 0.9` |
| **Record** | Throttling despite total capacity of 2,000/s. This is the evidence for the composite-key discussion. |

## After every experiment

```bash
python scripts/slo_report.py --minutes 45      # paste the table into the write-up
python scripts/chaos.py revert                  # belt and braces
gh workflow run destroy.yml -f confirm=destroy
```

Save the write-up as `docs/postmortems/YYYY-MM-DD-<experiment>.md`.

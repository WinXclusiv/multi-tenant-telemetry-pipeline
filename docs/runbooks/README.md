# Runbooks

Every alarm's description links to its runbook. Alarm names carry their severity.

| Alarm | Severity | Runbook |
| --- | --- | --- |
| `fde-telemetry-PAGE-slo-fast-burn` | Page | [slo-burn-rate.md](slo-burn-rate.md) |
| `fde-telemetry-TICKET-slo-slow-burn` | Ticket | [slo-burn-rate.md](slo-burn-rate.md) |
| `fde-telemetry-PAGE-consumer-lag` | Page | [consumer-lag.md](consumer-lag.md) |
| `fde-telemetry-PAGE-dlq-not-empty` | Page | [dlq-not-empty.md](dlq-not-empty.md) |
| `fde-telemetry-TICKET-consumer-errors` | Ticket | [consumer-errors.md](consumer-errors.md) |
| `fde-telemetry-TICKET-consumer-near-timeout` | Ticket | [consumer-errors.md](consumer-errors.md) |
| `fde-telemetry-TICKET-consumer-throttled` | Ticket | [consumer-errors.md](consumer-errors.md) |
| `fde-telemetry-TICKET-producer-throttled` | Ticket | [producer-throttling.md](producer-throttling.md) |
| `fde-telemetry-TICKET-malformed-records` | Ticket | [malformed-records.md](malformed-records.md) |

## First five minutes, any alert

1. **Acknowledge** and say in the incident channel that you are looking.
2. **Open the dashboard:** `terraform output -raw dashboard_url`. Read the SLO row, then the four
   golden-signal graphs from left to right: traffic, latency, errors, saturation.
3. **Check what changed:** the last Deploy workflow run, and `git log -5 main`.
   A recent deploy is the most likely cause. Rolling back is a valid first move.
4. **Decide the severity.** Is data at risk of loss (DLQ, records near 24 h retention)? That comes first.
5. **Write a timeline as you go** (UTC times). It becomes the postmortem.

## Shell setup used by every runbook

```bash
export AWS_REGION=us-east-1
P=fde-telemetry
FN=$P-consumer
STREAM=$P-stream
DLQ_URL=$(aws sqs get-queue-url --queue-name $P-consumer-dlq --query QueueUrl --output text)
```

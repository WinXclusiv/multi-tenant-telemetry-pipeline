# Service Level Objectives

The pipeline's job is to make tenant telemetry queryable quickly and without loss.
Users notice two things: whether recent data is there yet, and whether data goes missing.
The SLO measures the first directly. Loss is guarded by alarms that page.

## SLI: freshness

> **Good event:** an event written to the S3 data lake within **60 seconds** of its `event_ts`.
>
> **SLI** = good events ÷ (events written + events whose write failed)

| Term | Source | Emitted by |
| --- | --- | --- |
| Events written | `FdeTelemetry/EventsWritten` | Lambda, via Embedded Metric Format |
| Good events | `FdeTelemetry/EventsWithinSlo` | Lambda (written, and `now - event_ts <= 60 s`) |
| Failed writes | `FdeTelemetry/EventsFailedWrite` | Lambda (S3 write raised; the records are retried) |

Freshness is measured at the moment the S3 `PutObject` succeeds, because that is when Athena can
see the event. Partition projection means no crawler or repair step sits in between.

## SLO

| | |
| --- | --- |
| Objective | **99%** of events are good |
| Window | Rolling **28 days** |
| Error budget | 1% of events. At 1M events per 28 days, that is 10,000 late or failed events |

All three values are Terraform variables (`slo_target`, `freshness_slo_seconds`, `slo_window_days`).
The alert thresholds below are derived from them, so changing the SLO keeps the alerts consistent.

## Alerting: multi-window, multi-burn-rate

Burn rate = how fast the budget is being spent. A burn rate of 1 uses exactly the whole
budget over 28 days. Following the [Google SRE Workbook](https://sre.google/workbook/alerting-on-slos/),
each alert needs a long window (is it significant?) **and** a short window (is it still happening?),
so alerts fire fast and reset fast.

| Alert | Budget consumed | Long window | Short window | Burn-rate threshold (28-day window) | Severity |
| --- | --- | --- | --- | --- | --- |
| `PAGE-slo-fast-burn` | 2% | 1 h | 5 min | 13.44 | Page |
| `TICKET-slo-slow-burn` | 5% | 6 h | 30 min | 5.6 | Ticket |

Threshold = budget share × window hours ÷ long-window hours (for example 0.02 × 672 ÷ 1 = 13.44).
Each composite alarm fires only when both of its child alarms are in ALARM.

## Why there are also cause-based pages

An SLI built from the consumer's own metrics has a blind spot: **if the consumer stops,
the SLI goes silent instead of red.** Two alarms close that gap:

| Alarm | Catches | Why it pages |
| --- | --- | --- |
| `PAGE-consumer-lag` | IteratorAge > 60 s | The consumer is behind or stuck, so fresh data isn't arriving, even if nothing errors |
| `PAGE-dlq-not-empty` | Any batch exhausted its retries | For a Kinesis source, the DLQ holds pointers only. The records expire from the stream after 24 h unless replayed |

Everything else (Lambda errors, throttles, near-timeout, producer throttling, malformed records)
is a ticket. It needs attention, but retries and the alarms above protect users meanwhile.

## Known measurement gaps

- **Producer clock skew.** `event_ts` comes from the producer's clock. A producer clock running fast
  makes events look fresher than they are. Production fix: compare against Kinesis
  `ApproximateArrivalTimestamp` too.
- **Lost events never counted.** Events dropped before Kinesis (producer crash) or after DLQ expiry
  never enter the SLI. The DLQ page and replay procedure exist for the second case.
- **Retries inflate the denominator.** A failed write is counted once per attempt. This makes the
  SLI slightly pessimistic during incidents, which is the safe direction.
- **Malformed records are excluded.** They are a data-quality problem upstream, tracked by their own
  ticket alarm, and not a freshness failure of this pipeline.

## Error budget policy

| Budget remaining | What happens |
| --- | --- |
| > 50% | Ship normally. Game days and chaos experiments allowed. |
| 25-50% | Changes need a rollback plan in the PR. No chaos experiments. |
| < 25% | Feature changes pause. Work goes to the top reliability item from recent postmortems. |
| Exhausted | Only fixes and reliability work merge until the 28-day window recovers. |

Any incident that consumes more than 20% of the budget gets a postmortem
([template](postmortems/TEMPLATE.md)).

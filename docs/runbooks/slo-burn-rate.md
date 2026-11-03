# SLO burn rate (fast = page, slow = ticket)

**What it means:** more than 1% of recent events are arriving in the data lake later than 60 s,
or failing to write. Fast burn spends 2% of the 28-day budget in an hour; slow burn spends 5% in six hours.

**User impact:** tenant dashboards and Athena queries show incomplete recent data. Nothing is lost yet,
but failed writes that keep failing will reach the DLQ.

## Triage: is the problem "late" or "failing"?

On the dashboard **Errors** graph:

- **Failed S3 writes > 0** → the write path is broken. Go to [Failing writes](#failing-writes).
- **Failed writes = 0, but freshness p99 > 60 s** → events are succeeding, but slowly. Go to [Late events](#late-events).

## Failing writes

```bash
# What is the error?
aws logs tail /aws/lambda/$FN --since 15m --filter-pattern '"Write failed"' | head -40
```

| Error in logs | Likely cause | Fix |
| --- | --- | --- |
| `AccessDenied` | IAM change, or a leftover chaos policy | `python scripts/chaos.py revert`, then check the latest IAM diff in the Deploy run |
| `NoSuchBucket` | Bucket deleted or renamed | Re-apply Terraform from `main` |
| `SlowDown` / 503 | S3 request-rate throttling on a prefix | Usually transient. Retries handle it; if sustained, the batch size is too small |
| Timeouts | Regional S3 issue | Check the AWS Health Dashboard. Retries continue; watch the DLQ |

## Late events

Check in order:

1. **Consumer lag** (IteratorAge on the latency graph) high? Follow [consumer-lag.md](consumer-lag.md).
2. **Freshness high but lag low?** The events were already old when they reached Kinesis:
   producers are buffering or retrying, or their clock is wrong. Check one recent record:
   ```bash
   aws athena start-query-execution --work-group $P-wg --query-string \
     "SELECT tenant, max(from_iso8601_timestamp(event_ts)) AS newest FROM fde_telemetry_db.events
      WHERE dt = date_format(current_date, '%Y-%m-%d') GROUP BY tenant"
   ```
   If only one tenant is late, it is that tenant's producer. This is not a pipeline problem: tell the tenant.
3. **A DLQ replay in progress?** Replayed events keep their original `event_ts`, so they correctly count as late.
   The burn stops when the replay finishes.

## Resolve

The alert clears by itself once both windows drop below the threshold (the short window clears within minutes).
If the incident consumed more than 20% of the error budget (see the dashboard), open a postmortem.

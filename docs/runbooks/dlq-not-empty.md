# Dead-letter queue not empty (page)

**What it means:** one or more batches failed every retry (3 attempts with bisection) and were
skipped so the shard could move on.

**Why it pages:** for a Kinesis event source, each DLQ message contains only a **pointer**
(shard ID and sequence-number range), not the records. The records themselves are only in the
stream, and they **expire after 24 hours**. Replay them before that.

## 1. Stop the bleeding

Find out why the batches failed, or the replay will fail the same way:

```bash
aws logs tail /aws/lambda/$FN --since 1h --filter-pattern '?"Write failed" ?ERROR ?Task timed out' | head -50
```

Common causes: S3 permission or bucket problems (see [slo-burn-rate.md](slo-burn-rate.md#failing-writes)),
a bad deploy (roll back), or a leftover game-day policy (`python scripts/chaos.py revert`).

## 2. Inspect (dry run)

```bash
python scripts/replay_dlq.py --queue-url "$DLQ_URL" --dump-dir results/dlq
```

For each batch this prints `FOUND` with its record count, or `EXPIRED` if the records are already gone.
The NDJSON files in `results/dlq/` let you look at the actual records.

## 3. Replay

```bash
python scripts/replay_dlq.py --queue-url "$DLQ_URL" --execute
```

This re-publishes each batch's records to the stream with the same partition key, then deletes the
DLQ message. Expect:

- **Duplicates are possible.** Delivery is at-least-once, so query with `count(DISTINCT id)`.
- **The freshness SLI dips.** Replayed events keep their original `event_ts` and correctly count as late.

## 4. Record loss

If any batch printed `EXPIRED`, data was lost. The message is left in the queue as evidence.
Note the shard and sequence range in the postmortem. A postmortem is **required** for any data loss.

## Hardening ideas (for the postmortem's action items)

- Lambda can send failed Kinesis batches to an **S3 on-failure destination**, which includes the
  full payload, not just pointers. That removes the 24-hour race.
- Raise stream retention (up to 365 days) so there is more time to replay.

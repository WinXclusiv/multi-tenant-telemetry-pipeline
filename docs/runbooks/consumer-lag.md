# Consumer lag (page)

**What it means:** the Lambda consumer's IteratorAge is above 60 s. It is reading records that are
more than a minute old, so it is either too slow or stuck.

**Why this pages even when the SLO alert is quiet:** if the consumer stops completely, it emits no
SLI metrics at all. This alarm is the only thing that notices.

**Data at risk:** records expire from Kinesis after **24 hours**. Lag above about 20 hours is an emergency.

## Triage

```bash
# Is the event source mapping running?
aws lambda list-event-source-mappings --function-name $FN \
  --query 'EventSourceMappings[].{State:State,Reason:StateTransitionReason,LastResult:LastProcessingResult}'

# Is the function erroring or being throttled?
aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name Errors \
  --dimensions Name=FunctionName,Value=$FN --statistics Sum --period 300 \
  --start-time $(date -u -d '-30 min' +%FT%TZ) --end-time $(date -u +%FT%TZ)
aws logs tail /aws/lambda/$FN --since 15m | tail -50
```

| What you see | Cause | Fix |
| --- | --- | --- |
| Mapping `State` = `Disabled` | Someone disabled it, or a failed deploy | `aws lambda update-event-source-mapping --uuid <uuid> --enabled` |
| Errors high, same batch repeating | Crash on a batch (bisection and retries in progress) | Wait for retries (at most 3) to move the batch to the DLQ, then [dlq-not-empty.md](dlq-not-empty.md). If a deploy caused it, roll back |
| Throttles > 0 | Account concurrency exhausted by another function | Raise the account limit, or reserve concurrency for the consumer |
| No errors, lag rising steadily, traffic high | Throughput: incoming rate > processing rate | Raise `parallelization_factor` (up to 10) on the mapping, or add shards |
| Duration near 30 s timeout | Slow S3 writes or large batches | See [consumer-errors.md](consumer-errors.md) |

## Mitigation for throughput

```bash
# Process up to 4 batches per shard in parallel (per-tenant ordering is still kept,
# because ordering is per partition key)
aws lambda update-event-source-mapping --uuid <uuid> --parallelization-factor 4
```

Then make the change permanent in `main.tf` through a PR. Otherwise the next deploy reverts it.

## Resolve

Lag drains on its own once the cause is fixed. Watch IteratorAge fall to near zero.

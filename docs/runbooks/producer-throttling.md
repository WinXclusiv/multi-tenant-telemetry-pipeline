# Producer throttling (ticket)

**What it means:** producers are writing faster than the stream accepts. Each shard takes
**1 MB/s or 1,000 records/s**. Rejected records are retried by well-behaved producers (the load test
does this); a producer without retries loses them.

## Triage: whole stream, or one hot tenant?

```bash
aws kinesis describe-stream-summary --stream-name $STREAM \
  --query 'StreamDescriptionSummary.{Shards:OpenShardCount,Mode:StreamModeDetails.StreamMode}'
```

On the dashboard, compare **Traffic** (records/min ÷ 60 = records/s) against shards × 1,000.

- **Total rate near shards × 1,000** → the stream is undersized.
- **Total rate well below the limit** → one partition key (tenant) is hot and its shard is saturated.
  The **Events written per tenant** graph shows which one.

## Fixes

| Cause | Short term | Long term |
| --- | --- | --- |
| Undersized stream | `aws kinesis update-shard-count --stream-name $STREAM --target-shard-count 2 --scaling-type UNIFORM_SCALING` | Set `shard_count` in Terraform, or switch to on-demand mode for spiky traffic |
| Hot tenant | Ask the tenant to batch or slow down | Composite partition key for large tenants (`tenant#bucket`). This trades strict per-tenant ordering for spread |
| Many small records | Producers should use `PutRecords` (500 per call) | Aggregate records on the producer side |

A manual reshard drifts from Terraform. Follow it with a PR that sets the same `shard_count`.

# Malformed records (ticket)

**What it means:** the consumer received records it could not parse (invalid JSON, or missing
`tenant_id` or `event_ts`). It **skips** them on purpose: retrying can't fix bad data, and one poison
record must not block a shard. Those records are **not stored**.

## Triage

```bash
aws logs tail /aws/lambda/$FN --since 1h --filter-pattern '"Skipping malformed record"' | head -20
```

Each line shows the sequence number and the parse error.

- **A game day running?** `scripts/chaos.py poison` sends exactly this. Check the game-day log before escalating.
- **One producer or tenant?** A client shipped a schema change or a bug. Tell them, with the error text.
- **Every record?** A pipeline-side change broke parsing. Check the latest deploy and roll back.

## If the dropped records matter

They are still in the stream for 24 hours. Fetch and inspect them by sequence number:

```bash
aws kinesis get-shard-iterator --stream-name $STREAM --shard-id shardId-000000000000 \
  --shard-iterator-type AT_SEQUENCE_NUMBER --starting-sequence-number <seq from log>
```

## Longer term

- Validate on the producer side, before the stream.
- Add a quarantine prefix in S3 (`quarantine/dt=…/`) so malformed records are kept, not dropped.
- Publish a schema (JSON Schema, or the Glue Schema Registry) as the contract with producers.

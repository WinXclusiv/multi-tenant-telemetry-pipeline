# Consumer errors, near-timeout, or throttled (ticket)

These three alarms share a runbook because they share a cause: the Lambda consumer is unhealthy,
but retries and bisection are still protecting the data. If consumer lag or the DLQ alarm is also
firing, work those first.

## Consumer errors

The handler catches S3 write failures and malformed records itself, so `Errors` counts only
**crashes and timeouts**.

```bash
aws logs tail /aws/lambda/$FN --since 1h --filter-pattern '?ERROR ?Traceback ?"Task timed out"' | head -60
```

- **Traceback after a recent deploy** → roll back (re-run Deploy from the previous commit), then fix forward.
- **`Task timed out`** → see near-timeout below.
- **`Runtime.OutOfMemory`** → raise `memory_size` in `main.tf`.

## Near timeout (Duration p99 > 80% of 30 s)

Batch processing is slow. Usually S3 latency or a large batch.

- Check the **Saturation** graph. Did duration rise with traffic, or on its own?
- With traffic → lower `batch_size` or raise `parallelization_factor`.
- On its own → S3 latency; check the AWS Health Dashboard.
- Raising `timeout` is the last option: it hides the problem and slows retries.

## Throttled

Lambda refused to run the consumer because account concurrency is exhausted.

```bash
aws lambda get-account-settings --query 'AccountLimit.ConcurrentExecutions'
aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name ConcurrentExecutions \
  --statistics Maximum --period 300 \
  --start-time $(date -u -d '-1 hour' +%FT%TZ) --end-time $(date -u +%FT%TZ)
```

New AWS accounts can have a very low concurrency limit. Request an increase through Service Quotas,
or reserve concurrency for the consumer so other functions can't starve it.

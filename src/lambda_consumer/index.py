"""Kinesis -> S3 consumer.

Groups each batch by (tenant, UTC date) and writes one newline-delimited JSON
file per group, under Hive-style partitions that Athena partition projection
understands: events/tenant=<tenant>/dt=<YYYY-MM-DD>/<uuid>.json

Uses partial batch responses (ReportBatchItemFailures): if a group's S3 write
fails, only that group's sequence numbers are returned for retry.

Emits the pipeline's SLI metrics with CloudWatch Embedded Metric Format (EMF):
structured JSON log lines that CloudWatch turns into metrics asynchronously,
so there are no PutMetricData calls, no extra IAM permissions and no added latency.
"""

import base64
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_NAME"]
NAMESPACE = os.environ.get("METRICS_NAMESPACE", "FdeTelemetry")
PIPELINE = os.environ.get("PIPELINE_NAME", "fde-telemetry")
FRESHNESS_SLO_MS = int(os.environ.get("FRESHNESS_SLO_MS", "60000"))
EMF_MAX_VALUES = 100  # EMF accepts at most 100 values per metric per document

log = logging.getLogger()
log.setLevel(logging.INFO)


def _parse_ts(value):
    """Parse an ISO-8601 timestamp; naive values are treated as UTC."""
    ts = datetime.fromisoformat(value)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def _emit(metrics, dimensions=None, units=None):
    """Write one EMF document to stdout.

    print() is deliberate: the logging module prefixes each line with a level and
    request id, which stops CloudWatch from recognising the line as EMF JSON.
    """
    dimensions = {"Pipeline": PIPELINE, **(dimensions or {})}
    units = units or {}
    doc = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": NAMESPACE,
                    "Dimensions": [list(dimensions)],
                    "Metrics": [
                        {"Name": name, "Unit": units.get(name, "Count")} for name in metrics
                    ],
                }
            ],
        },
        **dimensions,
        **metrics,
    }
    print(json.dumps(doc))


def handler(event, context):
    groups = defaultdict(list)  # (tenant, dt) -> [(sequence_number, json_line, event_ms)]
    failures = []
    malformed = 0

    for rec in event["Records"]:
        seq = rec["kinesis"]["sequenceNumber"]
        try:
            data = json.loads(base64.b64decode(rec["kinesis"]["data"]))
            ts = _parse_ts(data["event_ts"])
            groups[(data["tenant_id"], ts.strftime("%Y-%m-%d"))].append(
                (seq, json.dumps(data), ts.timestamp() * 1000)
            )
        except (ValueError, KeyError, TypeError) as exc:
            # Poison pill: retrying won't fix malformed data, so log and skip
            malformed += 1
            log.warning("Skipping malformed record %s: %s", seq, exc)

    written = good = failed = 0
    freshness_ms = []
    per_tenant = defaultdict(int)

    for (tenant, dt), items in groups.items():
        key = f"events/tenant={tenant}/dt={dt}/{uuid.uuid4()}.json"
        body = "\n".join(line for _, line, _ in items) + "\n"
        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=body.encode(),
                ContentType="application/x-ndjson",
            )
        except Exception:
            log.exception("Write failed for %s", key)
            failures.extend({"itemIdentifier": seq} for seq, _, _ in items)
            failed += len(items)
            continue

        # Freshness SLI: time from the event happening to it being queryable in S3
        queryable_ms = time.time() * 1000
        for _, _, event_ms in items:
            lag = max(queryable_ms - event_ms, 0.0)
            freshness_ms.append(round(lag, 1))
            good += lag <= FRESHNESS_SLO_MS
        written += len(items)
        per_tenant[tenant] += len(items)

    _emit(
        {
            "RecordsReceived": len(event["Records"]),
            "RecordsMalformed": malformed,
            "EventsWritten": written,
            "EventsWithinSlo": good,
            "EventsFailedWrite": failed,
        }
    )
    for i in range(0, len(freshness_ms), EMF_MAX_VALUES):
        _emit(
            {"FreshnessMs": freshness_ms[i : i + EMF_MAX_VALUES]},
            units={"FreshnessMs": "Milliseconds"},
        )
    for tenant, count in per_tenant.items():
        _emit({"RecordsWritten": count}, dimensions={"Tenant": tenant})

    log.info(
        "records=%d written=%d within_slo=%d failed=%d malformed=%d files=%d",
        len(event["Records"]),
        written,
        good,
        failed,
        malformed,
        len(groups),
    )
    return {"batchItemFailures": failures}

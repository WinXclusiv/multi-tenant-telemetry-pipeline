"""Unit tests for the Lambda consumer, using moto to mock S3."""

import base64
import importlib
import json

import boto3
import pytest
from moto import mock_aws

BUCKET = "test-datalake"


def kinesis_record(seq, payload):
    raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return {"kinesis": {"sequenceNumber": str(seq), "data": base64.b64encode(raw).decode()}}


def event(tenant, ts="2026-10-23T14:05:00+00:00", **extra):
    return {
        "id": f"id-{tenant}-{ts}",
        "tenant_id": tenant,
        "event_ts": ts,
        "latency_ms": 12.5,
        "status_code": 200,
        **extra,
    }


@pytest.fixture
def consumer(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", BUCKET)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        boto3.client("s3").create_bucket(Bucket=BUCKET)
        import index  # module creates its boto3 client at import, so (re)load inside the mock

        yield importlib.reload(index)


def list_objects():
    s3 = boto3.client("s3")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])]
    return {k: s3.get_object(Bucket=BUCKET, Key=k)["Body"].read().decode() for k in keys}


def test_groups_batch_into_one_ndjson_file_per_tenant_and_day(consumer):
    records = [
        kinesis_record(1, event("tenant_A")),
        kinesis_record(2, event("tenant_A", ts="2026-10-23T23:59:59+00:00")),
        kinesis_record(3, event("tenant_B")),
        kinesis_record(4, event("tenant_A", ts="2026-10-24T00:00:01+00:00")),
    ]

    result = consumer.handler({"Records": records}, None)

    assert result == {"batchItemFailures": []}
    objects = list_objects()
    prefixes = sorted(k.rsplit("/", 1)[0] for k in objects)
    assert prefixes == [
        "events/tenant=tenant_A/dt=2026-10-23",
        "events/tenant=tenant_A/dt=2026-10-24",
        "events/tenant=tenant_B/dt=2026-10-23",
    ]
    tenant_a_day1 = next(v for k, v in objects.items() if "tenant_A/dt=2026-10-23" in k)
    lines = tenant_a_day1.strip().split("\n")
    assert len(lines) == 2
    assert all(json.loads(line)["tenant_id"] == "tenant_A" for line in lines)


def test_malformed_records_are_skipped_not_retried(consumer):
    records = [
        kinesis_record(1, b"not json"),
        kinesis_record(2, {"tenant_id": "tenant_A"}),  # missing event_ts
        kinesis_record(3, event("tenant_C")),
    ]

    result = consumer.handler({"Records": records}, None)

    assert result == {"batchItemFailures": []}
    assert len(list_objects()) == 1


def test_failed_write_reports_only_that_groups_sequence_numbers(consumer, monkeypatch):
    real_put = consumer.s3.put_object

    def flaky_put(**kwargs):
        if "tenant=tenant_B" in kwargs["Key"]:
            raise RuntimeError("simulated S3 outage")
        return real_put(**kwargs)

    monkeypatch.setattr(consumer.s3, "put_object", flaky_put)
    records = [
        kinesis_record(1, event("tenant_A")),
        kinesis_record(2, event("tenant_B")),
        kinesis_record(3, event("tenant_B", ts="2026-10-23T15:00:00+00:00")),
    ]

    result = consumer.handler({"Records": records}, None)

    failed = sorted(f["itemIdentifier"] for f in result["batchItemFailures"])
    assert failed == ["2", "3"]
    objects = list_objects()
    assert len(objects) == 1
    assert "tenant=tenant_A" in next(iter(objects))


def test_empty_batch(consumer):
    assert consumer.handler({"Records": []}, None) == {"batchItemFailures": []}


# ---------- SLI metrics (Embedded Metric Format) ----------


def emf_docs(captured):
    """Parse the EMF JSON documents the handler printed to stdout."""
    docs = []
    for line in captured.out.splitlines():
        line = line.strip()
        if line.startswith("{") and '"_aws"' in line:
            docs.append(json.loads(line))
    return docs


def test_emits_valid_emf_with_sli_counters(consumer, capsys):
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    fresh = (now - timedelta(seconds=5)).isoformat()
    stale = (now - timedelta(minutes=10)).isoformat()
    records = [
        kinesis_record(1, event("tenant_A", ts=fresh)),
        kinesis_record(2, event("tenant_A", ts=stale)),
        kinesis_record(3, event("tenant_B", ts=fresh)),
        kinesis_record(4, b"not json"),
    ]

    consumer.handler({"Records": records}, None)

    docs = emf_docs(capsys.readouterr())
    for d in docs:  # every document must satisfy the EMF spec
        directive = d["_aws"]["CloudWatchMetrics"][0]
        assert directive["Namespace"] == "FdeTelemetry"
        for dims in directive["Dimensions"]:
            assert all(name in d for name in dims)
        for m in directive["Metrics"]:
            assert m["Name"] in d

    counters = next(d for d in docs if "EventsWritten" in d)
    assert counters["RecordsReceived"] == 4
    assert counters["RecordsMalformed"] == 1
    assert counters["EventsWritten"] == 3
    assert counters["EventsWithinSlo"] == 2  # the 10-minute-old event breaches the 60 s SLO
    assert counters["EventsFailedWrite"] == 0

    freshness = next(d for d in docs if "FreshnessMs" in d)["FreshnessMs"]
    assert len(freshness) == 3
    assert max(freshness) > 9 * 60 * 1000

    tenants = {d["Tenant"]: d["RecordsWritten"] for d in docs if "Tenant" in d}
    assert tenants == {"tenant_A": 2, "tenant_B": 1}


def test_failed_writes_count_against_the_sli(consumer, capsys, monkeypatch):
    def broken_put(**kwargs):
        raise RuntimeError("simulated S3 outage")

    monkeypatch.setattr(consumer.s3, "put_object", broken_put)
    consumer.handler({"Records": [kinesis_record(1, event("tenant_A"))]}, None)

    counters = next(d for d in emf_docs(capsys.readouterr()) if "EventsWritten" in d)
    assert counters["EventsWritten"] == 0
    assert counters["EventsFailedWrite"] == 1


def test_non_utc_timestamps_partition_by_utc_date(consumer):
    # 22:30 in New York (UTC-4) is 02:30 the next day in UTC
    consumer.handler(
        {"Records": [kinesis_record(1, event("tenant_A", ts="2026-10-23T22:30:00-04:00"))]},
        None,
    )
    (key,) = list_objects()
    assert "/dt=2026-10-24/" in key


def test_freshness_values_are_chunked_to_emf_limit(consumer, capsys):
    records = [kinesis_record(i, event("tenant_A")) for i in range(250)]
    consumer.handler({"Records": records}, None)
    chunks = [d["FreshnessMs"] for d in emf_docs(capsys.readouterr()) if "FreshnessMs" in d]
    assert [len(c) for c in chunks] == [100, 100, 50]

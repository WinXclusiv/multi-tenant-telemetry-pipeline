"""Producer tests: event schema matches the Glue table, and events reach Kinesis."""

import json
from datetime import datetime

import boto3
from moto import mock_aws

from producer import TENANTS, make_event, send_events

GLUE_COLUMNS = {"id", "tenant_id", "event_ts", "latency_ms", "status_code"}


def test_event_matches_glue_schema():
    e = make_event()
    assert set(e) == GLUE_COLUMNS
    assert e["tenant_id"] in TENANTS
    assert datetime.fromisoformat(e["event_ts"]).utcoffset() is not None  # timezone-aware UTC
    assert isinstance(e["latency_ms"], float)
    assert e["status_code"] in {200, 201, 400, 500}


def test_send_events_writes_to_stream(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        kinesis = boto3.client("kinesis", region_name="us-east-1")
        kinesis.create_stream(StreamName="s", ShardCount=1)

        sent = send_events(kinesis, "s", count=5, rate=0, verbose=False)

        shard = kinesis.list_shards(StreamName="s")["Shards"][0]["ShardId"]
        it = kinesis.get_shard_iterator(
            StreamName="s", ShardId=shard, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        records = kinesis.get_records(ShardIterator=it)["Records"]
        assert [json.loads(r["Data"])["id"] for r in records] == sent

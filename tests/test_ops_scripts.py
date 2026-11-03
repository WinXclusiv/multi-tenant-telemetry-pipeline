"""Tests for the operational scripts (load test, DLQ replay, chaos, SLO report)."""

import json

import boto3
import chaos
import load_test
import pytest
import replay_dlq
import slo_report
from moto import mock_aws

STREAM = "fde-telemetry-stream"


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        yield


def read_all(kinesis, stream=STREAM):
    shard = kinesis.list_shards(StreamName=stream)["Shards"][0]["ShardId"]
    it = kinesis.get_shard_iterator(
        StreamName=stream, ShardId=shard, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]
    return kinesis.get_records(ShardIterator=it, Limit=10000)["Records"]


# ---------- load test ----------


def test_load_test_sends_target_volume_and_respects_hot_tenant(aws):
    kinesis = boto3.client("kinesis")
    kinesis.create_stream(StreamName=STREAM, ShardCount=1)

    result = load_test.run(
        kinesis,
        STREAM,
        rate=120,
        duration=3,
        hot_tenant="tenant_A",
        hot_share=1.0,
        sleep=lambda s: None,
    )

    assert result["sent"] == 360
    assert result["dropped"] == 0
    assert result["per_tenant"] == {"tenant_A": 360}
    assert len(read_all(kinesis)) == 360


def test_put_with_retries_retries_only_throttled_entries():
    calls = []

    class FakeKinesis:
        def put_records(self, StreamName, Records):
            calls.append(len(Records))
            if len(calls) == 1:  # first call: second record throttled
                return {
                    "FailedRecordCount": 1,
                    "Records": [
                        {"SequenceNumber": "1"},
                        {"ErrorCode": "ProvisionedThroughputExceededException"},
                    ],
                }
            return {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "2"}]}

    stats = load_test.Counter(sent=0, throttled_attempts=0, other_errors=0, dropped=0, api_calls=0)
    entries = [{"Data": b"a", "PartitionKey": "k"}, {"Data": b"b", "PartitionKey": "k"}]
    load_test.put_with_retries(FakeKinesis(), STREAM, entries, stats, sleep=lambda s: None)

    assert calls == [2, 1]
    assert stats["sent"] == 2 and stats["throttled_attempts"] == 1 and stats["dropped"] == 0


# ---------- DLQ replay ----------


def dlq_message(stream_arn, shard, start, end):
    return json.dumps(
        {
            "requestContext": {"condition": "RetryAttemptsExhausted"},
            "KinesisBatchInfo": {
                "shardId": shard,
                "startSequenceNumber": start,
                "endSequenceNumber": end,
                "batchSize": 3,
                "streamArn": stream_arn,
            },
        }
    )


@pytest.fixture
def stream_with_dlq(aws):
    kinesis, sqs = boto3.client("kinesis"), boto3.client("sqs")
    kinesis.create_stream(StreamName=STREAM, ShardCount=1)
    arn = kinesis.describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"][
        "StreamARN"
    ]
    seqs = [
        kinesis.put_record(
            StreamName=STREAM, Data=f'{{"n": {i}}}'.encode(), PartitionKey="tenant_A"
        )
        for i in range(5)
    ]
    queue = sqs.create_queue(QueueName="dlq")["QueueUrl"]
    # The failed batch is records 1..3
    sqs.send_message(
        QueueUrl=queue,
        MessageBody=dlq_message(
            arn, seqs[1]["ShardId"], seqs[1]["SequenceNumber"], seqs[3]["SequenceNumber"]
        ),
    )
    return kinesis, sqs, queue


def test_replay_dry_run_changes_nothing(stream_with_dlq):
    kinesis, sqs, queue = stream_with_dlq
    report = replay_dlq.process(sqs, kinesis, queue, execute=False)

    assert report["records_found"] == 3 and report["replayed"] == 0 and report["deleted"] == 0
    assert len(read_all(kinesis)) == 5


def test_replay_execute_republishes_exact_range_and_deletes_message(stream_with_dlq, tmp_path):
    kinesis, sqs, queue = stream_with_dlq
    report = replay_dlq.process(sqs, kinesis, queue, execute=True, dump_dir=tmp_path)

    assert report == {
        "messages": 1,
        "records_found": 3,
        "replayed": 3,
        "expired_batches": 0,
        "deleted": 1,
    }
    replayed = [json.loads(r["Data"])["n"] for r in read_all(kinesis)[5:]]
    assert replayed == [1, 2, 3]
    assert all(r["PartitionKey"] == "tenant_A" for r in read_all(kinesis)[5:])
    (dump,) = tmp_path.iterdir()
    assert len(dump.read_text().splitlines()) == 3
    attrs = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["All"])["Attributes"]
    assert attrs["ApproximateNumberOfMessages"] == "0"


# ---------- chaos ----------


def test_s3_outage_injects_and_always_reverts(aws):
    iam = boto3.client("iam")
    iam.create_role(RoleName="fde-telemetry-consumer-role", AssumeRolePolicyDocument="{}")
    seen = []

    def fake_sleep(seconds):
        seen.append(iam.list_role_policies(RoleName="fde-telemetry-consumer-role")["PolicyNames"])

    chaos.s3_outage(iam, "fde-telemetry", minutes=0, sleep=fake_sleep)

    assert seen == [[chaos.CHAOS_POLICY]]  # deny policy present during the outage
    assert iam.list_role_policies(RoleName="fde-telemetry-consumer-role")["PolicyNames"] == []
    assert chaos.revert(iam, "fde-telemetry") is False  # idempotent


# ---------- SLO report ----------


def test_slo_summary_math():
    values = {
        "good": [980, 10],
        "written": [995, 10],
        "failed": [5],
        "fresh_p99": [42000.0, 58000.0],
        "dlq_max": [0, 1],
    }
    s = slo_report.summarize(values, slo_target=0.99, slo_seconds=60)

    assert s["sli_pct"] == 98.02  # 990 good of 1,010 attempts
    assert s["slo_met"] is False
    assert s["burn_rate"] == 1.98
    assert s["freshness_p99_s"] == 58.0  # worst datapoint, not an average of percentiles
    assert s["dlq_max_batches"] == 1


def test_slo_summary_with_no_traffic():
    s = slo_report.summarize({}, slo_target=0.99, slo_seconds=60)
    assert s["sli_pct"] is None and s["burn_rate"] is None and s["slo_met"] is None


# ---------- account guard ----------


def test_account_guard_blocks_wrong_or_missing_account(aws, monkeypatch):
    import account_guard

    session = boto3.Session(region_name="us-east-1")  # moto's account is 123456789012

    monkeypatch.delenv("PIPELINE_AWS_ACCOUNT_ID", raising=False)
    with pytest.raises(SystemExit, match="set PIPELINE_AWS_ACCOUNT_ID"):
        account_guard.require_sandbox(session)

    monkeypatch.setenv("PIPELINE_AWS_ACCOUNT_ID", "999999999999")  # e.g. the prod account
    with pytest.raises(SystemExit, match="credentials are for account 123456789012"):
        account_guard.require_sandbox(session)

    monkeypatch.setenv("PIPELINE_AWS_ACCOUNT_ID", "123456789012")
    assert account_guard.require_sandbox(session) == "123456789012"

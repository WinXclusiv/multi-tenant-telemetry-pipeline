"""Recover batches that landed in the consumer's dead-letter queue.

For a Kinesis event source, each DLQ message holds only *pointers* (shard ID and
a sequence-number range), not the records. This script reads those records back
from the stream and, with --execute, re-publishes them so the (fixed) pipeline
processes them again. Records older than the stream's retention (24h) are gone.

Fix the root cause first, or the replayed records will fail again.

  DLQ_URL="$(terraform output -raw dlq_url)"
  python scripts/replay_dlq.py --queue-url "$DLQ_URL"                         # dry run
  python scripts/replay_dlq.py --queue-url "$DLQ_URL" --execute               # replay + delete
  python scripts/replay_dlq.py --queue-url "$DLQ_URL" --dump-dir results/dlq  # save records

Replayed events keep their original event_ts, so they count as late in the
freshness SLI. That is correct: they were late.
"""

import argparse
import json
import time
from pathlib import Path

import boto3
from account_guard import require_sandbox

MAX_BATCH = 500


def parse_message(body):
    info = json.loads(body)["KinesisBatchInfo"]
    return {
        "stream_name": info["streamArn"].split("/", 1)[1],
        "shard_id": info["shardId"],
        "start": int(info["startSequenceNumber"]),
        "end": int(info["endSequenceNumber"]),
        "batch_size": info.get("batchSize"),
    }


def fetch_range(kinesis, stream, shard_id, start, end, max_calls=1000, sleep=time.sleep):
    """Return records with start <= sequence number <= end, or [] if they have expired."""
    try:
        iterator = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType="AT_SEQUENCE_NUMBER",
            StartingSequenceNumber=str(start),
        )["ShardIterator"]
    except kinesis.exceptions.InvalidArgumentException:
        return []  # sequence number is past retention or invalid

    records = []
    for _ in range(max_calls):
        resp = kinesis.get_records(ShardIterator=iterator, Limit=1000)
        for r in resp["Records"]:
            seq = int(r["SequenceNumber"])
            if seq > end:
                return records
            if seq >= start:
                records.append(r)
        iterator = resp.get("NextShardIterator")
        if not iterator or (not resp["Records"] and resp.get("MillisBehindLatest", 0) == 0):
            return records
        if not resp["Records"]:
            sleep(0.2)  # stay under 5 GetRecords calls/s per shard
    return records


def republish(kinesis, stream, records, sleep=time.sleep):
    entries = [{"Data": r["Data"], "PartitionKey": r["PartitionKey"]} for r in records]
    for i in range(0, len(entries), MAX_BATCH):
        pending = entries[i : i + MAX_BATCH]
        for attempt in range(6):
            resp = kinesis.put_records(StreamName=stream, Records=pending)
            if resp.get("FailedRecordCount", 0) == 0:
                break
            pending = [
                e for e, res in zip(pending, resp["Records"], strict=True) if "ErrorCode" in res
            ]
            sleep(0.2 * 2**attempt)
        else:
            raise RuntimeError(f"{len(pending)} records could not be re-published")


def process(sqs, kinesis, queue_url, execute=False, dump_dir=None, max_messages=100):
    report = {"messages": 0, "records_found": 0, "replayed": 0, "expired_batches": 0, "deleted": 0}
    while report["messages"] < max_messages:
        msgs = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=120
        ).get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            report["messages"] += 1
            info = parse_message(m["Body"])
            records = fetch_range(
                kinesis, info["stream_name"], info["shard_id"], info["start"], info["end"]
            )
            report["records_found"] += len(records)
            label = f"{info['shard_id']} [{info['start']}..{info['end']}]"
            if not records:
                report["expired_batches"] += 1
                print(f"EXPIRED  {label}: records no longer in the stream; message kept")
                continue
            print(f"FOUND    {label}: {len(records)} records (batch size {info['batch_size']})")
            if dump_dir:
                d = Path(dump_dir)
                d.mkdir(parents=True, exist_ok=True)
                with open(d / f"{info['shard_id']}-{info['start']}.ndjson", "wb") as f:
                    for r in records:
                        f.write(r["Data"].rstrip(b"\n") + b"\n")
            if execute:
                republish(kinesis, info["stream_name"], records)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                report["replayed"] += len(records)
                report["deleted"] += 1
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--queue-url", required=True)
    p.add_argument("--execute", action="store_true", help="re-publish records and delete messages")
    p.add_argument("--dump-dir", help="also save each batch's records as NDJSON here")
    p.add_argument("--max-messages", type=int, default=100)
    p.add_argument("--region", default="us-east-1")
    args = p.parse_args()

    session = boto3.Session(region_name=args.region)
    require_sandbox(session)
    report = process(
        session.client("sqs"),
        session.client("kinesis"),
        args.queue_url,
        args.execute,
        args.dump_dir,
        args.max_messages,
    )
    if not args.execute:
        print("\nDry run: nothing re-published or deleted. Add --execute to replay.")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

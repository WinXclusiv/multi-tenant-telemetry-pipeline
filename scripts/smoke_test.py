"""End-to-end smoke test run by the deploy workflow after `terraform apply`.

1. Waits for the Kinesis -> Lambda event source mapping to be Enabled.
2. Sends small bursts of events through Kinesis.
3. Passes once new objects appear under events/ in the data lake bucket.

Exits non-zero (failing the workflow) if nothing lands before the timeout.
"""

import argparse
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from producer import send_events  # noqa: E402


def wait_for_mapping(lambda_client, uuid, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = lambda_client.get_event_source_mapping(UUID=uuid)["State"]
        print(f"Event source mapping state: {state}")
        if state == "Enabled":
            return
        time.sleep(5)
    raise SystemExit(f"FAIL: event source mapping not Enabled after {timeout}s")


def new_objects(s3, bucket, since):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix="events/")
    return [o["Key"] for o in resp.get("Contents", []) if o["LastModified"] >= since]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stream", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--mapping-uuid", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--timeout", type=int, default=180)
    args = p.parse_args()

    session = boto3.Session(region_name=args.region)
    kinesis, s3, lam = session.client("kinesis"), session.client("s3"), session.client("lambda")

    wait_for_mapping(lam, args.mapping_uuid, timeout=120)

    start = datetime.now(UTC).replace(microsecond=0)
    deadline = time.time() + args.timeout
    total_sent = 0
    while time.time() < deadline:
        total_sent += len(send_events(kinesis, args.stream, count=20, rate=0, verbose=False))
        time.sleep(10)
        keys = new_objects(s3, args.bucket, start)
        if keys:
            print(
                f"PASS: sent {total_sent} events, {len(keys)} new object(s) in s3://{args.bucket}/"
            )
            for k in keys[:5]:
                print(f"  {k}")
            return
        print(f"Sent {total_sent} events so far, no objects yet...")

    raise SystemExit(f"FAIL: nothing written to s3://{args.bucket}/events/ in {args.timeout}s")


if __name__ == "__main__":
    main()

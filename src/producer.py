"""Mock telemetry producer: sends JSON events to a Kinesis stream.

Usage:
    python src/producer.py --stream "$(terraform output -raw stream_name)" --count 500 --rate 20
"""

import argparse
import json
import random
import time
import uuid
from datetime import UTC, datetime

import boto3

TENANTS = ["tenant_A", "tenant_B", "tenant_C"]


def make_event(tenant_id=None):
    return {
        "id": str(uuid.uuid4()),
        "tenant_id": tenant_id or random.choice(TENANTS),
        "event_ts": datetime.now(UTC).isoformat(),
        "latency_ms": round(random.uniform(5.0, 150.0), 2),
        "status_code": random.choices([200, 201, 400, 500], weights=[70, 15, 10, 5])[0],
    }


def send_events(kinesis, stream, count, rate, verbose=True):
    """Send `count` events at roughly `rate` events/second. Returns the event ids sent."""
    sent = []
    for i in range(count):
        event = make_event()
        resp = kinesis.put_record(
            StreamName=stream,
            Data=json.dumps(event).encode(),
            PartitionKey=event["tenant_id"],  # per-tenant ordering (see README: hot shards)
        )
        sent.append(event["id"])
        if verbose and i % 50 == 0:
            print(f"[{i}/{count}] {resp['ShardId']} {event['tenant_id']} {event['status_code']}")
        if rate > 0:
            time.sleep(1 / rate)
    return sent


def main():
    p = argparse.ArgumentParser(description="Send mock telemetry to Kinesis")
    p.add_argument("--stream", required=True)
    p.add_argument("--count", type=int, default=500)
    p.add_argument("--rate", type=float, default=10.0, help="events per second")
    p.add_argument("--region", default="us-east-1")
    args = p.parse_args()

    kinesis = boto3.client("kinesis", region_name=args.region)
    sent = send_events(kinesis, args.stream, args.count, args.rate)
    print(f"Done: sent {len(sent)} events to {args.stream}")


if __name__ == "__main__":
    main()

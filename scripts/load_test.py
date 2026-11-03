"""Drive the pipeline at a target rate and report what actually happened.

Sends events with PutRecords (up to 500 per call), retries throttled records with
backoff, and writes a JSON result file you can cite in the README or a postmortem.

Examples:
  # Steady load well inside one shard's limit (1,000 records/s)
  python scripts/load_test.py --stream fde-telemetry-stream --rate 200 --duration 300

  # Over one shard's limit: expect WriteProvisionedThroughputExceeded and a ticket alarm
  python scripts/load_test.py --stream fde-telemetry-stream --rate 1500 --duration 120

  # Hot tenant: 90% of traffic from one partition key
  python scripts/load_test.py --stream fde-telemetry-stream --rate 300 \
      --hot-tenant tenant_A --hot-share 0.9

Then wait ~5 minutes for metrics and run:
  python scripts/slo_report.py --minutes 15
"""

import argparse
import json
import random
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from account_guard import require_sandbox  # noqa: E402

from producer import TENANTS, make_event  # noqa: E402

MAX_BATCH = 500  # PutRecords limit
MAX_ATTEMPTS = 5


def pick_tenant(hot_tenant, hot_share):
    if hot_tenant and random.random() < hot_share:
        return hot_tenant
    return random.choice(TENANTS)


def build_entries(count, hot_tenant=None, hot_share=0.0, malformed_share=0.0):
    entries, tenants = [], Counter()
    for _ in range(count):
        if malformed_share and random.random() < malformed_share:
            entries.append({"Data": b"{not valid json", "PartitionKey": "malformed"})
            tenants["(malformed)"] += 1
            continue
        event = make_event(pick_tenant(hot_tenant, hot_share))
        entries.append({"Data": json.dumps(event).encode(), "PartitionKey": event["tenant_id"]})
        tenants[event["tenant_id"]] += 1
    return entries, tenants


def put_with_retries(kinesis, stream, entries, stats, sleep=time.sleep):
    """PutRecords with per-record retry of throttled entries (exponential backoff)."""
    pending = entries
    for attempt in range(MAX_ATTEMPTS):
        resp = kinesis.put_records(StreamName=stream, Records=pending)
        stats["api_calls"] += 1
        if resp.get("FailedRecordCount", 0) == 0:
            stats["sent"] += len(pending)
            return
        retry = []
        for entry, result in zip(pending, resp["Records"], strict=True):
            if "ErrorCode" in result:
                stats["throttled_attempts"] += result["ErrorCode"] == (
                    "ProvisionedThroughputExceededException"
                )
                stats["other_errors"] += result["ErrorCode"] != (
                    "ProvisionedThroughputExceededException"
                )
                retry.append(entry)
            else:
                stats["sent"] += 1
        pending = retry
        sleep(min(0.1 * 2**attempt, 2.0) + random.uniform(0, 0.05))
    stats["dropped"] += len(pending)


def run(
    kinesis,
    stream,
    rate,
    duration,
    hot_tenant=None,
    hot_share=0.0,
    malformed_share=0.0,
    clock=time.monotonic,
    sleep=time.sleep,
):
    stats = Counter(sent=0, throttled_attempts=0, other_errors=0, dropped=0, api_calls=0)
    tenants = Counter()
    start = clock()
    for second in range(int(duration)):
        entries, t = build_entries(int(rate), hot_tenant, hot_share, malformed_share)
        tenants.update(t)
        for i in range(0, len(entries), MAX_BATCH):
            put_with_retries(kinesis, stream, entries[i : i + MAX_BATCH], stats, sleep)
        # Pace to one batch-of-a-second per wall-clock second
        behind = (start + second + 1) - clock()
        if behind > 0:
            sleep(behind)
        if second % 10 == 0:
            print(
                f"t={second:>4}s sent={stats['sent']:>8} "
                f"throttled={stats['throttled_attempts']:>6} dropped={stats['dropped']}",
                flush=True,
            )
    elapsed = max(clock() - start, 1e-9)
    return {
        "stream": stream,
        "target_rate_per_s": rate,
        "duration_s": duration,
        "elapsed_s": round(elapsed, 1),
        "achieved_rate_per_s": round(stats["sent"] / elapsed, 1),
        "hot_tenant": hot_tenant,
        "hot_share": hot_share,
        "malformed_share": malformed_share,
        **stats,
        "per_tenant": dict(tenants),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--stream", required=True)
    p.add_argument("--rate", type=float, default=200, help="target records per second")
    p.add_argument("--duration", type=int, default=300, help="seconds")
    p.add_argument("--hot-tenant", choices=TENANTS)
    p.add_argument(
        "--hot-share", type=float, default=0.0, help="0-1 share of traffic for the hot tenant"
    )
    p.add_argument(
        "--malformed-share", type=float, default=0.0, help="0-1 share of invalid records"
    )
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--out-dir", default="results")
    args = p.parse_args()

    started = datetime.now(UTC)
    print(f"Load test: {args.rate}/s for {args.duration}s into {args.stream}")
    print(f"Started {started:%H:%M:%S} UTC")
    session = boto3.Session(region_name=args.region)
    require_sandbox(session)
    result = run(
        session.client("kinesis"),
        args.stream,
        args.rate,
        args.duration,
        args.hot_tenant,
        args.hot_share,
        args.malformed_share,
    )
    result["started_utc"] = started.isoformat()
    result["ended_utc"] = datetime.now(UTC).isoformat()

    out = Path(args.out_dir)
    out.mkdir(exist_ok=True)
    path = out / f"load-test-{started:%Y%m%dT%H%M%SZ}.json"
    path.write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != "per_tenant"}, indent=2))
    print(f"\nSaved {path}. Wait ~5 minutes for metrics, then run:")
    print(f"  python scripts/slo_report.py --minutes {max(15, args.duration // 60 + 10)}")


if __name__ == "__main__":
    main()

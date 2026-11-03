"""Summarise the pipeline's SLIs from CloudWatch for a recent window.

Prints a Markdown table you can paste into the README, a game-day log or a
postmortem, and saves the same numbers as JSON.

  python scripts/slo_report.py --minutes 60
  python scripts/slo_report.py --minutes 1440 --project fde-telemetry
"""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
from account_guard import require_sandbox

NAMESPACE = "FdeTelemetry"


def queries(project, period):
    fn, stream, dlq = f"{project}-consumer", f"{project}-stream", f"{project}-consumer-dlq"

    def q(qid, ns, name, dims, stat):
        return {
            "Id": qid,
            "MetricStat": {
                "Metric": {
                    "Namespace": ns,
                    "MetricName": name,
                    "Dimensions": [{"Name": k, "Value": v} for k, v in dims.items()],
                },
                "Period": period,
                "Stat": stat,
            },
        }

    pipe = {"Pipeline": project}
    return [
        q("good", NAMESPACE, "EventsWithinSlo", pipe, "Sum"),
        q("written", NAMESPACE, "EventsWritten", pipe, "Sum"),
        q("failed", NAMESPACE, "EventsFailedWrite", pipe, "Sum"),
        q("malformed", NAMESPACE, "RecordsMalformed", pipe, "Sum"),
        q("fresh_p50", NAMESPACE, "FreshnessMs", pipe, "p50"),
        q("fresh_p99", NAMESPACE, "FreshnessMs", pipe, "p99"),
        q("fresh_max", NAMESPACE, "FreshnessMs", pipe, "Maximum"),
        q("incoming", "AWS/Kinesis", "IncomingRecords", {"StreamName": stream}, "Sum"),
        q(
            "throttled",
            "AWS/Kinesis",
            "WriteProvisionedThroughputExceeded",
            {"StreamName": stream},
            "Sum",
        ),
        q("lag_max", "AWS/Lambda", "IteratorAge", {"FunctionName": fn}, "Maximum"),
        q("errors", "AWS/Lambda", "Errors", {"FunctionName": fn}, "Sum"),
        q(
            "dlq_max",
            "AWS/SQS",
            "ApproximateNumberOfMessagesVisible",
            {"QueueName": dlq},
            "Maximum",
        ),
    ]


def summarize(values, slo_target, slo_seconds):
    """values: query id -> list of datapoint values for the window."""

    def total(k):
        return sum(values.get(k, []))

    def peak(k):
        return max(values.get(k, []), default=None)

    good, written, failed = total("good"), total("written"), total("failed")
    attempts = written + failed
    sli = good / attempts if attempts else None
    burn = (1 - sli) / (1 - slo_target) if sli is not None else None
    return {
        "events_in": int(total("incoming")),
        "events_written": int(written),
        "events_within_slo": int(good),
        "failed_writes": int(failed),
        "malformed_records": int(total("malformed")),
        "producer_throttles": int(total("throttled")),
        "lambda_errors": int(total("errors")),
        "sli_pct": round(sli * 100, 3) if sli is not None else None,
        "slo_pct": slo_target * 100,
        "slo_met": sli >= slo_target if sli is not None else None,
        "burn_rate": round(burn, 2) if burn is not None else None,
        # Percentiles can't be merged across datapoints; the worst one is the safe report
        "freshness_p50_s": _secs(peak("fresh_p50")),
        "freshness_p99_s": _secs(peak("fresh_p99")),
        "freshness_max_s": _secs(peak("fresh_max")),
        "freshness_slo_s": slo_seconds,
        "consumer_lag_max_s": _secs(peak("lag_max")),
        "dlq_max_batches": int(peak("dlq_max") or 0),
    }


def _secs(ms):
    return round(ms / 1000, 2) if ms is not None else None


def to_markdown(s, start, end):
    rows = [
        ("Window (UTC)", f"{start:%Y-%m-%d %H:%M} to {end:%H:%M}"),
        ("Events into Kinesis", f"{s['events_in']:,}"),
        ("Events written to S3", f"{s['events_written']:,}"),
        (
            "Freshness SLI",
            f"{s['sli_pct']}% (target {s['slo_pct']:g}%)"
            if s["sli_pct"] is not None
            else "no data",
        ),
        ("Burn rate", s["burn_rate"]),
        (
            "Freshness p50 / p99 / max",
            f"{s['freshness_p50_s']} s / {s['freshness_p99_s']} s / {s['freshness_max_s']} s",
        ),
        ("Max consumer lag", f"{s['consumer_lag_max_s']} s"),
        ("Failed writes / malformed", f"{s['failed_writes']} / {s['malformed_records']}"),
        ("Producer throttles", s["producer_throttles"]),
        ("Lambda errors", s["lambda_errors"]),
        ("DLQ peak (batches)", s["dlq_max_batches"]),
    ]
    lines = ["| Metric | Value |", "| --- | --- |"] + [f"| {k} | {v} |" for k, v in rows]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--minutes", type=int, default=60)
    p.add_argument("--project", default="fde-telemetry")
    p.add_argument("--slo-target", type=float, default=0.99)
    p.add_argument("--slo-seconds", type=int, default=60)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--out-dir", default="results")
    args = p.parse_args()

    end = datetime.now(UTC).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=args.minutes)
    period = args.minutes * 60

    session = boto3.Session(region_name=args.region)
    require_sandbox(session)
    cw = session.client("cloudwatch")
    values = {}
    paginator = cw.get_paginator("get_metric_data")
    for page in paginator.paginate(
        MetricDataQueries=queries(args.project, period), StartTime=start, EndTime=end
    ):
        for r in page["MetricDataResults"]:
            values.setdefault(r["Id"], []).extend(r["Values"])

    summary = summarize(values, args.slo_target, args.slo_seconds)
    print(to_markdown(summary, start, end))

    out = Path(args.out_dir)
    out.mkdir(exist_ok=True)
    path = out / f"slo-report-{end:%Y%m%dT%H%MZ}.json"
    path.write_text(
        json.dumps({"start": start.isoformat(), "end": end.isoformat(), **summary}, indent=2)
    )
    print(f"\nSaved {path}")


if __name__ == "__main__":
    main()

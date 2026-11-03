"""Controlled failure injection for game days (see docs/game-days.md).

Run with your own admin credentials against the SANDBOX account only.

  # Deny the consumer's S3 writes for 10 minutes, then restore automatically
  python scripts/chaos.py s3-outage --minutes 10

  # Safety net: remove the injected deny policy if a run was interrupted
  python scripts/chaos.py revert

  # Send records the consumer cannot parse
  python scripts/chaos.py poison --stream fde-telemetry-stream --count 25
"""

import argparse
import json
import sys
import time

import boto3
from account_guard import require_sandbox

CHAOS_POLICY = "chaos-deny-s3-put"


def role_name(project):
    return f"{project}-consumer-role"


def inject_s3_outage(iam, project):
    iam.put_role_policy(
        RoleName=role_name(project),
        PolicyName=CHAOS_POLICY,
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "GameDayS3Outage",
                        "Effect": "Deny",
                        "Action": "s3:PutObject",
                        "Resource": "*",
                    }
                ],
            }
        ),
    )


def revert(iam, project):
    try:
        iam.delete_role_policy(RoleName=role_name(project), PolicyName=CHAOS_POLICY)
        return True
    except iam.exceptions.NoSuchEntityException:
        return False


def s3_outage(iam, project, minutes, sleep=time.sleep):
    inject_s3_outage(iam, project)
    print(f"Injected: {CHAOS_POLICY} on {role_name(project)} (S3 writes denied).")
    print("IAM changes take up to a minute to apply. Watch the dashboard for:")
    print("  failed writes > 0, burn rate rising, DLQ depth > 0, then the PAGE alarms.")
    try:
        sleep(minutes * 60)
    except KeyboardInterrupt:
        print("\nInterrupted; reverting now.")
    finally:
        revert(iam, project)
        print(f"Reverted: {CHAOS_POLICY} removed. Next: follow docs/runbooks/dlq-not-empty.md")


def poison(kinesis, stream, count):
    entries = [{"Data": b"{this is not json", "PartitionKey": "chaos"} for _ in range(count)]
    resp = kinesis.put_records(StreamName=stream, Records=entries)
    return count - resp.get("FailedRecordCount", 0)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--project", default="fde-telemetry")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    sub = p.add_subparsers(dest="cmd", required=True)
    o = sub.add_parser("s3-outage")
    o.add_argument("--minutes", type=float, default=10)
    sub.add_parser("revert")
    po = sub.add_parser("poison")
    po.add_argument("--stream", required=True)
    po.add_argument("--count", type=int, default=25)
    args = p.parse_args()

    session = boto3.Session(region_name=args.region)
    account = require_sandbox(session)
    if args.cmd != "revert" and not args.yes:
        answer = input(
            f"Run '{args.cmd}' against AWS account {account}? Type the account ID to confirm: "
        )
        if answer.strip() != account:
            sys.exit("Aborted.")

    if args.cmd == "s3-outage":
        s3_outage(session.client("iam"), args.project, args.minutes)
    elif args.cmd == "revert":
        removed = revert(session.client("iam"), args.project)
        print("Removed the chaos policy." if removed else "No chaos policy was attached.")
    elif args.cmd == "poison":
        sent = poison(session.client("kinesis"), args.stream, args.count)
        print(f"Sent {sent} malformed records.")
        print("Expect the TICKET-malformed-records alarm within ~5 minutes.")


if __name__ == "__main__":
    main()

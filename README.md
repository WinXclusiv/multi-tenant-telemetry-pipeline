# Real-Time Multi-Tenant Telemetry Pipeline on AWS

A production-style streaming pipeline that ingests tenant telemetry events, lands them in a
partitioned S3 data lake, and makes them queryable with SQL within seconds. Everything is
defined in Terraform and shipped through a GitHub Actions pipeline that uses short-lived
OIDC credentials. No AWS keys are stored anywhere.

**Business framing:** a multi-tenant SaaS company needs per-tenant observability (latency
percentiles, error rates) without running servers or a warehouse. This design costs close
to nothing when idle apart from the stream, scales by adding shards, and keeps tenant data
physically partitioned for cheap, isolated queries.

## Architecture

```
 producer.py ──put_record──▶ Kinesis Data Stream ──Event Source Mapping──▶ Lambda consumer
 (PartitionKey=tenant_id)    (KMS-encrypted)        (batch ≤100 / 5 s)        │        │
                                                                              │        └─ failed after
                                                                  NDJSON per  │           retries ─▶ SQS DLQ
                                                                  tenant/day  ▼
 Analyst ──SQL──▶ Athena workgroup ◀──schema + partition projection── S3 data lake
                                        (Glue Data Catalog)            events/tenant=X/dt=YYYY-MM-DD/
```

| Hop | Mechanism | Why it matters |
|---|---|---|
| Producer → Kinesis | `PartitionKey = tenant_id` hashes to a shard | Per-tenant ordering; trade-off is a possible hot shard for a very large tenant |
| Kinesis → Lambda | AWS-managed Event Source Mapping polls each shard | No servers to run; batching amortizes invocation cost |
| Lambda → S3 | One NDJSON file per (tenant, day) per batch | Avoids the small-files problem and cuts S3 PUT cost |
| Failures | `ReportBatchItemFailures` + bisect + max 3 retries + SQS on-failure destination | A bad write retries only its own records; poison batches can't block a shard forever |
| S3 → Athena | Glue table with partition projection | No crawler, no `MSCK REPAIR`; queries filtered by `dt` scan only that day |
| IAM | Least-privilege role, capped by a permissions boundary | Even the CI deploy role cannot create a more powerful role |

Delivery is **at-least-once**, so a retried batch can produce duplicates. Deduplicate by `id`
in queries when exact counts matter.

## Repository layout

```
.
├── .github/workflows/
│   ├── ci.yml          # PR + main: lint, tests, fmt, validate, tflint, checkov; plan comment on PRs
│   ├── deploy.yml      # manual + approval: OIDC → plan → apply → end-to-end smoke test
│   └── destroy.yml     # manual (type "destroy") + nightly safety net
├── bootstrap/          # one-time: state bucket, OIDC provider, boundary, plan/deploy roles
├── src/
│   ├── producer.py
│   └── lambda_consumer/index.py
├── scripts/
│   ├── smoke_test.py   # post-deploy end-to-end check (used by deploy.yml)
│   ├── load_test.py    # drive load at a target rate; hot-tenant and malformed options
│   ├── slo_report.py   # SLI, burn rate and freshness percentiles for a window, as Markdown
│   ├── replay_dlq.py   # recover batches from the DLQ before they expire
│   └── chaos.py        # game-day fault injection (S3 outage, poison records)
├── docs/
│   ├── SLO.md          # SLI/SLO definitions, burn-rate alerting, error budget policy
│   ├── runbooks/       # one per alarm, linked from every alarm description
│   ├── game-days.md    # four failure experiments with hypotheses
│   ├── postmortems/TEMPLATE.md
│   └── adr/0001-observability-platform.md
├── tests/              # pytest + moto (mocked AWS)
├── main.tf  observability.tf  variables.tf  outputs.tf  backend.tf
├── .tflint.hcl  .checkov.yaml  pyproject.toml
└── requirements.txt  requirements-dev.txt
```

## CI/CD

| Workflow | Trigger | AWS role | What it does |
|---|---|---|---|
| **CI** | Pull request, push to `main` | Read-only `plan` role (PRs only) | Ruff lint and format, pytest, `terraform fmt`/`validate`, tflint, checkov, then `terraform plan` posted as a PR comment |
| **Deploy** | Manual, `main` only, needs approval | Scoped `deploy` role | Plan → apply → smoke test that sends events and confirms they land in S3 |
| **Destroy** | Manual (type `destroy`) or nightly 1 AM ET | Scoped `deploy` role | `terraform destroy`; no-op if nothing is deployed |

**Security model**

- GitHub issues a short-lived OIDC token per job. AWS exchanges it for 1-hour credentials.
- The `plan` role trusts only `repo:<owner>/multi-tenant-telemetry-pipeline:pull_request` and is read-only.
- The `deploy` role trusts only jobs running in the `sandbox` or `sandbox-teardown` GitHub
  Environments of this repo. It can only touch resources prefixed `fde-telemetry-`.
- Any IAM role the deploy role creates **must** carry the Lambda permissions boundary. Removing
  a boundary is explicitly denied. This blocks the classic "CI creates an admin role" escalation.
- Checkov runs on every PR. Each intentional exception is documented inline with
  `#checkov:skip=<ID>:<reason>` next to the resource it applies to.

## Observability and SRE

The pipeline has one SLO, measured from metrics the Lambda emits with
[Embedded Metric Format](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format.html)
(structured log lines CloudWatch turns into metrics, with no extra API calls or IAM permissions):

> **99% of events are queryable in S3 within 60 seconds of `event_ts`, over a rolling 28 days.**

| Signal | How it's covered |
| --- | --- |
| SLO burn | Multi-window, multi-burn-rate composite alarms: fast burn pages, slow burn tickets. Thresholds are derived from the SLO variables |
| Consumer stopped | `PAGE-consumer-lag` on IteratorAge. The SLI goes silent, not red, when the consumer stops, so this pages separately |
| Data at risk | `PAGE-dlq-not-empty`. DLQ messages are pointers; the records expire with stream retention |
| Causes | Tickets for Lambda errors, near-timeout, throttles, producer throttling, malformed records |
| Dashboard | SLI and error budget (28 days), burn rate, the four golden signals, per-tenant volume, alarm state |

Alerts route to two SNS topics (page, ticket) created by the bootstrap, so an email subscription is
confirmed once and survives nightly teardown. Every alarm description links to its runbook in
[`docs/runbooks/`](docs/runbooks/). The platform choice is explained in
[ADR 0001](docs/adr/0001-observability-platform.md) (CloudWatch; Datadog would reuse the same SLIs and runbooks).

**Proving it works:** [docs/game-days.md](docs/game-days.md) has four experiments (S3 outage, poison
records, over-capacity, hot tenant), each with a hypothesis and the scripts to run it.
Results go in `docs/postmortems/` using the [template](docs/postmortems/TEMPLATE.md).

```bash
python scripts/load_test.py --stream "$(terraform output -raw stream_name)" --rate 200 --duration 300
python scripts/slo_report.py --minutes 15        # Markdown table of SLI, burn rate, p50/p99 freshness
python scripts/chaos.py s3-outage --minutes 10   # then follow docs/runbooks/dlq-not-empty.md
```

## Working safely next to a production account

If the same machine has credentials for a production account, three guards keep this project
in the sandbox:

1. **Terraform** refuses to run unless the credentials belong to `aws_account_id`
   (`allowed_account_ids` in both providers). The check happens before any API call.
2. **The local scripts** (load test, replay, SLO report, chaos) refuse to run unless the caller's
   account matches `PIPELINE_AWS_ACCOUNT_ID`.
3. **No credentials are stored** in the repo, in VS Code, or in GitHub. Locally you select a named
   profile per terminal. CI uses short-lived OIDC credentials.

Set this up once in VS Code by creating `.vscode/settings.json` (gitignored). It names a profile
and an account ID, not a secret, and it applies only to terminals opened in this workspace:

```json
{
  "terminal.integrated.env.windows": {
    "AWS_PROFILE": "Practice",
    "PIPELINE_AWS_ACCOUNT_ID": "<sandbox account id>"
  }
}
```

Put your Terraform inputs in `bootstrap/terraform.tfvars` and `terraform.tfvars` (both gitignored):

```hcl
aws_account_id = "<sandbox account id>"
```

## One-time setup

**Prerequisites:** AWS CLI v2, Terraform ≥ 1.10, Python 3.12+, the GitHub CLI (`gh auth login`),
and admin credentials for a **sandbox** AWS account.

Environment protection rules and branch protection are free on **public** repos. Private repos
need GitHub Pro or Team. A public repo is the right choice for a portfolio anyway.

### 1. Create the GitHub repo and push

```bash
git init -b main
git add .
git commit -m "feat: streaming pipeline with terraform, ci/cd, and oidc bootstrap"
gh repo create multi-tenant-telemetry-pipeline --public --source=. --push
```

### 2. Bootstrap AWS (run once, locally)

```bash
cd bootstrap
terraform init
# bootstrap/terraform.tfvars (gitignored):
#   aws_account_id = "<sandbox account id>"
#   github_owner   = "<your-github-username>"
#   alert_email    = "<you@example.com>"
terraform apply
# If the account already has a GitHub OIDC provider, add: -var="create_oidc_provider=false"
```

AWS emails a confirmation link for each alert topic (page and ticket). Click both.

`github_owner` is case-sensitive and must match GitHub exactly. Bootstrap keeps local state;
its `terraform.tfstate` is gitignored, so keep it safe.

### 3. Point GitHub at the new roles

The bootstrap prints ready-made commands. Run them from the repo root:

```bash
terraform -chdir=bootstrap output -raw github_variables_commands | bash
```

These are repository *variables*, not secrets. Role ARNs and a bucket name aren't sensitive;
OIDC is what makes them safe to expose.

### 4. Create the two deployment environments

```bash
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
ME=$(gh api user -q .id)

# sandbox: required reviewer (you) + protected branches only
gh api -X PUT "repos/$REPO/environments/sandbox" --input - <<EOF
{"reviewers":[{"type":"User","id":$ME}],
 "deployment_branch_policy":{"protected_branches":true,"custom_branch_policies":false}}
EOF

# sandbox-teardown: no reviewer (so the nightly destroy isn't blocked), protected branches only
gh api -X PUT "repos/$REPO/environments/sandbox-teardown" --input - <<EOF
{"deployment_branch_policy":{"protected_branches":true,"custom_branch_policies":false}}
EOF
```

### 5. Protect `main` and require the CI checks

```bash
gh api -X PUT "repos/$REPO/branches/main/protection" --input - <<EOF
{"required_status_checks":{"strict":true,"contexts":[
   "Python lint & tests",
   "Terraform fmt, validate, lint, security scan",
   "Terraform plan"]},
 "enforce_admins":false,
 "required_pull_request_reviews":null,
 "restrictions":null}
EOF
```

From now on every change goes through a PR.

## Day-to-day workflow

```bash
git switch -c feat/my-change
# ...edit...
git commit -am "feat: describe the change"
git push -u origin feat/my-change
gh pr create --fill          # CI runs; the plan appears as a PR comment
gh pr merge --squash         # after checks pass
gh workflow run deploy.yml   # then approve it in the Actions tab
gh workflow run destroy.yml -f confirm=destroy   # when done
```

## Running locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
ruff check . && pytest -v

# Local deploys share the same remote state as CI
terraform init -backend-config="bucket=$(terraform -chdir=bootstrap output -raw state_bucket)"
terraform apply
python src/producer.py --stream "$(terraform output -raw stream_name)" --count 500 --rate 20
aws logs tail "/aws/lambda/$(terraform output -raw lambda_name)" --follow
```

If you already applied the stack earlier with local state, either run `terraform destroy`
first or move the state into S3 with `terraform init -migrate-state -backend-config=...`.

## Querying with Athena

Open the Athena console and select the `fde-telemetry-wg` workgroup and the
`fde_telemetry_db` database:

```sql
SELECT tenant,
       status_code,
       count(DISTINCT id)                  AS events,
       approx_percentile(latency_ms, 0.95) AS p95_latency_ms
FROM events
WHERE dt = date_format(current_date, '%Y-%m-%d')
GROUP BY tenant, status_code
ORDER BY tenant, status_code;
```

Partitions are in UTC, so late in the evening Eastern time today's data sits under tomorrow's date.

## Cost

| Resource | Cost driver | Notes |
|---|---|---|
| Kinesis (1 provisioned shard) | Shard-hours, billed even when idle | The main cost. Destroy after demos; the nightly destroy is a safety net |
| Lambda, SQS, S3, Glue | Per request / per GB | Effectively free at demo volume |
| CloudWatch alarms, dashboard, custom metrics | Per alarm metric, per composite alarm | Roughly $2/month if left deployed all month; the 9 custom metrics fit the free tier of 10 |
| Athena | Per TB scanned | Partition projection keeps scans to the filtered days |
| Remote state bucket | Storage | Pennies; old versions expire after 90 days |

## Design decisions and trade-offs

- **Partition key = `tenant_id`**: ordering per tenant, at the risk of a hot shard. The
  alternative (random key) spreads load evenly but loses ordering.
- **`TRIM_HORIZON` starting position**: the consumer processes everything still in the
  24-hour retention window, so events produced before deploy finishes aren't dropped.
- **Plan on PR, apply on demand** instead of auto-apply on merge: in a personal account,
  auto-apply would spin up a billable stream for every README fix. Client environments
  almost always gate changes behind a human approval anyway.
- **`ReadOnlyAccess` on CI roles**: covers the many Describe/List calls Terraform makes.
  Next step: generate a tighter policy from CloudTrail with IAM Access Analyzer.
- **Actions pinned to major tags**: the next hardening step is pinning to commit SHAs
  (with Dependabot to keep them current) to defend against a compromised action tag.

## Teardown

```bash
gh workflow run destroy.yml -f confirm=destroy
# To remove the CI/CD foundation as well (empty the state bucket first; it is versioned):
terraform -chdir=bootstrap destroy -var="github_owner=<your-github-username>"
```

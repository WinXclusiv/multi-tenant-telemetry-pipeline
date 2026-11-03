# ADR 0001: CloudWatch as the observability platform

**Status:** Accepted · **Date:** 2026-10-30

## Context

The pipeline needs metrics, SLO tracking, alerting and a dashboard. Candidates were Amazon CloudWatch,
Datadog, and Grafana Cloud. Constraints:

- It's a single-account AWS sandbox, destroyed nightly and redeployed through CI.
- Alerting must be defined in the same Terraform as the pipeline, reviewed in the same PR.
- Cost should be near zero at demo volume.
- Every AWS-native metric (Kinesis, Lambda, SQS) originates in CloudWatch whichever tool displays it.

## Decision

Use **CloudWatch** for everything: Embedded Metric Format for custom SLI metrics, metric and
composite alarms for burn-rate alerting, SNS for routing, and one CloudWatch dashboard.

## Options considered

| | CloudWatch | Datadog | Grafana Cloud |
| --- | --- | --- | --- |
| Where AWS metrics come from | Native | Pulled from CloudWatch by the AWS integration, or pushed with Metric Streams | Read from CloudWatch as a data source |
| Extra accounts and agents | None | Datadog account, API keys, AWS integration role, Lambda extension or forwarder | Grafana account, AWS data-source role |
| IaC | Same AWS provider | `datadog` Terraform provider | `grafana` Terraform provider |
| SLO features | Build it: metric math, composite alarms | First-class SLO objects, burn-rate monitors, error-budget UI | SLO plugin |
| Cost at this scale | Around $2/month while deployed (alarms, composites); custom metrics within the free 10 | Free tier keeps metrics 1 day; paid serverless monitoring is listed at $5-7 per active function per month | Free tier covers it |
| Multi-cloud, APM, log correlation | Limited | Strongest | Good |

## Consequences

- The SLO math is explicit and reviewable in `observability.tf`. That's useful for learning and for interviews.
- The CloudWatch SLO UI is weaker than Datadog's. The dashboard approximates it with metric-math widgets.
- **What carries over to Datadog unchanged:** the SLI definition, EMF metrics (Datadog ingests them
  through the AWS integration), the alert severities and burn-rate thresholds, runbooks, game days
  and postmortems. Only the alert and dashboard *definitions* would move to the `datadog` provider.

## When to revisit

- A client already standardises on Datadog, Grafana or New Relic. Use theirs; don't add a second pane of glass.
- Tracing across services becomes the main debugging need (Datadog APM, or OpenTelemetry to X-Ray or Grafana Tempo).
- More than a handful of services, or more than one cloud.

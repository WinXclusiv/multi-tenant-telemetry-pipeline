# =============================================================================
# Observability: SLO burn-rate alerts, cause-based alarms, and one dashboard.
#
# Alert philosophy (see docs/SLO.md):
#   PAGE   = users are being hurt now or data is at risk of loss
#   TICKET = needs attention within a working day
# Every alarm description links to its runbook in docs/runbooks/.
# =============================================================================

locals {
  error_budget = 1 - var.slo_target
  window_hours = var.slo_window_days * 24

  # Multi-window, multi-burn-rate thresholds (Google SRE Workbook, ch. 5):
  #   fast = 2% of the budget burned in 1 hour  -> page
  #   slow = 5% of the budget burned in 6 hours -> ticket
  # burn rate = (share of budget consumed) * (window hours) / (alert window hours)
  burn_fast = 0.02 * local.window_hours / 1
  burn_slow = 0.05 * local.window_hours / 6

  # Fixed-precision strings for use inside metric-math expressions and descriptions
  budget_s    = format("%.6f", local.error_budget)
  burn_fast_s = format("%.2f", local.burn_fast)
  burn_slow_s = format("%.2f", local.burn_slow)
  target_pct  = format("%g", var.slo_target * 100)

  page_actions   = var.page_topic_arn == "" ? [] : [var.page_topic_arn]
  ticket_actions = var.ticket_topic_arn == "" ? [] : [var.ticket_topic_arn]

  pipeline_dims = { Pipeline = var.project }
  runbooks      = trimsuffix(var.runbook_base_url, "/")

  # Burn-rate alarm windows: name => [period seconds, threshold]
  burn_windows = {
    "1h"  = [3600, local.burn_fast]
    "5m"  = [300, local.burn_fast]
    "6h"  = [21600, local.burn_slow]
    "30m" = [1800, local.burn_slow]
  }
}

# ---------- SLO burn-rate alarms ----------
# Bad events = written late (> freshness SLO) + failed writes.
# The child alarms have no actions; only the composites below notify.
resource "aws_cloudwatch_metric_alarm" "burn" {
  for_each = local.burn_windows

  alarm_name          = "${var.project}-slo-burn-${each.key}"
  alarm_description   = "Freshness SLO burn rate over ${each.key}. Child of a composite alarm; does not notify on its own."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = each.value[1]
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "burn"
    label       = "Burn rate (${each.key})"
    expression  = "IF(total > 0, (total - FILL(good, 0)) / total / ${local.budget_s}, 0)"
    return_data = true
  }

  metric_query {
    id         = "total"
    expression = "FILL(written, 0) + FILL(failed, 0)"
  }

  metric_query {
    id = "good"
    metric {
      namespace   = local.metrics_namespace
      metric_name = "EventsWithinSlo"
      dimensions  = local.pipeline_dims
      period      = each.value[0]
      stat        = "Sum"
    }
  }

  metric_query {
    id = "written"
    metric {
      namespace   = local.metrics_namespace
      metric_name = "EventsWritten"
      dimensions  = local.pipeline_dims
      period      = each.value[0]
      stat        = "Sum"
    }
  }

  metric_query {
    id = "failed"
    metric {
      namespace   = local.metrics_namespace
      metric_name = "EventsFailedWrite"
      dimensions  = local.pipeline_dims
      period      = each.value[0]
      stat        = "Sum"
    }
  }
}

resource "aws_cloudwatch_composite_alarm" "slo_fast_burn" {
  alarm_name        = "${var.project}-PAGE-slo-fast-burn"
  alarm_description = "Freshness SLO is burning error budget fast (>= ${local.burn_fast_s}x over 1h and 5m): 2% of the ${var.slo_window_days}-day budget gone in an hour. Runbook: ${local.runbooks}/slo-burn-rate.md"
  alarm_rule        = "ALARM(\"${aws_cloudwatch_metric_alarm.burn["1h"].alarm_name}\") AND ALARM(\"${aws_cloudwatch_metric_alarm.burn["5m"].alarm_name}\")"
  alarm_actions     = local.page_actions
  ok_actions        = local.page_actions
}

resource "aws_cloudwatch_composite_alarm" "slo_slow_burn" {
  alarm_name        = "${var.project}-TICKET-slo-slow-burn"
  alarm_description = "Freshness SLO is burning error budget steadily (>= ${local.burn_slow_s}x over 6h and 30m): 5% of the budget gone in 6 hours. Runbook: ${local.runbooks}/slo-burn-rate.md"
  alarm_rule        = "ALARM(\"${aws_cloudwatch_metric_alarm.burn["6h"].alarm_name}\") AND ALARM(\"${aws_cloudwatch_metric_alarm.burn["30m"].alarm_name}\")"
  alarm_actions     = local.ticket_actions
  ok_actions        = local.ticket_actions
}

# ---------- Cause-based alarms ----------

# The SLI only counts events that reach the consumer. If the consumer stops
# entirely, the SLI goes silent, so consumer lag must page on its own.
resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${var.project}-PAGE-consumer-lag"
  alarm_description   = "Lambda is more than ${var.freshness_slo_seconds}s behind the stream (IteratorAge). Data freshness is degrading even if nothing is erroring. Runbook: ${local.runbooks}/consumer-lag.md"
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 3
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.freshness_slo_seconds * 1000
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.page_actions
  ok_actions          = local.page_actions
}

# For a Kinesis source the DLQ holds pointers, not payloads: the records
# themselves expire with stream retention (24h). That is why this pages.
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.project}-PAGE-dlq-not-empty"
  alarm_description   = "Batches exhausted their retries. Their records expire from Kinesis after ${aws_kinesis_stream.telemetry.retention_period}h unless replayed. Runbook: ${local.runbooks}/dlq-not-empty.md"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.consumer_dlq.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.page_actions
  ok_actions          = local.page_actions
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project}-TICKET-consumer-errors"
  alarm_description   = "Consumer invocations are crashing or timing out (S3 write failures are handled and do not count here). Runbook: ${local.runbooks}/consumer-errors.md"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ticket_actions
  ok_actions          = local.ticket_actions
}

resource "aws_cloudwatch_metric_alarm" "lambda_duration" {
  alarm_name          = "${var.project}-TICKET-consumer-near-timeout"
  alarm_description   = "Consumer invocations are using over 80% of the ${aws_lambda_function.consumer.timeout}s timeout. Runbook: ${local.runbooks}/consumer-errors.md"
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  extended_statistic  = "p99"
  period              = 300
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  comparison_operator = "GreaterThanThreshold"
  threshold           = aws_lambda_function.consumer.timeout * 1000 * 0.8
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ticket_actions
  ok_actions          = local.ticket_actions
}

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  alarm_name          = "${var.project}-TICKET-consumer-throttled"
  alarm_description   = "Lambda is throttling the consumer (account or reserved concurrency limit). Runbook: ${local.runbooks}/consumer-errors.md"
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ticket_actions
  ok_actions          = local.ticket_actions
}

resource "aws_cloudwatch_metric_alarm" "producer_throttled" {
  alarm_name          = "${var.project}-TICKET-producer-throttled"
  alarm_description   = "Producers are exceeding shard write capacity (1 MB/s or 1,000 records/s per shard). Runbook: ${local.runbooks}/producer-throttling.md"
  namespace           = "AWS/Kinesis"
  metric_name         = "WriteProvisionedThroughputExceeded"
  dimensions          = { StreamName = aws_kinesis_stream.telemetry.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ticket_actions
  ok_actions          = local.ticket_actions
}

resource "aws_cloudwatch_metric_alarm" "malformed_records" {
  alarm_name          = "${var.project}-TICKET-malformed-records"
  alarm_description   = "Producers are sending records the consumer cannot parse; they are skipped, not stored. Runbook: ${local.runbooks}/malformed-records.md"
  namespace           = local.metrics_namespace
  metric_name         = "RecordsMalformed"
  dimensions          = local.pipeline_dims
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ticket_actions
  ok_actions          = local.ticket_actions
}

# ---------- Dashboard ----------

locals {
  ns     = local.metrics_namespace
  fn     = aws_lambda_function.consumer.function_name
  stream = aws_kinesis_stream.telemetry.name
  dlq    = aws_sqs_queue.consumer_dlq.name

  # Hidden building blocks for SLI math, reused across widgets
  sli_inputs = [
    [local.ns, "EventsWithinSlo", "Pipeline", var.project, { id = "good", stat = "Sum", visible = false }],
    [local.ns, "EventsWritten", "Pipeline", var.project, { id = "written", stat = "Sum", visible = false }],
    [local.ns, "EventsFailedWrite", "Pipeline", var.project, { id = "failed", stat = "Sum", visible = false }],
  ]
  total_expr = "(FILL(written, 0) + FILL(failed, 0))"

  all_alarm_arns = concat(
    [aws_cloudwatch_composite_alarm.slo_fast_burn.arn, aws_cloudwatch_composite_alarm.slo_slow_burn.arn],
    [
      aws_cloudwatch_metric_alarm.iterator_age.arn,
      aws_cloudwatch_metric_alarm.dlq_not_empty.arn,
      aws_cloudwatch_metric_alarm.lambda_errors.arn,
      aws_cloudwatch_metric_alarm.lambda_duration.arn,
      aws_cloudwatch_metric_alarm.lambda_throttles.arn,
      aws_cloudwatch_metric_alarm.producer_throttled.arn,
      aws_cloudwatch_metric_alarm.malformed_records.arn,
    ],
  )
}

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${var.project}-pipeline"
  dashboard_body = jsonencode({
    start          = "-PT3H"
    periodOverride = "inherit"
    widgets = [
      {
        type = "text", x = 0, y = 0, width = 24, height = 2
        properties = {
          markdown = "## ${var.project} pipeline\n**SLO:** ${local.target_pct}% of events queryable within ${var.freshness_slo_seconds}s of `event_ts`, rolling ${var.slo_window_days} days. **Pages:** fast burn, consumer lag, DLQ not empty. [Runbooks](${local.runbooks}) · [SLO definition](${replace(local.runbooks, "/runbooks", "")}/SLO.md)"
        }
      },

      # --- Row 1: the SLO ---
      {
        type = "metric", x = 0, y = 2, width = 6, height = 5
        properties = {
          title                = "Freshness SLI, ${var.slo_window_days} days (target ${local.target_pct}%)"
          view                 = "singleValue"
          region               = var.region
          start                = "-P${var.slo_window_days}D"
          setPeriodToTimeRange = true
          metrics = concat(local.sli_inputs, [
            [{ expression = "100 * FILL(good, 0) / ${local.total_expr}", label = "Good events (%)", id = "sli" }],
          ])
        }
      },
      {
        type = "metric", x = 6, y = 2, width = 6, height = 5
        properties = {
          title                = "Error budget remaining, ${var.slo_window_days} days"
          view                 = "singleValue"
          region               = var.region
          start                = "-P${var.slo_window_days}D"
          setPeriodToTimeRange = true
          metrics = concat(local.sli_inputs, [
            [{ expression = "100 * (1 - ((${local.total_expr} - FILL(good, 0)) / ${local.total_expr}) / ${local.budget_s})", label = "Budget left (%)", id = "budget" }],
          ])
        }
      },
      {
        type = "metric", x = 12, y = 2, width = 12, height = 5
        properties = {
          title  = "Burn rate, 1-hour windows (1.0 = spending budget exactly on schedule)"
          view   = "timeSeries"
          region = var.region
          start  = "-P2D"
          period = 3600
          metrics = concat(local.sli_inputs, [
            [{ expression = "IF(${local.total_expr} > 0, (${local.total_expr} - FILL(good, 0)) / ${local.total_expr} / ${local.budget_s}, 0)", label = "Burn rate", id = "burn" }],
          ])
          annotations = {
            horizontal = [
              { label = "Page (1h)", value = local.burn_fast },
              { label = "Ticket (6h)", value = local.burn_slow },
            ]
          }
        }
      },

      # --- Row 2: golden signals ---
      {
        type = "metric", x = 0, y = 7, width = 6, height = 6
        properties = {
          title  = "Traffic (records per minute)"
          view   = "timeSeries"
          region = var.region
          period = 60
          stat   = "Sum"
          metrics = [
            ["AWS/Kinesis", "IncomingRecords", "StreamName", local.stream, { label = "Into Kinesis" }],
            [local.ns, "EventsWritten", "Pipeline", var.project, { label = "Written to S3" }],
          ]
        }
      },
      {
        type = "metric", x = 6, y = 7, width = 6, height = 6
        properties = {
          title  = "Latency: freshness and consumer lag (ms)"
          view   = "timeSeries"
          region = var.region
          period = 60
          metrics = [
            [local.ns, "FreshnessMs", "Pipeline", var.project, { stat = "p50", label = "Freshness p50" }],
            [local.ns, "FreshnessMs", "Pipeline", var.project, { stat = "p99", label = "Freshness p99" }],
            ["AWS/Lambda", "IteratorAge", "FunctionName", local.fn, { stat = "Maximum", label = "Consumer lag (IteratorAge max)" }],
          ]
          annotations = {
            horizontal = [{ label = "SLO threshold", value = var.freshness_slo_seconds * 1000 }]
          }
        }
      },
      {
        type = "metric", x = 12, y = 7, width = 6, height = 6
        properties = {
          title  = "Errors"
          view   = "timeSeries"
          region = var.region
          period = 300
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", local.fn, { stat = "Sum", label = "Lambda errors" }],
            [local.ns, "EventsFailedWrite", "Pipeline", var.project, { stat = "Sum", label = "Failed S3 writes (events)" }],
            [local.ns, "RecordsMalformed", "Pipeline", var.project, { stat = "Sum", label = "Malformed records" }],
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", local.dlq, { stat = "Maximum", label = "DLQ depth (batches)", yAxis = "right" }],
          ]
        }
      },
      {
        type = "metric", x = 18, y = 7, width = 6, height = 6
        properties = {
          title  = "Saturation"
          view   = "timeSeries"
          region = var.region
          period = 300
          metrics = [
            ["AWS/Kinesis", "WriteProvisionedThroughputExceeded", "StreamName", local.stream, { stat = "Sum", label = "Producer throttles" }],
            ["AWS/Lambda", "Throttles", "FunctionName", local.fn, { stat = "Sum", label = "Lambda throttles" }],
            ["AWS/Lambda", "Duration", "FunctionName", local.fn, { stat = "p99", label = "Duration p99 (ms)", yAxis = "right" }],
          ]
          annotations = {
            horizontal = [{ label = "Lambda timeout", value = aws_lambda_function.consumer.timeout * 1000, yAxis = "right" }]
          }
        }
      },

      # --- Row 3: per tenant and alarm state ---
      {
        type = "metric", x = 0, y = 13, width = 12, height = 6
        properties = {
          title  = "Events written per tenant (per minute)"
          view   = "timeSeries"
          region = var.region
          period = 60
          metrics = [
            [{ expression = "SEARCH('{${local.ns},Pipeline,Tenant} MetricName=\"RecordsWritten\" Pipeline=\"${var.project}\"', 'Sum', 60)", id = "tenants" }],
          ]
        }
      },
      {
        type = "alarm", x = 12, y = 13, width = 12, height = 6
        properties = {
          title  = "Alarm state"
          alarms = local.all_alarm_arns
          sortBy = "stateUpdatedTimestamp"
        }
      },
    ]
  })
}

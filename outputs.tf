output "stream_name" {
  value = aws_kinesis_stream.telemetry.name
}

output "bucket_name" {
  value = aws_s3_bucket.datalake.bucket
}

output "lambda_name" {
  value = aws_lambda_function.consumer.function_name
}

output "event_source_mapping_uuid" {
  value = aws_lambda_event_source_mapping.kinesis.uuid
}

output "athena_workgroup" {
  value = aws_athena_workgroup.analytics.name
}

output "glue_database" {
  value = aws_glue_catalog_database.telemetry.name
}

output "dlq_url" {
  value = aws_sqs_queue.consumer_dlq.url
}

output "dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards/dashboard/${aws_cloudwatch_dashboard.pipeline.dashboard_name}"
}

output "slo_burn_thresholds" {
  description = "Derived multi-window burn-rate thresholds (fast pages, slow tickets)"
  value       = { fast = local.burn_fast_s, slow = local.burn_slow_s }
}

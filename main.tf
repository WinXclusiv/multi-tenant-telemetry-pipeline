terraform {
  required_version = ">= 1.10" # needed for S3-native state locking (use_lockfile)
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 6.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
    random  = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

provider "aws" {
  region = var.region

  # Safety guard: fail before any API call if the credentials belong to another account
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = { Project = var.project, ManagedBy = "terraform" }
  }
}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  bucket_name   = "${var.project}-datalake-${random_id.suffix.hex}"
  function_name = "${var.project}-consumer"

  # Custom SLI metrics are emitted by the Lambda via Embedded Metric Format
  metrics_namespace = "FdeTelemetry"
}

# ---------- Ingestion: Kinesis ----------
resource "aws_kinesis_stream" "telemetry" {
  name             = "${var.project}-stream"
  shard_count      = var.shard_count
  retention_period = 24

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis" # AWS-managed key: no extra KMS permissions needed
  #checkov:skip=CKV_AWS_185:AWS-managed key is sufficient for demo telemetry; a CMK adds cost and key-policy management

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

# ---------- Storage: S3 data lake ----------
resource "aws_s3_bucket" "datalake" {
  bucket        = local.bucket_name
  force_destroy = true
  #checkov:skip=CKV_AWS_21:Append-only data with unique (uuid) keys; versioning would only add cost
  #checkov:skip=CKV_AWS_18:Access logging needs a second bucket; CloudTrail S3 data events are the audit path if required
  #checkov:skip=CKV_AWS_144:Cross-region replication is out of scope for a single-region sandbox
  #checkov:skip=CKV_AWS_145:SSE-S3 (AES-256) is enabled; a KMS CMK adds per-request cost without a compliance driver here
  #checkov:skip=CKV2_AWS_62:No downstream consumer of S3 events in this design
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# ---------- Compute: Lambda ----------
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = 7
  #checkov:skip=CKV_AWS_338:7-day retention keeps sandbox costs down; raise for production
  #checkov:skip=CKV_AWS_158:Logs contain no sensitive data; default CloudWatch encryption applies
}

# Records that still fail after retries land here, with shard + sequence metadata,
# instead of blocking the shard forever.
resource "aws_sqs_queue" "consumer_dlq" {
  name                      = "${local.function_name}-dlq"
  message_retention_seconds = 1209600 # 14 days, the SQS maximum
  sqs_managed_sse_enabled   = true
}

resource "aws_iam_role" "lambda" {
  name                 = "${local.function_name}-role"
  permissions_boundary = var.lambda_permissions_boundary_arn
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "least-privilege"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadStream"
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords", "kinesis:GetShardIterator",
          "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.telemetry.arn
      },
      {
        Sid      = "ListStreams"
        Effect   = "Allow"
        Action   = ["kinesis:ListStreams"]
        Resource = "*"
      },
      {
        Sid      = "WriteEvents"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.datalake.arn}/events/*"
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.lambda.arn}:*"
      },
      {
        Sid      = "FailedRecordsToDlq"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.consumer_dlq.arn
      },
      {
        Sid      = "XRayTracing"
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
        Resource = "*"
      }
    ]
  })
}

data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/src/lambda_consumer"
  output_path = "${path.module}/build/lambda_consumer.zip"
  excludes    = ["__pycache__"]
}

resource "aws_lambda_function" "consumer" {
  function_name    = local.function_name
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "index.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 30
  memory_size      = 256
  #checkov:skip=CKV_AWS_116:Stream sources don't use function DLQs; failures go to the event source mapping's SQS on-failure destination
  #checkov:skip=CKV_AWS_117:No private resources to reach; a VPC would add NAT Gateway cost for S3 access
  #checkov:skip=CKV_AWS_115:Concurrency is bounded by shard count; reserving it can fail on new accounts with a 10-execution limit
  #checkov:skip=CKV_AWS_173:Only environment variable is the bucket name, which is not a secret
  #checkov:skip=CKV_AWS_272:Code signing is out of scope; deployments only come from the approved CI pipeline

  tracing_config {
    mode = "Active"
  }

  environment {
    variables = {
      BUCKET_NAME       = aws_s3_bucket.datalake.bucket
      METRICS_NAMESPACE = local.metrics_namespace
      PIPELINE_NAME     = var.project
      FRESHNESS_SLO_MS  = tostring(var.freshness_slo_seconds * 1000)
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}

# Kinesis -> Lambda wiring
resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn                   = aws_kinesis_stream.telemetry.arn
  function_name                      = aws_lambda_function.consumer.arn
  starting_position                  = "TRIM_HORIZON" # don't drop events produced before the consumer is ready
  batch_size                         = 100
  maximum_batching_window_in_seconds = 5
  bisect_batch_on_function_error     = true
  maximum_retry_attempts             = 3
  function_response_types            = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.consumer_dlq.arn
    }
  }

  depends_on = [aws_iam_role_policy.lambda]
}

# ---------- Catalog + Query: Glue & Athena ----------
resource "aws_glue_catalog_database" "telemetry" {
  name = replace("${var.project}_db", "-", "_")
}

resource "aws_glue_catalog_table" "events" {
  name          = "events"
  database_name = aws_glue_catalog_database.telemetry.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"            = "json"
    "projection.enabled"        = "true"
    "projection.tenant.type"    = "enum"
    "projection.tenant.values"  = join(",", var.tenants)
    "projection.dt.type"        = "date"
    "projection.dt.format"      = "yyyy-MM-dd"
    "projection.dt.range"       = "2026-01-01,NOW"
    "storage.location.template" = "s3://${aws_s3_bucket.datalake.bucket}/events/tenant=$${tenant}/dt=$${dt}/"
  }

  partition_keys {
    name = "tenant"
    type = "string"
  }
  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.bucket}/events/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    dynamic "columns" {
      for_each = {
        id          = "string"
        tenant_id   = "string"
        event_ts    = "string"
        latency_ms  = "double"
        status_code = "int"
      }
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

resource "aws_athena_workgroup" "analytics" {
  name          = "${var.project}-wg"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

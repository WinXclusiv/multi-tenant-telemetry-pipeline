# One-time setup, run locally with admin credentials:
#   - S3 bucket for the main stack's Terraform state
#   - GitHub OIDC identity provider
#   - Permissions boundary that caps what the pipeline's Lambda role can ever do
#   - Two roles GitHub Actions can assume: read-only "plan" (PRs) and scoped "deploy"
#
# This stack keeps LOCAL state (it creates the remote state bucket, so it can't use it).
# Its terraform.tfstate is gitignored; keep it safe.

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 6.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

provider "aws" {
  region = var.region

  # Safety guard: fail before any API call if the credentials belong to another account
  allowed_account_ids = [var.aws_account_id]
  default_tags {
    tags = { Project = var.project, ManagedBy = "terraform-bootstrap" }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  account   = data.aws_caller_identity.current.account_id
  partition = data.aws_partition.current.partition
  repo      = "${var.github_owner}/${var.github_repo}"
  glue_db   = replace("${var.project}_db", "-", "_")

  # Subject used in the OIDC trust policies. Repos created after July 15, 2026 get
  # GitHub's immutable format, which pins the owner and repo IDs:
  #   repo:<owner>@<owner_id>/<repo>@<repo_id>:...
  # A deleted-and-recreated repo with the same name can't match the IDs.
  immutable_ids = var.github_owner_id != null && var.github_repo_id != null
  repo_subject  = local.immutable_ids ? "${var.github_owner}@${var.github_owner_id}/${var.github_repo}@${var.github_repo_id}" : local.repo

  # Deliberately NOT prefixed with var.project, so the deploy role's
  # "${var.project}-*" permissions can't touch the state bucket or its own role.
  state_bucket = "tfstate-${var.project}-${random_id.suffix.hex}"

  oidc_provider_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : data.aws_iam_openid_connect_provider.github[0].arn
}

# ---------- Remote state bucket ----------
resource "aws_s3_bucket" "state" {
  bucket = local.state_bucket
  #checkov:skip=CKV_AWS_18:Access logging needs a second bucket; CloudTrail covers state-bucket API activity
  #checkov:skip=CKV_AWS_144:Cross-region replication is out of scope for a single-region sandbox
  #checkov:skip=CKV_AWS_145:SSE-S3 is enabled; a KMS CMK adds cost without a compliance driver here
  #checkov:skip=CKV2_AWS_62:No consumer of state-bucket events
}

resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled" # recover from a corrupted or accidentally deleted state file
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "state_tls_only" {
  bucket = aws_s3_bucket.state.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.state]
}

# ---------- GitHub OIDC provider (one per AWS account) ----------
resource "aws_iam_openid_connect_provider" "github" {
  count          = var.create_oidc_provider ? 1 : 0
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS validates GitHub's certificate via its own trusted CA library; these
  # thumbprints are kept for providers/tools that still expect the field.
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
  ]
}

data "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 0 : 1
  url   = "https://token.actions.githubusercontent.com"
}

# ---------- Permissions boundary for the pipeline's Lambda role ----------
# The deploy role may only create/modify IAM roles that carry this boundary,
# so even a malicious PR can't mint an admin role (privilege escalation guard).
resource "aws_iam_policy" "lambda_boundary" {
  name        = "${var.project}-lambda-boundary"
  description = "Maximum permissions any ${var.project} Lambda role can ever have"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadStream"
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary", "kinesis:ListShards"
        ]
        Resource = "arn:${local.partition}:kinesis:${var.region}:${local.account}:stream/${var.project}-*"
      },
      {
        Sid      = "ListStreams"
        Effect   = "Allow"
        Action   = ["kinesis:ListStreams"]
        Resource = "*"
      },
      {
        Sid      = "FailedRecordsToDlq"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = "arn:${local.partition}:sqs:${var.region}:${local.account}:${var.project}-*"
      },
      {
        Sid      = "XRayTracing"
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
        Resource = "*"
      },
      {
        Sid      = "WriteEvents"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "arn:${local.partition}:s3:::${var.project}-*/events/*"
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:${local.partition}:logs:${var.region}:${local.account}:log-group:/aws/lambda/${var.project}-*"
      }
    ]
  })
}

# ---------- Shared pieces ----------
data "aws_iam_policy_document" "state_access" {
  statement {
    sid       = "ListStateBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.state.arn]
  }
  statement {
    sid       = "ReadWriteStateAndLock"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.state.arn}/*"]
  }
}

data "aws_iam_policy_document" "trust_plan" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${local.repo_subject}:pull_request"]
    }
  }
}

data "aws_iam_policy_document" "trust_deploy" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    # Only jobs running in these protected GitHub Environments can deploy.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${local.repo_subject}:environment:sandbox",
        "repo:${local.repo_subject}:environment:sandbox-teardown",
      ]
    }
  }
}

# ---------- Plan role: pull requests, read-only ----------
resource "aws_iam_role" "plan" {
  name                 = "gha-${var.project}-plan"
  assume_role_policy   = data.aws_iam_policy_document.trust_plan.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "plan_readonly" {
  role       = aws_iam_role.plan.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_iam_role_policy" "plan_state" {
  name   = "terraform-state"
  role   = aws_iam_role.plan.id
  policy = data.aws_iam_policy_document.state_access.json
}

# ---------- Deploy role: protected environments, scoped writes ----------
resource "aws_iam_role" "deploy" {
  name                 = "gha-${var.project}-deploy"
  assume_role_policy   = data.aws_iam_policy_document.trust_deploy.json
  max_session_duration = 3600
}

# Read access covers the many Describe/List/Get calls Terraform makes while
# refreshing state. Tighten later with IAM Access Analyzer policy generation.
resource "aws_iam_role_policy_attachment" "deploy_readonly" {
  role       = aws_iam_role.deploy.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_iam_role_policy" "deploy_state" {
  name   = "terraform-state"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.state_access.json
}

data "aws_iam_policy_document" "deploy" {
  #checkov:skip=CKV_AWS_356:Event source mapping APIs don't support resource-level scoping; writes are constrained by the lambda:FunctionArn condition
  statement {
    sid       = "Kinesis"
    actions   = ["kinesis:*"]
    resources = ["arn:${local.partition}:kinesis:${var.region}:${local.account}:stream/${var.project}-*"]
  }

  statement {
    sid     = "DataLakeBucket"
    actions = ["s3:*"]
    resources = [
      "arn:${local.partition}:s3:::${var.project}-*",
      "arn:${local.partition}:s3:::${var.project}-*/*",
    ]
  }

  statement {
    sid       = "LambdaFunction"
    actions   = ["lambda:*"]
    resources = ["arn:${local.partition}:lambda:${var.region}:${local.account}:function:${var.project}-*"]
  }

  statement {
    sid       = "EventSourceMappingRead"
    actions   = ["lambda:GetEventSourceMapping", "lambda:ListEventSourceMappings"]
    resources = ["*"]
  }

  statement {
    sid = "EventSourceMappingWrite"
    actions = [
      "lambda:CreateEventSourceMapping",
      "lambda:UpdateEventSourceMapping",
      "lambda:DeleteEventSourceMapping",
    ]
    resources = ["*"]
    condition {
      test     = "ArnLike"
      variable = "lambda:FunctionArn"
      values   = ["arn:${local.partition}:lambda:${var.region}:${local.account}:function:${var.project}-*"]
    }
  }

  statement {
    sid       = "EventSourceMappingTags"
    actions   = ["lambda:TagResource", "lambda:UntagResource", "lambda:ListTags"]
    resources = ["arn:${local.partition}:lambda:${var.region}:${local.account}:event-source-mapping:*"]
  }

  statement {
    sid = "IamRoleWritesRequireBoundary"
    actions = [
      "iam:CreateRole",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:PutRolePermissionsBoundary",
    ]
    resources = ["arn:${local.partition}:iam::${local.account}:role/${var.project}-*"]
    condition {
      test     = "StringEquals"
      variable = "iam:PermissionsBoundary"
      values   = [aws_iam_policy.lambda_boundary.arn]
    }
  }

  statement {
    sid = "IamRoleManage"
    actions = [
      "iam:GetRole", "iam:GetRolePolicy", "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies", "iam:ListInstanceProfilesForRole",
      "iam:DeleteRole", "iam:TagRole", "iam:UntagRole", "iam:UpdateRole",
    ]
    resources = ["arn:${local.partition}:iam::${local.account}:role/${var.project}-*"]
  }

  statement {
    sid       = "PassRoleToLambdaOnly"
    actions   = ["iam:PassRole"]
    resources = ["arn:${local.partition}:iam::${local.account}:role/${var.project}-*"]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["lambda.amazonaws.com"]
    }
  }

  statement {
    sid       = "NeverRemoveBoundary"
    effect    = "Deny"
    actions   = ["iam:DeleteRolePermissionsBoundary"]
    resources = ["*"]
  }

  statement {
    sid     = "Logs"
    actions = ["logs:*"]
    resources = [
      "arn:${local.partition}:logs:${var.region}:${local.account}:log-group:/aws/lambda/${var.project}-*",
      "arn:${local.partition}:logs:${var.region}:${local.account}:log-group:/aws/lambda/${var.project}-*:*",
    ]
  }

  statement {
    sid     = "GlueCatalog"
    actions = ["glue:*"]
    resources = [
      "arn:${local.partition}:glue:${var.region}:${local.account}:catalog",
      "arn:${local.partition}:glue:${var.region}:${local.account}:database/${local.glue_db}",
      "arn:${local.partition}:glue:${var.region}:${local.account}:table/${local.glue_db}/*",
      "arn:${local.partition}:glue:${var.region}:${local.account}:userDefinedFunction/${local.glue_db}/*",
    ]
  }

  statement {
    sid       = "DeadLetterQueue"
    actions   = ["sqs:*"]
    resources = ["arn:${local.partition}:sqs:${var.region}:${local.account}:${var.project}-*"]
  }

  statement {
    sid       = "AthenaWorkgroup"
    actions   = ["athena:*"]
    resources = ["arn:${local.partition}:athena:${var.region}:${local.account}:workgroup/${var.project}-*"]
  }

  statement {
    sid     = "AlarmsAndDashboards"
    actions = ["cloudwatch:*"]
    resources = [
      "arn:${local.partition}:cloudwatch:${var.region}:${local.account}:alarm:${var.project}-*",
      "arn:${local.partition}:cloudwatch::${local.account}:dashboard/${var.project}-*",
    ]
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "scoped-deploy"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}

# ---------- Alert routing ----------
# Lives in the bootstrap (not the pipeline stack) on purpose: the pipeline is
# destroyed nightly, and an email subscription would need re-confirming after
# every redeploy. Alarms in the pipeline stack just reference these ARNs.
resource "aws_sns_topic" "alerts" {
  for_each = toset(["page", "ticket"])
  name     = "alerts-${var.project}-${each.key}"
  #checkov:skip=CKV_AWS_26:CloudWatch alarms cannot publish to topics encrypted with the AWS-managed aws/sns key; alert text holds no sensitive data
}

resource "aws_sns_topic_policy" "alerts" {
  for_each = aws_sns_topic.alerts
  arn      = each.value.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "CloudWatchAlarmsFromThisAccount"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = each.value.arn
        Condition = {
          StringEquals = { "aws:SourceAccount" = local.account }
          ArnLike      = { "aws:SourceArn" = "arn:${local.partition}:cloudwatch:${var.region}:${local.account}:alarm:${var.project}-*" }
        }
      },
      {
        Sid       = "AccountOwnerManages"
        Effect    = "Allow"
        Principal = { AWS = "arn:${local.partition}:iam::${local.account}:root" }
        Action    = ["sns:GetTopicAttributes", "sns:SetTopicAttributes", "sns:Subscribe", "sns:ListSubscriptionsByTopic", "sns:Publish"]
        Resource  = each.value.arn
      },
    ]
  })
}

resource "aws_sns_topic_subscription" "alert_email" {
  for_each  = { for k, t in aws_sns_topic.alerts : k => t.arn if var.alert_email != "" }
  topic_arn = each.value
  protocol  = "email"
  endpoint  = var.alert_email
}

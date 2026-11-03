variable "aws_account_id" {
  type        = string
  description = "The ONLY AWS account this stack may touch. Terraform refuses to run against any other account (protects production accounts on the same machine)."

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be a 12-digit AWS account ID."
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type        = string
  default     = "fde-telemetry"
  description = "Name prefix for every resource. The CI/CD deploy role is scoped to this prefix."
}

variable "shard_count" {
  type        = number
  default     = 1
  description = "Each provisioned shard bills per hour even when idle"
}

variable "tenants" {
  type    = list(string)
  default = ["tenant_A", "tenant_B", "tenant_C"]
}

variable "lambda_permissions_boundary_arn" {
  type        = string
  default     = null
  description = "Permissions boundary for the Lambda role. Required when deploying through CI (output of bootstrap/); optional for local deploys."
}

# ---------- Observability / SLO ----------

variable "slo_target" {
  type        = number
  default     = 0.99
  description = "Freshness SLO: fraction of events that must be queryable within freshness_slo_seconds"

  validation {
    condition     = var.slo_target > 0 && var.slo_target < 1
    error_message = "slo_target must be between 0 and 1 (for example 0.99)."
  }
}

variable "freshness_slo_seconds" {
  type        = number
  default     = 60
  description = "An event is 'good' if it is written to the data lake within this many seconds of event_ts"
}

variable "slo_window_days" {
  type        = number
  default     = 28
  description = "Rolling SLO window; burn-rate alert thresholds are derived from it"
}

variable "page_topic_arn" {
  type        = string
  default     = ""
  description = "SNS topic for urgent alerts (output of bootstrap/). Empty = alarms still evaluate but notify nobody."
}

variable "ticket_topic_arn" {
  type        = string
  default     = ""
  description = "SNS topic for non-urgent alerts (output of bootstrap/)"
}

variable "runbook_base_url" {
  type        = string
  default     = "docs/runbooks"
  description = "Base URL for runbook links in alarm descriptions, e.g. https://github.com/<owner>/<repo>/blob/main/docs/runbooks"
}

variable "aws_account_id" {
  type        = string
  description = "The ONLY AWS account this stack may touch. Terraform refuses to run against any other account (protects production accounts on the same machine)."

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be a 12-digit AWS account ID."
  }
}

variable "github_owner" {
  type        = string
  description = "GitHub user or org that owns the repo (case-sensitive, as shown on GitHub)"
}

variable "github_repo" {
  type    = string
  default = "multi-tenant-telemetry-pipeline"
}

variable "project" {
  type        = string
  default     = "fde-telemetry"
  description = "Must match var.project in the root stack; the deploy role is scoped to this prefix"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "create_oidc_provider" {
  type        = bool
  default     = true
  description = "Set false if this AWS account already has the GitHub OIDC provider (only one is allowed per account)"
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "Email that receives page and ticket alerts. AWS sends a confirmation link once; empty = no subscription."
}

output "state_bucket" {
  value = aws_s3_bucket.state.bucket
}

output "plan_role_arn" {
  value = aws_iam_role.plan.arn
}

output "deploy_role_arn" {
  value = aws_iam_role.deploy.arn
}

output "lambda_boundary_arn" {
  value = aws_iam_policy.lambda_boundary.arn
}

output "github_variables_commands" {
  description = "Paste these into your shell (from the repo root) to configure the GitHub repo"
  value       = <<-EOT
    gh variable set AWS_ACCOUNT_ID      --body "${var.aws_account_id}"
    gh variable set AWS_REGION          --body "${var.region}"
    gh variable set TF_STATE_BUCKET     --body "${aws_s3_bucket.state.bucket}"
    gh variable set AWS_PLAN_ROLE_ARN   --body "${aws_iam_role.plan.arn}"
    gh variable set AWS_DEPLOY_ROLE_ARN --body "${aws_iam_role.deploy.arn}"
    gh variable set LAMBDA_BOUNDARY_ARN --body "${aws_iam_policy.lambda_boundary.arn}"
    gh variable set ALERT_PAGE_TOPIC_ARN   --body "${aws_sns_topic.alerts["page"].arn}"
    gh variable set ALERT_TICKET_TOPIC_ARN --body "${aws_sns_topic.alerts["ticket"].arn}"
  EOT
}

output "alert_topic_arns" {
  value = { for k, t in aws_sns_topic.alerts : k => t.arn }
}

# Copy to terraform.tfvars (gitignored) and fill in your own values.
aws_account_id = "123456789012"
github_owner   = "your-github-username"
alert_email    = "you@example.com"

# Repos created after July 15, 2026 use GitHub's immutable OIDC subject.
#   gh api users/<owner> -q .id
#   gh api repos/<owner>/<repo> -q .id
github_owner_id = 0
github_repo_id  = 0
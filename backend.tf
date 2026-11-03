# Remote state so CI runners (which are wiped after every job) and your laptop
# share one state file. The bucket name is NOT hard-coded: pass it at init time.
#
#   terraform init -backend-config="bucket=<state bucket from bootstrap output>"
#
# use_lockfile = true gives S3-native state locking (Terraform >= 1.10),
# replacing the old DynamoDB lock table.
terraform {
  backend "s3" {
    key          = "multi-tenant-telemetry-pipeline/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

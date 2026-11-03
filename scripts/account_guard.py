"""Refuse to run against any AWS account except the sandbox.

Set the sandbox account ID once per terminal (or in your VS Code workspace settings):
  PowerShell:  $env:PIPELINE_AWS_ACCOUNT_ID = "123456789012"
  bash:        export PIPELINE_AWS_ACCOUNT_ID=123456789012
"""

import os
import sys

ENV_VAR = "PIPELINE_AWS_ACCOUNT_ID"


def require_sandbox(session):
    expected = os.environ.get(ENV_VAR, "").strip()
    if not expected:
        sys.exit(f"Refusing to run: set {ENV_VAR} to your sandbox account ID first.")
    actual = session.client("sts").get_caller_identity()["Account"]
    if actual != expected:
        sys.exit(
            f"Refusing to run: credentials are for account {actual}, "
            f"but {ENV_VAR} is {expected}. Check AWS_PROFILE."
        )
    return actual

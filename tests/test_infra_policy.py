"""Static checks on the Glue job IAM policy in infra/main.tf."""

import re
from pathlib import Path

MAIN_TF = Path(__file__).resolve().parents[1] / "infra" / "main.tf"


def glue_job_policy() -> str:
    text = MAIN_TF.read_text()
    match = re.search(r'data "aws_iam_policy_document" "glue_job" \{(.*?)\n\}\n', text, re.S)
    assert match, "glue_job policy document not found"
    return match.group(1)


def test_glue_role_can_use_saas_connector():
    # Required to use a Glue SaaS connection such as Salesforce Marketing Cloud:
    # https://docs.aws.amazon.com/glue/latest/dg/salesforce-marketing-cloud-configuring-iam-permissions.html
    policy = glue_job_policy()
    for action in (
        "glue:ListConnectionTypes",
        "glue:DescribeConnectionType",
        "glue:RefreshOAuth2Tokens",
        "glue:ListEntities",
        "glue:DescribeEntity",
    ):
        assert f'"{action}"' in policy, action


def test_glue_role_can_write_refreshed_tokens_to_connection_secret():
    assert '"secretsmanager:PutSecretValue"' in glue_job_policy()

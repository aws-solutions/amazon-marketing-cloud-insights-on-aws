# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This module is responsible for testing Sagemaker security in the AMC Insights stack.
"""

import pytest


def check_policy(logical_id, resource):
    policy_statements = resource["Properties"]["PolicyDocument"]["Statement"]
    found_deny_statement = False
    for statement in policy_statements:
        if statement["Effect"] == "Deny":
            for action in statement["Action"]:
                if action.startswith("s3:"):
                    try:
                        assert statement["Condition"]["StringNotEquals"]["aws:ResourceAccount"]
                        found_deny_statement = True
                        break
                    except KeyError:
                        continue

    if not found_deny_statement:
        pytest.fail(
            f"Sagemaker instance policy {logical_id} must have account conditional properties for denial of S3 actions")


def test_sagemaker_security(template):
    instances = template.find_resources("AWS::SageMaker::NotebookInstance")
    policies = template.find_resources("AWS::IAM::ManagedPolicy")

    for logical_id, sagemaker_instance in instances.items():
        instance_properties = sagemaker_instance["Properties"]

        # ensure sagemaker data is KMS encrypted
        assert instance_properties.get("KmsKeyId", None)

        # ensure sagemaker has assigned role
        try:
            instance_role = instance_properties["RoleArn"]["Fn::GetAtt"][0]
        except KeyError:
            pytest.fail(f"Sagemaker instance {logical_id} must have assigned Role")

        # ensure sagemaker cant access bucket not owned by AWS account
        for policy_logical_id, policy in policies.items():
            try:
                policy_roles = policy["Properties"]["Roles"]
                for role in policy_roles:
                    if role["Ref"] == instance_role:
                        check_policy(logical_id=policy_logical_id, resource=policy)
            except KeyError:
                continue

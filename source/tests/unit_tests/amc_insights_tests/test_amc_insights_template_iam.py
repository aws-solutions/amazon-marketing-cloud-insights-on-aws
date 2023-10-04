# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This module is responsible for testing IAM resources in the AMC Insights stack.
"""

# source/amc_insights_tests/test_amc_insights_template_iam.py

from pathlib import Path
import pytest

from aws_cdk import App
from amc_insights.amc_insights_stack import AMCInsightsStack
from aws_cdk.assertions import Template

from aws_solutions.cdk import CDKSolution

STOCK_BASIC_EXECUTION_ROLE_COUNT = 29


@pytest.fixture(scope="module")
def mock_solution():
    path = Path(__file__).parent.parent.parent.parent / "infrastructure" / "cdk.json"
    return CDKSolution(cdk_json_path=path)


@pytest.fixture(scope="module")
def template(mock_solution):
    app = App(context=mock_solution.context.context)
    stack = AMCInsightsStack(
        app,
        AMCInsightsStack.name,
        description=AMCInsightsStack.description,
        template_filename=AMCInsightsStack.template_filename,
        synthesizer=mock_solution.synthesizer)
    yield Template.from_stack(stack)


def test_lambda_function_roles(template):
    """
    Testing only selected roles in the template
    """
    # related to most Lambda functions for basic execution role
    found = template.find_resources(
        "AWS::IAM::Role", {
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Statement": [{
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        }
                    }],
                    "Version":
                    "2012-10-17"
                },
                "ManagedPolicyArns": [{
                    "Fn::Join": [
                        "",
                        [
                            "arn:", {
                                "Ref": "AWS::Partition"
                            },
                            ":iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                        ]
                    ]
                }]
            }
        })
    assert len(found) == STOCK_BASIC_EXECUTION_ROLE_COUNT



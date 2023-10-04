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


def test_cloudtrail_policy(template):
    # test the cloudtrail bucket policy
    template.has_resource(
        "AWS::S3::BucketPolicy", {
            "Properties": {
                "Bucket": {
                    "Ref": "bucketslogging3F0A1C76"
                },
                "PolicyDocument": {
                    "Statement": [
                        {
                            "Action": "s3:*",
                            "Condition": {
                                "Bool": {
                                    "aws:SecureTransport": "false"
                                }
                            },
                            "Effect": "Deny",
                            "Principal": {
                                "AWS": "*"
                            },
                            "Resource": [
                                {
                                    "Fn::GetAtt": [
                                        "bucketslogging3F0A1C76",
                                        "Arn"
                                    ]
                                },
                                {
                                    "Fn::Join": [
                                        "",
                                        [
                                            {
                                                "Fn::GetAtt": [
                                                    "bucketslogging3F0A1C76",
                                                    "Arn"
                                                ]
                                            },
                                            "/*"
                                        ]
                                    ]
                                }
                            ]
                        },
                        {
                            "Action": "s3:GetBucketAcl",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "cloudtrail.amazonaws.com"
                            },
                            "Resource": {
                                "Fn::GetAtt": [
                                    "bucketslogging3F0A1C76",
                                    "Arn"
                                ]
                            }
                        },
                        {
                            "Action": "s3:PutObject",
                            "Condition": {
                                "StringEquals": {
                                    "s3:x-amz-acl": "bucket-owner-full-control"
                                }
                            },
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "cloudtrail.amazonaws.com"
                            },
                            "Resource": {
                                "Fn::Join": [
                                    "",
                                    [
                                        {
                                            "Fn::GetAtt": [
                                                "bucketslogging3F0A1C76",
                                                "Arn"
                                            ]
                                        },
                                        "/AWSLogs/",
                                        {
                                            "Ref": "AWS::AccountId"
                                        },
                                        "/*"
                                    ]
                                ]
                            }
                        }
                    ],
                    "Version": "2012-10-17"
                }
            },
        })

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for MicroServices/AMCInstance Tps handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_tps_AMCInstance_handler.py
###############################################################################

import contextlib
import sys
import os
import pytest
import boto3
import json
from moto import mock_aws
from unittest.mock import patch


@pytest.fixture
def test_yaml():
    def _method(test_no="1"):
        # fake yaml
        return f"""
            AWSTemplateFormatVersion: "2010-09-09"
            Description: 'This is a fake yaml, with fake resource template'
            Resources:
              TestRole{test_no}:
                Type: AWS::IAM::Role
                Properties:
                  AssumeRolePolicyDocument:
                    Version: 2012-10-17
                    Statement:
                      - Effect: Allow
                        Principal:
                          Service:
                            - lambda.amazonaws.com
                        Action:
                          - sts:AssumeRole
        """
    return _method


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['DATA_LAKE_ENABLED'] = "Yes"
    os.environ['WFM_LAMBDA_ROLE_NAMES'] = "wfm_config_table"
    os.environ['SNS_KMS_KEY_ID'] = "123456"
    os.environ['RESOURCE_PREFIX'] = "lambda"
    os.environ['ADD_AMC_INSTANCE_LAMBDA_ROLE_ARN'] = "arn:aws:iam::978365123:role/lambda-us-east-1-978365123-invokeAmcApiRole"
    os.environ['LOGGING_BUCKET_NAME'] = "bucket_name"
    os.environ['TEMPLATE_URL'] = "https://test_template.s3.amazonaws.com"
    os.environ['ARTIFACTS_BUCKET_NAME'] = "tps_artifact_bucket"
    os.environ['ROUTING_QUEUE_LOGICAL_ID'] = "somearn"
    os.environ['STAGE_A_ROLE_LOGICAL_ID'] = "somerole"
    os.environ['AWS_ACCOUNT_ID'] = os.environ["MOTO_ACCOUNT_ID"]
    os.environ['API_INVOKE_ROLE_STANDARD'] = "amcinsights-us-east-1-123456789-invokeAmcApiRole"


@contextlib.contextmanager
def mock_handler_resource(test_yaml):
    s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
    s3.create_bucket(Bucket='test_template')
    s3.put_object(
        Body=test_yaml(),
        Bucket="test_template",
        Key="amc-initialize.yaml"
    )

    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'Test Policy',
            'Effect': 'Allow',
            'Principal': '*',
            'Action': ['s3:GetObject'],
            'Resource': f'arn:aws:s3:::{os.environ["ARTIFACTS_BUCKET_NAME"]}/*'
        }]
    }

    s3.create_bucket(Bucket=os.environ['ARTIFACTS_BUCKET_NAME'])
    s3.put_bucket_policy(Bucket=os.environ['ARTIFACTS_BUCKET_NAME'], Policy=json.dumps(bucket_policy))


    key_policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'Test Key Policy',
            'Effect': 'Allow',
            'Principal': '*',
            'Action': "kms:*",
            'Resource': "*"
        }]
    }
    kms = boto3.client("kms", region_name=os.environ["AWS_DEFAULT_REGION"])
    kms_res = kms.create_key(Policy=json.dumps(key_policy))
    yield kms_res["KeyMetadata"]["KeyId"]


@mock_aws
@patch('aws_solutions.extended.resource_lookup.ResourceLookup.get_physical_id')
def test_handler(mock_get_physical_id, test_yaml):

    mock_get_physical_id.return_value = 'some_lambda'

    with mock_handler_resource(test_yaml) as kms_key:
        os.environ['ARTIFACTS_BUCKET_KEY_ID'] = kms_key
        os.environ['APPLICATION_REGION'] = os.environ["AWS_DEFAULT_REGION"]
        os.environ['APPLICATION_ACCOUNT'] = os.environ['AWS_ACCOUNT_ID']

        sys.path.insert(0, "./infrastructure/amc_insights/microservices/tenant_provisioning_service/lambdas/AddAMCInstance/")
        from amc_insights.microservices.tenant_provisioning_service.lambdas.AddAMCInstance.handler import handler

        assert 'no activity' == handler({}, None)

        fake_event_cross_account = {
            "TenantName": "customer",
            "customerName": "customer_name",
            "amcOrangeAwsAccount": "978365123",
            "amcRedAwsAccount": "999365123",
            "BucketName": "bucket_name",
            "amcDatasetName": "test",
            "amcTeamName": "test",
            "amcRegion": os.environ['APPLICATION_REGION'],
            "amcApiEndpoint": "https://test123.execute-api.us-east-1.amazonaws.com/prod",
            "bucketExists": None,
            "bucketAccount":  "123456789",
            "bucketRegion": os.environ['APPLICATION_REGION'],
            "createSnsTopic": "true"
        }

        resp = handler(fake_event_cross_account, None)
        test_stack_name = "lambda-tps-instance-customer"
        assert test_stack_name in resp["StackId"]

        cf = boto3.client("cloudformation", region_name=os.environ["AWS_DEFAULT_REGION"])
        assert len(cf.list_stacks()["StackSummaries"]) == 1 # only stack should exist
        resp = cf.describe_stacks(StackName=test_stack_name) # check if stack was created
        assert resp["Stacks"][0]["StackName"] == test_stack_name

        s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
        s3.put_object(
            Body=test_yaml("update2"),
            Bucket="test_template",
            Key="amc-initialize.yaml"
        )

        resp = handler(fake_event_cross_account, None) # should update, rather than create
        assert test_stack_name in resp["StackId"]
        response = cf.describe_stacks(StackName=test_stack_name)
        assert response["Stacks"][0]["StackStatus"] == "UPDATE_COMPLETE"


        fake_event_cross_region = {
            "TenantName": "customer",
            "customerName": "customer_name",
            "amcOrangeAwsAccount": "978365123",
            "amcRedAwsAccount": "999365123",
            "BucketName": "bucket_name",
            "amcDatasetName": "test",
            "amcTeamName": "test",
            "amcRegion": os.environ['AWS_DEFAULT_REGION'],
            "amcApiEndpoint": "https://test123.execute-api.us-east-1.amazonaws.com/prod",
            "bucketExists": "true",
            "bucketAccount":  os.environ['AWS_ACCOUNT_ID'],
            "bucketRegion": "us-west-2",
            "createSnsTopic": "some_topic"
        }

        resp = handler(fake_event_cross_region, None)
        test_stack_name = "lambda-tps-instance-customer-crossregion"
        assert test_stack_name in resp["StackId"]


        fake_event_standard = {
            "TenantName": "customer",
            "customerName": "customer_name",
            "amcOrangeAwsAccount": "978365123",
            "amcRedAwsAccount": "999365123",
            "BucketName": "bucket_name",
            "amcDatasetName": "test",
            "amcTeamName": "test",
            "amcRegion": os.environ['AWS_DEFAULT_REGION'],
            "amcApiEndpoint": "https://test123.execute-api.us-east-1.amazonaws.com/prod",
            "bucketExists": "true",
            "bucketAccount":  os.environ['AWS_ACCOUNT_ID'],
            "bucketRegion": os.environ['APPLICATION_REGION'],
            "createSnsTopic": "some_topic"
        }

        resp = handler(fake_event_standard, None)
        test_stack_name = "lambda-tps-instance-customer"
        assert test_stack_name in resp["StackId"]


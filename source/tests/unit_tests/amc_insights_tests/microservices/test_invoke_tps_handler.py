# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for InvokeTPSInitializeSM/handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_invoke_tps_handler.py


import os
import sys
import boto3
import pytest
import json
import decimal
from unittest.mock import Mock
from moto import mock_aws
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0,
                "./infrastructure/amc_insights/microservices/workflow_management_service/lambda_layers/wfm_layer/python/")


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ[
        'STATE_MACHINE_ARN'] = f'arn:aws:states:us-east-1:{os.environ["MOTO_ACCOUNT_ID"]}:stateMachine:test_role_step_function'
    os.environ['DATASET_NAME'] = "test_dataset_name"
    os.environ['TEAM_NAME'] = "test_team_name"
    os.environ['APPLICATION_ACCOUNT'] = os.environ["MOTO_ACCOUNT_ID"]
    os.environ['DEFAULT_SNS_TOPIC'] = "test_sns_topic"
    os.environ['RESOURCE_PREFIX'] = "sqs"
    os.environ['APPLICATION_REGION'] = "us-east-1"


@pytest.fixture()
def _mock_cloudwatch_client():
    cloudwatch_client = get_service_client('cloudwatch')
    cloudwatch_client.put_metric_data = Mock(
        return_value={}
    )
    return cloudwatch_client


@pytest.fixture()
def _mock_secrets_manager_client():
    secrets_manager_client = get_service_client('secretsmanager')
    secrets_manager_client.get_secret_value = Mock(
        return_value={'SecretString': '123456'}
    )
    return secrets_manager_client


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_cloudwatch_client, _mock_secrets_manager_client):
    monkeypatch.setitem(_helpers_service_clients, 'cloudwatch', _mock_cloudwatch_client)
    monkeypatch.setitem(_helpers_service_clients, 'secretsmanager', _mock_secrets_manager_client)


@pytest.fixture
def customer_details():
    return {
        "amc": {
            "endpoint_url": "https://abc1234567.execute-api.us-east-1.amazonaws.com/beta",
            "aws_orange_account_id": "12390",
            "aws_red_account_id": "33333",
            "bucket_name": "test_bucket",
            "instance_id": "amc122345",
            "amazon_ads_advertiser_id": "12345",
            "amazon_ads_marketplace_id": "12345"
        },
        "customer_id": "12345",
        "customer_name": "some_cust_test_name",
    }


def test_json_encoder_default():
    from amc_insights.microservices.tenant_provisioning_service.lambdas.InvokeTPSInitializeSM.handler import \
        json_encoder_default

    assert "2" == json_encoder_default(decimal.Decimal(2))
    assert "2" == json_encoder_default(int(2))


def test_format_payload(customer_details):
    from amc_insights.microservices.tenant_provisioning_service.lambdas.InvokeTPSInitializeSM.handler import \
        format_payload

    test_payload = format_payload(customer_details)
    assert test_payload["TenantName"] == customer_details['customer_id']
    assert test_payload["customerName"] == customer_details['customer_name']
    assert test_payload["createSnsTopic"] == "false"
    assert test_payload["snsTopicArn"] == os.environ['DEFAULT_SNS_TOPIC']
    assert test_payload["amcOrangeAwsAccount"] == customer_details["amc"]["aws_orange_account_id"]
    assert test_payload["BucketName"] == customer_details["amc"]["bucket_name"]
    assert test_payload["amcDatasetName"] == os.environ['DATASET_NAME']
    assert test_payload["amcTeamName"] == os.environ['TEAM_NAME']
    assert test_payload["bucketExists"] == "true"
    assert test_payload["bucketAccount"] == os.environ['APPLICATION_ACCOUNT']
    assert test_payload["bucketRegion"] == os.environ['APPLICATION_REGION']


@mock_aws
def test_handler(customer_details, _mock_clients):
    from amc_insights.microservices.tenant_provisioning_service.lambdas.InvokeTPSInitializeSM.handler import handler

    test_role = "StepFunctionLambdaBasicExecution"
    iam_client = boto3.client('iam')
    iam_client.create_role(RoleName=test_role, AssumeRolePolicyDocument=json.dumps({
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {
                "Service": "states.amazonaws.com"
            }
        }],
        "Version": "2012-10-17"
    }))

    iam_client.put_role_policy(
        RoleName=test_role,
        PolicyName='LambdaAction',
        PolicyDocument=json.dumps({
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "lambda:InvokeFunction"
                    ],
                    "Resource": [
                        "*"
                    ]
                }
            ],
            "Version": "2012-10-17"
        })
    )

    test_role_arn = iam_client.get_role(RoleName=test_role)

    client = boto3.client('stepfunctions')

    client.create_state_machine(
        name="test_role_step_function",
        definition='some_defintion',
        roleArn=test_role_arn['Role']['Arn'],
    )

    message = handler({"customer_details": customer_details}, None)
    assert "arn:aws:states:us-east-1:111111111111:execution:test_role_step_function" in json.loads(message)[
        "executionArn"]

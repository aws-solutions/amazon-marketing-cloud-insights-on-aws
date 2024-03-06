# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import sys

import boto3
import pytest
from unittest.mock import Mock, MagicMock
from moto import mock_aws
from dataclasses import dataclass
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients, _helpers_service_resources



@pytest.fixture()
def _mock_sts_client():
    sts_client = get_service_client('sts')
    sts_client.get_caller_identity = Mock(
        return_value={
            'UserId': 'user_id',
            'Account': 'account_id',
            'Arn': 'arn'
        }
    )
    return sts_client


@pytest.fixture()
def _dynamodb_resource():
    with mock_aws():
        ddb = boto3.resource('dynamodb', 'us-east-1')
        pipelines_table_attr = [
            {
                'AttributeName': 'name',
                'AttributeType': 'S'
            }
        ]
        pipelines_table_schema = [
            {
                'AttributeName': 'name',
                'KeyType': 'HASH'
            }
        ]

        ddb.create_table(AttributeDefinitions=pipelines_table_attr,
                         TableName="octagon-Pipelines-dev-prefix",
                         KeySchema=pipelines_table_schema,
                         BillingMode='PAY_PER_REQUEST')

        dynamodb_table = ddb.Table("octagon-Pipelines-dev-prefix")
        dynamodb_table.put_item(
            Item={
                'name': "adtech-insights-stage-a",
                'description': "amci1 data lake light transform",
                'id': "sdlf-stage-a",
                "last_execution_date": '2023-05-23',
                "last_execution_duration_in_seconds": "7.822",
                "last_execution_id": "b3f4bba3-b03e-4803-95f2-9dc169803f03",
                "last_execution_status": "COMPLETED",
                "last_execution_timestamp": "2023-05-23T02:24:54.315Z",
                "last_updated_timestamp": "2023-05-23T02:24:54.315Z",
                "status": "ACTIVE",
                "version": 4
            }
        )

        object_metadata_table_attr = [
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            }
        ]
        object_metadata_table_schema = [
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }
        ]

        ddb.create_table(
            AttributeDefinitions=object_metadata_table_attr,
            TableName="octagon-ObjectMetadata-dev-prefix",
            KeySchema=object_metadata_table_schema,
            BillingMode='PAY_PER_REQUEST'
        )

        peh_table_attr = [
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            }
        ]
        peh_table_schema = [
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }
        ]

        ddb.create_table(
            AttributeDefinitions=peh_table_attr,
            TableName="octagon-PipelineExecutionHistory-dev-prefix",
            KeySchema=peh_table_schema,
            BillingMode='PAY_PER_REQUEST'
        )

        yield ddb


def side_effect(*args, **kwargs):
    if kwargs["Name"].endswith('ObjectMetadata'):
        return {
            'Parameter': {
                'Value': 'octagon-ObjectMetadata-dev-prefix',
            }
        }
    if kwargs["Name"].endswith('Datasets'):
        return {
            'Parameter': {
                'Value': 'octagon-Datasets-dev-prefix',
            }
        }
    return {}


@pytest.fixture()
def _mock_ssm_client():
    ssm_client = get_service_client('ssm')
    ssm_client.get_parameter = Mock(
        side_effect=side_effect
    )
    return ssm_client


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_sts_client, _dynamodb_resource, _mock_ssm_client):
    monkeypatch.setitem(_helpers_service_clients, 'sts', _mock_sts_client)
    monkeypatch.setitem(_helpers_service_resources, 'dynamodb', _dynamodb_resource)
    monkeypatch.setitem(_helpers_service_clients, 'ssm', _mock_ssm_client)


@pytest.fixture()
def __mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


@pytest.fixture()
def lambda_context():
    @dataclass
    class LambdaContext:
        function_name = "lambda-function-name"

    return LambdaContext()


@pytest.mark.parametrize(
    "lambda_event",
    [
        "{\"bucket\": \"raw_bucket\", \"key\": \"file_name\", \"timestamp\": 1684808686998, \"last_modified_date\": \"2023-05-23T02:24:43Z+00:00\", \"id\": \"s3://file_name\", \"stage\": \"raw\", \"team\": \"adtech\", \"dataset\": \"newdataset\", \"pipeline\": \"insights\", \"env\": \"dev\", \"pipeline_stage\": \"StageA\"}"
    ],
)
def test_handler_success(lambda_event, lambda_context, __mock_imports, _mock_clients, _dynamodb_resource):
    from data_lake.stages.sdlf_light_transform.lambdas.preupdate_metadata.handler import lambda_handler
    actual_response = lambda_handler(lambda_event, lambda_context)
    table = _dynamodb_resource.Table("octagon-PipelineExecutionHistory-dev-prefix")
    assert table.item_count == 1
    assert len(actual_response['body']) > 0


@pytest.mark.parametrize(
    "lambda_event",
    [
        "{\"bucket\": \"raw_bucket\", \"key\": \"file_name\", \"timestamp\": 1684808686998, \"last_modified_date\": \"2023-05-23T02:24:43Z+00:00\", \"id\": \"s3://file_name\", \"stage\": \"raw\", \"team\": \"adtech\"}"
    ],
)
def test_handler_fail(lambda_event, __mock_imports, _mock_clients):
    from data_lake.stages.sdlf_light_transform.lambdas.preupdate_metadata.handler import lambda_handler
    with pytest.raises(KeyError):
        lambda_handler(lambda_event, None)

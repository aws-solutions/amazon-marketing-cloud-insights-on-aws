# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import boto3
import pytest
from unittest.mock import Mock, MagicMock
import sys

from botocore.exceptions import ClientError
from moto import mock_aws
from dataclasses import dataclass
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients, _helpers_service_resources


@pytest.fixture()
def _mock_sts_client():
    sts_client = get_service_client('sts')
    sts_client.get_caller_identity = Mock(
        return_value={
            'Account': 'account_id',
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

        pipelines_table = ddb.Table("octagon-Pipelines-dev-prefix")
        pipelines_table.put_item(
            Item={
                'name': "adtech-insights-stage-b",
                'description': "amci1 data lake light transform",
                'id': "sdlf-stage-b",
                "last_execution_date": '2023-05-23',
                "last_execution_duration_in_seconds": "7.822",
                "last_execution_id": "d22222-111c-11b1-a11c-11111dg11o111",
                "last_execution_status": "COMPLETED",
                "last_execution_timestamp": "2023-05-23T02:24:54.315Z",
                "last_updated_timestamp": "2023-05-23T02:24:54.315Z",
                "status": "ACTIVE",
                "version": 4
            }
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

        datasets_table_attr = [
            {
                'AttributeName': 'name',
                'AttributeType': 'S'
            }
        ]
        datasets_table_schema = [
            {
                'AttributeName': 'name',
                'KeyType': 'HASH'
            }
        ]

        ddb.create_table(AttributeDefinitions=datasets_table_attr,
                         TableName="octagon-Datasets-dev-prefix",
                         KeySchema=datasets_table_schema,
                         BillingMode='PAY_PER_REQUEST')

        datasets_table = ddb.Table("octagon-Datasets-dev-prefix")
        datasets_table.put_item(
            Item={
                'name': 'adtech-datasetA',
                'description': 'sdlf dataset',
                'id': 'sdlf-dataset',
                'max_items_process': {"stage_c": 100, "stage_b": 100},
                "min_items_process": {"stage_c": 1, "stage_b": 1},
                'pipeline': 'insights',
                'transforms': {"stage_a_transform": "amc_light_transform",
                               "stage_b_transform": "default_heavy_transform"},
                'version': 1
            }
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
    if kwargs["Name"].endswith('StageDataCatalog'):
        return {
            'Parameter': {
                'Value': 'prefix_datalake_dev_adtech_datasetA_db',
            }
        }
    return {
        'Parameter': {
            'Value': 'dummy_value',
        }
    }


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
def _mock_imports():
    mocked_awswrangler = MagicMock()
    sys.modules['awswrangler'] = mocked_awswrangler


@pytest.fixture()
def lambda_context():
    @dataclass
    class LambdaContext:
        function_name = "lambda-function-name"

    return LambdaContext()


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "statusCode": 200,
            "body": {
                "bucket": "stage_bucket",
                "keysToProcess": [
                    "pre-stage/adtech/datasetA/filename_parsed",
                ],
                "team": "adtech",
                "pipeline": "insights",
                "pipeline_stage": "StageB",
                "dataset": "datasetA",
                "env": "dev"
            }
        }
    ],
)
def test_handler(lambda_event, lambda_context, _mock_clients, _dynamodb_resource, _mock_imports):
    from data_lake.stages.sdlf_heavy_transform.lambdas.process_object.handler import lambda_handler, \
        logger as lambda_function_logger
    lambda_function_logger.propagate = True

    with pytest.raises(ClientError):
        lambda_handler(lambda_event, lambda_context)

    peh_table = _helpers_service_resources["dynamodb"].Table("octagon-PipelineExecutionHistory-dev-prefix")
    assert peh_table.item_count == 1

    _helpers_service_clients["ssm"].get_parameter.assert_called()
    _helpers_service_clients["sts"].get_caller_identity.assert_called()


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "statusCode": 200,
            "body": {
                "bucket": "stage_bucket",
                "keysToProcess": [
                    "pre-stage/adtech/datasetA/filename_parsed",
                ],
                "team": "adtech",
                "pipeline": "insights",
                "dataset": "datasetA",
                "env": "dev"
            }
        }
    ],
)
def test_handler_fail_fetch_event_data(lambda_event, lambda_context, _mock_clients, _dynamodb_resource):
    from data_lake.stages.sdlf_heavy_transform.lambdas.process_object.handler import lambda_handler, \
        logger as lambda_function_logger
    lambda_function_logger.propagate = True

    with pytest.raises(KeyError):
        lambda_handler(lambda_event, lambda_context)

    peh_table = _helpers_service_resources["dynamodb"].Table("octagon-PipelineExecutionHistory-dev-prefix")
    assert peh_table.item_count == 0

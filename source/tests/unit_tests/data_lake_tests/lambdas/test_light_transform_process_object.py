# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import boto3
import pytest
from unittest.mock import Mock, MagicMock
import sys

from botocore.exceptions import ClientError
from moto import mock_dynamodb, mock_s3
from dataclasses import dataclass
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients, _helpers_service_resources


@pytest.fixture(autouse=True)
def mock_env_variables():
    os.environ["RESOURCE_PREFIX"] = "prefix"


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
def dynamodb_client():
    with mock_dynamodb():
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
                'name': "adtech-insights-stage-a",
                'description': "amci1 data lake light transform",
                'id': "sdlf-stage-a",
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

        peh_table = ddb.Table("octagon-PipelineExecutionHistory-dev-prefix")
        peh_table.put_item(
            Item={
                'id': "d11111-111c-11b1-a11c-11111dg11o111",
                'active': "false",
                'comment': "comment",
                "dataset_date": '2023-05-23',
                "duration_in_seconds": "7.822",
                "end_timestamp": "2023-05-23T02:24:54.315Z",
                "execution_date": "2023-05-23",
                "history": [
                    "{\"M\": {\"status\": {\"S\": \"STARTED\"}, \"timestamp\": {\"S\": \"2023-05-23T02:24:47.369Z\"}}}, {\"M\": {\"component\": {\"S\": \"Preupdate\"}, \"status\": {\"S\": \"StageA Preupdate Processing\"}, \"timestamp\": {\"S\": \"2023-05-23T02:24:47.529Z\"}}}, {\"M\": {\"component\": {\"S\": \"Process\"}, \"status\": {\"S\": \"StageA Process Processing\"}, \"timestamp\": {\"S\": \"2023-05-23T02:24:47.978Z\"}}}, {\"M\": {\"component\": {\"S\": \"Postupdate\"}, \"status\": {\"S\": \"StageA Postupdate Processing\"}, \"timestamp\": {\"S\": \"2023-05-23T02:24:52.240Z\"}}}, {\"M\": {\"status\": {\"S\": \"COMPLETED\"}, \"timestamp\": {\"S\": \"2023-05-23T02:24:52.320Z\"}}}"],
                "last_updated_timestamp": "2023-05-23T02:24:52.320Z",
                "pipeline": "adtech-insights-stage-a",
                "start_timestamp": "2023-05-23T02:24:47.369Z",
                "status": "COMPLETED",
                "status_last_updated_timestamp": "COMPLETED#2023-05-23T02:24:52.320Z",
                "success": "true",
                "ttl": 1695176682,
                "version": 5
            }
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
                               "stage_b_transform": "amc_heavy_transform"},
                'version': 1
            }
        )

        yield ddb


@pytest.fixture()
def _s3_resource():
    with mock_s3():
        s3 = boto3.resource('s3', 'us-east-1')
        s3.create_bucket(Bucket="stage_bucket")

        s3.create_bucket(Bucket="raw_bucket")
        s3_object = s3.Object('raw_bucket', "adtech/datasetA/filename/filename")
        s3_object.put(Body="pre-stage file content")
        print(s3_object.content_length)
        yield s3


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
    if kwargs["Name"].endswith('StageBucket'):
        return {
            'Parameter': {
                'Value': 'stage_bucket',
            }
        }
    return {
        'Parameter': {
            'Value': 'value',
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
def _mock_clients(monkeypatch, _mock_sts_client, dynamodb_client, _mock_ssm_client, _s3_resource):
    monkeypatch.setitem(_helpers_service_clients, 'sts', _mock_sts_client)
    monkeypatch.setitem(_helpers_service_resources, 'dynamodb', dynamodb_client)
    monkeypatch.setitem(_helpers_service_clients, 'ssm', _mock_ssm_client)
    monkeypatch.setitem(_helpers_service_resources, 's3', _s3_resource)


@pytest.fixture()
def _mock_imports():
    mocked_awswrangler = MagicMock()
    sys.modules['awswrangler'] = mocked_awswrangler

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
        {
            "statusCode": 200,
            "body": {
                "bucket": "raw_bucket",
                "key": "adtech/datasetA/filename/filename",
                "timestamp": 1684808687458,
                "last_modified_date": "2023-05-23T02:24:43Z+00:00",
                "id": "s3://raw_bucket/adtech/datasetA/filename/filename",
                "stage": "raw",
                "team": "adtech",
                "dataset": "datasetA",
                "pipeline": "insights",
                "env": "dev",
                "pipeline_stage": "StageA",
                "peh_id": "d11111-111c-11b1-a11c-11111dg11o111"
            }
        }
    ],
)
def test_handler(lambda_event, lambda_context, _mock_clients, dynamodb_client, _mock_imports):
    from data_lake.stages.sdlf_light_transform.lambdas.process_object.handler import lambda_handler
    with pytest.raises(ClientError):
        lambda_handler(lambda_event, lambda_context)

    peh_table = _helpers_service_resources["dynamodb"].Table("octagon-PipelineExecutionHistory-dev-prefix")
    res = peh_table.get_item(
        Key={
            'id': 'd11111-111c-11b1-a11c-11111dg11o111'
        }
    )
    assert res['Item']['status'] == 'FAILED'

    _helpers_service_clients["ssm"].get_parameter.assert_called()
    _helpers_service_clients["sts"].get_caller_identity.assert_called()


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "statusCode": 200,
            "body": {
                "bucket": "raw_bucket",
                "key": "adtech/datasetA/filename/filename",
                "timestamp": 1684808687458,
                "last_modified_date": "2023-05-23T02:24:43Z+00:00",
                "id": "s3://raw_bucket/adtech/datasetA/filename/filename",
                "stage": "raw",
                "pipeline_stage": "StageA",
                "peh_id": "d11111-111c-11b1-a11c-11111dg11o111"
            }
        }
    ],
)
def test_handler_fail_fetch_event_data(lambda_event, lambda_context, _mock_clients, dynamodb_client):
    from data_lake.stages.sdlf_light_transform.lambdas.process_object.handler import lambda_handler

    with pytest.raises(KeyError):
        lambda_handler(lambda_event, lambda_context)

    peh_table = _helpers_service_resources["dynamodb"].Table("octagon-PipelineExecutionHistory-dev-prefix")
    res = peh_table.get_item(
        Key={
            'id': 'd11111-111c-11b1-a11c-11111dg11o111'
        }
    )
    assert res['Item']['status'] == 'COMPLETED'

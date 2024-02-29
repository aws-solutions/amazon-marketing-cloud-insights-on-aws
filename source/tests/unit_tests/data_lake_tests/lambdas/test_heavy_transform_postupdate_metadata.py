# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# PURPOSE:
#   * Unit test for data_lake/stages/sdlf_heavy_transform/lambdas/postupdate_metadata/handler.py
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/lambdas/test_heavy_transform_postupdate_metadata.py
import os
from datetime import datetime

import boto3
import pytest
from unittest.mock import Mock
from moto import mock_aws
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
                "status": "FAILED",
                "status_last_updated_timestamp": "COMPLETED#2023-05-23T02:24:52.320Z",
                "success": "true",
                "ttl": 1695176682,
                "version": 5
            }
        )

        yield ddb


@pytest.fixture()
def _mock_s3_client():
    s3_client = get_service_client('s3')
    s3_client.head_object = Mock(
        return_value={
            'ContentLength': 1,
            'LastModified': datetime.now(),
        }
    )
    return s3_client


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
def _s3_resource():
    with mock_aws():
        s3 = boto3.resource('s3', 'us-east-1')
        s3.create_bucket(Bucket="stage_bucket")
        s3_object = s3.Object('stage_bucket', "post-stage/adtech/datasetA/filename/filename-parquet")
        s3_object.put(Body="pre-stage file content")
        yield s3


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_sts_client, _dynamodb_resource, _mock_ssm_client, _mock_s3_client, _s3_resource):
    monkeypatch.setitem(_helpers_service_clients, 'sts', _mock_sts_client)
    monkeypatch.setitem(_helpers_service_resources, 'dynamodb', _dynamodb_resource)
    monkeypatch.setitem(_helpers_service_clients, 'ssm', _mock_ssm_client)
    monkeypatch.setitem(_helpers_service_clients, 's3', _mock_s3_client)
    monkeypatch.setitem(_helpers_service_resources, 's3', _s3_resource)


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
                    "pre-stage/adtech/datasetA/filename_parsed"
                ],
                "team": "adtech",
                "pipeline": "insights",
                "pipeline_stage": "StageB",
                "dataset": "datasetA",
                "env": "dev",
                "job": {
                    "processedKeysPath": "post-stage/adtech/datasetA",
                    "jobDetails": {
                        "jobName": "prefix-adtech-datasetA-glue-job",
                        "jobRunId": "jr_a_id",
                        "jobStatus": "SUCCEEDED",
                        "tables": [
                            "filename"
                        ]
                    },
                    "peh_id": "d11111-111c-11b1-a11c-11111dg11o111"
                }
            }
        }
    ],
)
def test_handler(lambda_event, lambda_context, _mock_clients, _dynamodb_resource):
    from data_lake.stages.sdlf_heavy_transform.lambdas.postupdate_metadata.handler import lambda_handler, \
        logger as lambda_function_logger
    lambda_function_logger.propagate = True

    response = lambda_handler(lambda_event, lambda_context)
    peh_table = _helpers_service_resources["dynamodb"].Table("octagon-PipelineExecutionHistory-dev-prefix")
    res = peh_table.get_item(
        Key={
            'id': 'd11111-111c-11b1-a11c-11111dg11o111'
        }
    )
    assert res['Item']['status'] == 'COMPLETED'
    assert response == 200

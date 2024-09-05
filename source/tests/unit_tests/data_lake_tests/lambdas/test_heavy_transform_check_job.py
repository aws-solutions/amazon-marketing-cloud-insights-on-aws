# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import boto3
import pytest
from unittest.mock import Mock, MagicMock
import sys
from moto import mock_aws
from dataclasses import dataclass
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients, _helpers_service_resources


@pytest.fixture()
def _mock_glue():
    glue_client = get_service_client('glue')
    glue_client.get_job_run = Mock(
        return_value={
            'JobRun': {
                'JobRunState': 'RUNNING',
            }
        }
    )
    return glue_client


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
    return {}


@pytest.fixture()
def _mock_ssm_client():
    ssm_client = get_service_client('ssm')
    ssm_client.get_parameter = Mock(
        side_effect=side_effect
    )
    return ssm_client


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_sts_client, _dynamodb_resource, _mock_ssm_client, _mock_glue):
    monkeypatch.setitem(_helpers_service_clients, 'sts', _mock_sts_client)
    monkeypatch.setitem(_helpers_service_resources, 'dynamodb', _dynamodb_resource)
    monkeypatch.setitem(_helpers_service_clients, 'ssm', _mock_ssm_client)
    monkeypatch.setitem(_helpers_service_clients, 'glue', _mock_glue)


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
            "body": {
                "bucket": "stage_bucket",
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
def test_handler(lambda_event, lambda_context, _mock_clients, _dynamodb_resource, _mock_imports):
    from data_lake.stages.sdlf_heavy_transform.lambdas.check_job.handler import lambda_handler, \
        logger as lambda_function_logger
    lambda_function_logger.propagate = True

    lambda_handler(lambda_event, lambda_context)
    peh_table = _helpers_service_resources["dynamodb"].Table("octagon-PipelineExecutionHistory-dev-prefix")
    res = peh_table.get_item(
        Key={
            'id': 'd11111-111c-11b1-a11c-11111dg11o111'
        }
    )
    assert res['Item']['status'] == 'COMPLETED'

    _helpers_service_clients["ssm"].get_parameter.assert_called()
    _helpers_service_clients["sts"].get_caller_identity.assert_called()


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "body": {
                "bucket": "raw_bucket",
                "key": "filename",
                "timestamp": 1684808687458,
                "last_modified_date": "2023-05-23T02:24:43Z+00:00",
                "id": "s3://raw_bucket/filename",
                "pipeline_stage": "StageA",
                "peh_id": "d11111-111c-11b1-a11c-11111dg11o111"
            }
        }
    ],
)
def test_handler_fail_fetch_event_data(lambda_event, lambda_context, _mock_clients, _dynamodb_resource, _mock_imports):
    from data_lake.stages.sdlf_heavy_transform.lambdas.check_job.handler import lambda_handler, \
        logger as lambda_function_logger
    lambda_function_logger.propagate = True

    with pytest.raises(KeyError):
        lambda_handler(lambda_event, lambda_context)

    peh_table = _helpers_service_resources["dynamodb"].Table("octagon-PipelineExecutionHistory-dev-prefix")
    res = peh_table.get_item(
        Key={
            'id': 'd11111-111c-11b1-a11c-11111dg11o111'
        }
    )
    assert res['Item']['status'] == 'COMPLETED'

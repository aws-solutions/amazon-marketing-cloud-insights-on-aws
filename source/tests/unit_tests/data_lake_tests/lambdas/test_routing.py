# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# ###############################################################################
# PURPOSE:
#   * Unit test for data_lake/pipelines/lambdas/routing/handler.py
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/lambdas/test_routing.py

import sys
import pytest
import boto3
from datetime import datetime
from unittest.mock import Mock, MagicMock
from moto import mock_aws
from aws_solutions.core.helpers import get_service_client


def side_effect(*args, **kwargs):
    if kwargs["Name"].endswith('CustomerConfig'):
        return {
            'Parameter': {
                'Value': 'prefix-data-lake-customer-config-dev',
            }
        }
    return {}


@pytest.fixture(autouse=True)
def _mocked_cloudwatch_metrics(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


@pytest.fixture()
def _mock_ssm_client():
    ssm_client = get_service_client('ssm')
    ssm_client.get_parameter = Mock(
        side_effect=side_effect
    )
    return ssm_client


@pytest.fixture()
def _mock_sqs_resource():
    with mock_aws():
        sqs = boto3.resource('sqs', 'us-east-1')
        sqs.create_queue(
            QueueName='prefix-adtech-insights-queue-a.fifo',
            Attributes={'FifoQueue': 'true'}
        )
        yield sqs


@pytest.fixture()
def _mock_dynamodb_resource():
    with mock_aws():
        ddb = boto3.resource('dynamodb', 'us-east-1')

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
def _mock_clients_and_resources(monkeypatch, _mock_ssm_client, _mock_sqs_resource, _mock_dynamodb_resource):
    monkeypatch.setattr("data_lake.pipelines.lambdas.routing.handler.ssm", _mock_ssm_client)
    monkeypatch.setattr("data_lake.pipelines.lambdas.routing.handler.dynamodb", _mock_dynamodb_resource)
    monkeypatch.setattr("data_lake.pipelines.lambdas.routing.handler.sqs", _mock_sqs_resource)


@pytest.fixture()
def _mock_functions(monkeypatch):
    def mock_catalog_item(s3_event, message):
        return {'bucket': 'prefix-raw-bucket',
                'key': 'adtech/datasetA/file',
                'stage': 'raw'
                }

    def mock_get_item(table, team, dataset):
        return "insights"

    monkeypatch.setattr("data_lake.pipelines.lambdas.routing.handler.catalog_item", mock_catalog_item)
    monkeypatch.setattr("data_lake.pipelines.lambdas.routing.handler.get_item", mock_get_item)


@pytest.mark.parametrize(
    "lambda_event",
    [
        {   
            'detail-type': 'AWS API Call via CloudTrail',
            'time': '2023-05-23T02:24:33Z',
            'detail': {
                'eventName': 'PutObject',
                'requestParameters': {
                    'bucketName': 'prefix-raw-bucket',
                    'key': 'adtech/datasetA/file'
                }
            }
        }
    ],
)

def test_handler(lambda_event, _mock_clients_and_resources, _mock_functions, _mock_sqs_resource):
    from data_lake.pipelines.lambdas.routing.handler import lambda_handler

    lambda_handler(lambda_event, None)
    messages_in_queue = _mock_sqs_resource.get_queue_by_name(
        QueueName='prefix-adtech-insights-queue-a.fifo').receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert len(messages_in_queue) == 1


def test_get_item(_mock_dynamodb_resource):
    from data_lake.pipelines.lambdas.routing.handler import get_item

    table = _mock_dynamodb_resource.Table("octagon-Datasets-dev-prefix")
    response = get_item(table, "adtech", "datasetA")
    assert response == "insights"


def test_delete_item(_mock_dynamodb_resource):
    from data_lake.pipelines.lambdas.routing.handler import delete_item

    table = _mock_dynamodb_resource.Table("octagon-ObjectMetadata-dev-prefix")
    table.put_item(
        Item={
            'id': "s3://raw_bucket/adtech/datasetA/file"
        }
    )
    delete_item(table, {'id': "s3://raw_bucket/adtech/datasetA/file"})
    assert table.item_count == 0


def test_put_item(_mock_dynamodb_resource):
    from data_lake.pipelines.lambdas.routing.handler import put_item

    message = {'bucket': 'prefix-raw-bucket',
               'key': 'adtech/datasetA/file',
               'id': "s3://raw_bucket/adtech/datasetA/file"
               }
    table = _mock_dynamodb_resource.Table("octagon-ObjectMetadata-dev-prefix")
    put_item(table, message, "id")
    assert table.item_count == 1


def test_parse_s3_event_cloudtrail():
    s3_event = {
        'detail-type': 'AWS API Call via CloudTrail',
        'detail': {
            'requestParameters': {
                'bucketName': 'XXXXXXXXX',
                'key': 'path/to/file.txt'
            }
        },
        'time': '2023-04-25T12:34:56Z'
    }
    expected_output = {
        'bucket': 'XXXXXXXXX',
        'key': 'path/to/file.txt',
        'timestamp': int(round(datetime.utcnow().timestamp() * 1000, 0)),
        'last_modified_date': '2023-04-25T12:34:56Z+00:00'
    }
    
    from data_lake.pipelines.lambdas.routing.handler import parse_s3_event
    try:
        assert parse_s3_event(s3_event)["bucket"] == expected_output
    except AssertionError:
        for expected_key in ["bucket", "key", "last_modified_date"]:
            assert parse_s3_event(s3_event)[expected_key] == expected_output[expected_key]

def test_parse_s3_event_eventbridge():
    s3_event = {
        'detail-type': 'Object Created',
        'detail': {
            'bucket': {
                'name': 'another-bucket'
            },
            'object': {
                'key': 'folder/file.csv'
            }
        },
        'time': '2023-04-25T18:25:43Z'
    }
    expected_output = {
        'bucket': 'another-bucket', 
        'key': 'folder/file.csv',
        'timestamp': int(round(datetime.utcnow().timestamp() * 1000, 0)),
        'last_modified_date': '2023-04-25T18:25:43Z+00:00'
    }
    
    from data_lake.pipelines.lambdas.routing.handler import parse_s3_event

    try:
        assert parse_s3_event(s3_event)["bucket"] == expected_output
    except AssertionError:
        for expected_key in ["bucket", "key", "last_modified_date"]:
            assert parse_s3_event(s3_event)[expected_key] == expected_output[expected_key]
    
    
def test_parse_s3_event_invalid():
    s3_event = {
        'detail-type': 'SomeInvalidType'
    }
    
    from data_lake.pipelines.lambdas.routing.handler import parse_s3_event
    with pytest.raises(KeyError):
        parse_s3_event(s3_event)

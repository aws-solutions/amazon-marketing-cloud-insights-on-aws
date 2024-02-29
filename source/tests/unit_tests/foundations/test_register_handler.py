# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from moto import mock_aws
import boto3
from data_lake.foundations.lambdas.register.handler import on_update, on_delete, on_create
from aws_solutions.core.helpers import _helpers_service_resources


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
        yield ddb


@pytest.fixture()
def _mock_clients(monkeypatch, _dynamodb_resource):
    monkeypatch.setitem(_helpers_service_resources, 'dynamodb', _dynamodb_resource)


@pytest.mark.parametrize(
    "lambda_event",
    [
        {'RequestType': 'Create',
         'ResourceType': 'AWS::CloudFormation::CustomResource', 'ResourceProperties': {
            'RegisterProperties': {'name': 'adtech-insights-stage-a', 'description': 'prefix data lake light transform',
                                   'id': 'sdlf-stage-a', 'type': 'octagon_pipeline', 'version': '1',
                                   'status': 'ACTIVE'}}}
    ],
)
def test_on_create(lambda_event, _mock_clients, _dynamodb_resource):
    response = on_create(lambda_event, None)
    table = _dynamodb_resource.Table("octagon-Pipelines-dev-prefix")
    assert table.item_count == 1
    assert response == {'PhysicalResourceId': 'sdlf-stage-a-ddb-item'}


@pytest.mark.parametrize(
    "lambda_event",
    [
        {'RequestType': 'Update',
         'ResourceType': 'AWS::CloudFormation::CustomResource', 'ResourceProperties': {
            'RegisterProperties': {'name': 'adtech-insights-stage-a', 'description': 'prefix data lake light transform',
                                   'id': 'sdlf-stage-a', 'type': 'octagon_pipeline', 'version': '1',
                                   'status': 'ACTIVE'}},
         'PhysicalResourceId': 'sdlf-stage-a-ddb-item',
         }
    ],
)
def test_on_update(lambda_event, _mock_clients, _dynamodb_resource, caplog):
    on_update(lambda_event, None)
    table = _dynamodb_resource.Table("octagon-Pipelines-dev-prefix")
    assert table.item_count == 1
    assert "Update resource" in caplog.text


@pytest.mark.parametrize(
    "lambda_event",
    [
        {'RequestType': 'Delete',
         'ResourceType': 'AWS::CloudFormation::CustomResource', 'ResourceProperties': {
            'RegisterProperties': {'name': 'adtech-insights-stage-a', 'description': 'prefix data lake light transform',
                                   'id': 'sdlf-stage-a', 'type': 'octagon_pipeline', 'version': '1',
                                   'status': 'ACTIVE'}},
         'PhysicalResourceId': 'sdlf-stage-a-ddb-item',
         }
    ],
)
def test_on_delete(lambda_event, _mock_clients, _dynamodb_resource, caplog):
    table = _dynamodb_resource.Table("octagon-Pipelines-dev-prefix")
    table.put_item(Item={'name': 'adtech-insights-stage-a', 'description': 'amci1 data lake light transform',
                         'id': 'sdlf-stage-a', 'type': 'octagon_pipeline', 'version': '1',
                         'status': 'ACTIVE'})

    on_delete(lambda_event, None)

    table = _dynamodb_resource.Table("octagon-Pipelines-dev-prefix")
    assert table.item_count == 0

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import sys

import boto3
import pytest
from unittest.mock import Mock, MagicMock
from moto import mock_sqs
from aws_solutions.core.helpers import get_service_client, get_service_resource, _helpers_service_clients, \
    _helpers_service_resources


@pytest.fixture(autouse=True)
def mock_env_variables():
    os.environ["RESOURCE_PREFIX"] = "prefix"


def side_effect(*args, **kwargs):
    if kwargs["Name"].endswith('DLQ'):
        return {
            'Parameter': {
                'Value': 'stage_dlq_name.fifo',
            }
        }
    if kwargs["Name"].endswith('Queue'):
        return {
            'Parameter': {
                'Value': 'stage_queue_name.fifo',
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
def _mock_ssm(monkeypatch, _mock_ssm_client):
    monkeypatch.setitem(_helpers_service_clients, 'ssm', _mock_ssm_client)


@pytest.fixture()
def _mock_sqs_client_no_message():
    with mock_sqs():
        sqs = boto3.resource('sqs', 'us-east-1')
        sqs.create_queue(
            QueueName='stage_dlq_name.fifo',
            Attributes={'FifoQueue': 'true'}
        )
        yield sqs


@pytest.fixture()
def _mock_sqs_no_message(monkeypatch, _mock_sqs_client_no_message):
    monkeypatch.setitem(_helpers_service_resources, 'sqs', _mock_sqs_client_no_message)


@pytest.fixture()
def __mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


@pytest.mark.parametrize(
    "lambda_event",
    [
        "{\"bucket\": \"raw_bucket\", \"key\": \"file_name\", \"timestamp\": 1684808686998, \"last_modified_date\": \"2023-05-23T02:24:43Z+00:00\", \"id\": \"s3://file_name\", \"stage\": \"raw\", \"team\": \"adtech\", \"dataset\": \"newdataset\", \"pipeline\": \"insights\", \"env\": \"dev\", \"pipeline_stage\": \"StageA\"}"
    ],
)
def test_handler_success(lambda_event, __mock_imports, _mock_ssm, _mock_sqs_no_message, _mock_sqs_client_no_message):
    from data_lake.stages.sdlf_light_transform.lambdas.error.handler import lambda_handler
    lambda_handler(lambda_event, None)
    messages_in_stage_queue = _mock_sqs_client_no_message.get_queue_by_name(
        QueueName='stage_dlq_name.fifo').receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert len(messages_in_stage_queue) == 1

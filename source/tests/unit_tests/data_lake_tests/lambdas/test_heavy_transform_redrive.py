# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import sys

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime
from moto import mock_aws
import boto3
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients, _helpers_service_resources


@pytest.fixture()
def _mock_stepfunctions_client():
    client = get_service_client('stepfunctions')
    client.start_execution = Mock(
        return_value={
            'executionArn': 'state_machine_execution_arn',
            'startDate': datetime(2023, 1, 1)
        }
    )
    return client


def _side_effect(*args, **kwargs):
    if kwargs["Name"].endswith('DLQ'):
        return {
            'Parameter': {
                'Value': 'stage_dlq_name.fifo',
            }
        }
    if kwargs["Name"].endswith('SM'):
        return {
            'Parameter': {
                'Value': 'state_machine_arn',
            }
        }
    return {}


@pytest.fixture()
def _mock_ssm_client():
    ssm_client = get_service_client('ssm')
    ssm_client.get_parameter = Mock(
        side_effect=_side_effect
    )
    return ssm_client


@pytest.fixture()
def _mock_sqs_client():
    with mock_aws():
        sqs = boto3.resource('sqs', 'us-east-1')
        sqs.create_queue(
            QueueName='stage_dlq_name.fifo',
            Attributes={'FifoQueue': 'true'}
        )

        queue = sqs.get_queue_by_name(QueueName='stage_dlq_name.fifo')
        queue.send_message(MessageBody="{\"test_redrive\": \"stage_dlq_message_body\"}",
                           MessageGroupId="test_stage_dlq_group_id",
                           MessageDeduplicationId="test_stage_dlq_message_deduplication_id")

        yield sqs


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_stepfunctions_client, _mock_ssm_client, _mock_sqs_client):
    monkeypatch.setitem(_helpers_service_clients, 'stepfunctions', _mock_stepfunctions_client)
    monkeypatch.setitem(_helpers_service_clients, 'ssm', _mock_ssm_client)
    monkeypatch.setitem(_helpers_service_resources, 'sqs', _mock_sqs_client)


@pytest.fixture()
def _mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "dataset": "datasetA",
        }
    ],
)
def test_handler(lambda_event, _mock_imports, _mock_clients):
    from data_lake.stages.sdlf_heavy_transform.lambdas.redrive.handler import lambda_handler
    lambda_handler(lambda_event, None)
    _helpers_service_clients["stepfunctions"].start_execution.assert_called_once()

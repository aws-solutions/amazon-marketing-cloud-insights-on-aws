# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients


@pytest.fixture()
def _mock_stepfunctions_client():
    client = get_service_client('stepfunctions')
    client.start_execution = Mock(
        return_value={
            'executionArn': 'state_machine_execution_arn',
            'startDate': datetime(2023, 1, 1)
        }
    )
    client.list_executions = Mock(
        return_value={
            'executions': [],
            "nextToken": "any"
        }
    )
    return client


@pytest.fixture()
def _mock_ssm_client():
    ssm_client = get_service_client('ssm')
    ssm_client.get_parameter = Mock(
        return_value={
            'Parameter': {
                'Value': 'state_machine_arn',
            }
        }
    )
    return ssm_client


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_stepfunctions_client, _mock_ssm_client):
    monkeypatch.setitem(_helpers_service_clients, 'stepfunctions', _mock_stepfunctions_client)
    monkeypatch.setitem(_helpers_service_clients, 'ssm', _mock_ssm_client)


@pytest.fixture()
def _mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "Records": [
                {
                    "body": "{\"bucket\": \"raw_bucket\", \"key\": \"file1\", \"timestamp\": 1684808678810, \"last_modified_date\": \"date\", \"id\": \"s3://file1\", \"stage\": \"raw\", \"team\": \"adtech\", \"dataset\": \"newdataset\", \"pipeline\": \"insights\", \"env\": \"dev\", \"pipeline_stage\": \"StageA\"}"
                }
            ],
        }
    ],
)
def test_handler_success(lambda_event, _mock_imports, _mock_clients):
    from data_lake.stages.sdlf_light_transform.lambdas.routing.handler import lambda_handler
    response = lambda_handler(lambda_event, None)
    assert response is None
    _helpers_service_clients["ssm"].get_parameter.assert_called_once()
    _helpers_service_clients["stepfunctions"].start_execution.assert_called_once()


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "Records": [
                {
                    "body": "{\"bucket\": \"raw_bucket\", \"key\": \"file1\", \"timestamp\": 1684808678810, \"last_modified_date\": \"date\", \"id\": \"s3://file1\", \"stage\": \"raw\", \"dataset\": \"newdataset\", \"pipeline\": \"insights\", \"env\": \"dev\", \"pipeline_stage\": \"StageA\"}"
                }
            ],
        }
    ],
)
def test_handler_fail(lambda_event, _mock_imports, _mock_clients):
    from data_lake.stages.sdlf_light_transform.lambdas.routing.handler import lambda_handler
    with pytest.raises(KeyError, match=r'team'):
        lambda_handler(lambda_event, None)

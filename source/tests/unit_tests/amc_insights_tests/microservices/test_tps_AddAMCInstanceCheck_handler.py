# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for AddAMCInstanceCheck Handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_tps_AddAMCInstanceCheck_handler.py
###############################################################################
import sys
from unittest.mock import MagicMock, patch

import pytest


def mock_environ():
    """
    This function is the mocked (replaced) function for returning environment variables
    """
    return {
        "AWS_REGION": "us-east-1",
        "SOLUTION_ID": "SO999",
        "SOLUTION_VERSION": "v9.9.9",
    }


@pytest.fixture()
def __mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


def test_get_cfn_client(__mock_imports):
    from amc_insights.microservices.tenant_provisioning_service.lambdas.AddAMCInstanceCheck.handler import \
        get_cfn_client
    with patch('os.environ', new=mock_environ()), patch(
            'aws_solutions.core.helpers.get_service_client') as mock_client:
        mock_client().return_value = MagicMock()
        event = {
            'body': {
                'stackId': {
                    'StackId': 'invalid-stack-id'
                }
            },
            'bucketRegion': 'us-east-1'
        }
        get_cfn_client(event)
        mock_client.assert_called_once()


@patch('amc_insights.microservices.tenant_provisioning_service.lambdas.AddAMCInstanceCheck.handler.get_cfn_client')
def test_handler(mock_get_cfn_client, __mock_imports):
    from amc_insights.microservices.tenant_provisioning_service.lambdas.AddAMCInstanceCheck.handler import handler
    event = {
        'body': {
            'stackId': {
                'StackId': 'valid-stack-id'
            }
        },
        'bucketRegion': 'us-east-1'
    }
    mock_get_cfn_client.describe_stacks().return_value = {
        'Stacks': [{
            'StackStatus': 'CREATE_COMPLETE'
        }]
    }
    handler(event, None)
    mock_get_cfn_client.describe_stacks.assert_called_once()

    resp = handler({}, None)
    assert resp == "CREATE_COMPLETE"

    resp = handler({"body": {"stackId": {"StackId": "12345"}}, "bucketRegion": "us-east-2"}, None)
    assert resp == "FAILED"


def test_handler_2():
    from amc_insights.microservices.tenant_provisioning_service.lambdas.AddAMCInstanceCheck.handler import handler
    resp = handler({"body": {"stackId": {"StackId": "12345"}}}, None)
    assert resp == "FAILED"

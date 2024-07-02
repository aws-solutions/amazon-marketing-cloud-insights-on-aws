# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for MicroServices/WFM/UpdateWorkflow handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_wfm_cancel_workflow_execution_handler.py
###############################################################################
import os
from dataclasses import dataclass

import pytest
from unittest.mock import MagicMock
import sys


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['EXECUTION_STATUS_TABLE'] = "execution_status_tbl_name"


@pytest.fixture()
def _mock_imports():
    mocked_wfm_utilities = MagicMock()
    sys.modules['wfm_utilities'] = mocked_wfm_utilities
    mocked_wfm_utilities.wfm_utilities.Utils.return_value.dynamodb_put_item.return_value = True

    mock_wfm_amc_api_interface = MagicMock()
    sys.modules['wfm_amc_api_interface'] = mock_wfm_amc_api_interface
    mock_wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIResponse.return_value = MagicMock()
    mock_wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.return_value = MagicMock(
        config={"customerId": 12345})

    mock_wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.return_value.cancel_workflow_execution.return_value = MagicMock(
        success=True, response={'workflowExecutionId': "6789", "workflowId": "12345"})

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
            "customerConfig": {
            },
            "executionRequest": {
                "workflowExecutionId": "12345"
            }
        }
    ],
)
def test_handler(lambda_event, lambda_context, _mock_imports):
    from amc_insights.microservices.workflow_manager_service.lambdas.CancelWorkflowExecution.handler import handler
    event = handler(lambda_event, lambda_context)
    assert event == {
        'customerConfig': {
                            },
        'executionRequest': {'workflowExecutionId': '6789'}, 'EXECUTION_RUNNING_LAMBDA_NAME': 'lambda-function-name',
        'workflowExecutionId': '6789', 'workflowId': '12345'}

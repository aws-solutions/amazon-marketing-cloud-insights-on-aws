# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for MicroServices/WFM/GetWorkflow handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/workflow_manager_service/lambdas/test_GetExecutionSummary.py
###############################################################################


import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from moto import mock_aws
from datetime import datetime, timedelta


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['WORKFLOWS_TABLE_NAME'] = "wf_tbl_name"


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

    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics

    mocked_microservice_shared = MagicMock()
    sys.modules['microservice_shared'] = mocked_microservice_shared


@mock_aws
def test_handler(_mock_imports):
    from amc_insights.microservices.workflow_manager_service.lambdas.GetExecutionSummary.handler import handler

    customer_config = {
        'Item': {}
    }

    test_event = {
        "workflowId": 12345,
        "customerConfig": customer_config,
    }

    with (
        patch("wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIResponse") as amc_response_mock,
        patch("wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs") as amc_interface_mock,
    ):
        amc_interface_mock.return_value = MagicMock(config={"customerId": 12345})
        with patch("urllib3.PoolManager"):
            amc_response_mock.return_value = MagicMock()
            amc_interface_mock.return_value.get_execution_status_by_minimum_create_time.return_value = MagicMock(
                success=True, response={
                    "workflow": {"query": ["test_sql_query"], "workflowId": 12345}})

            event = handler(test_event, None)

            assert event == {'workflowId': 12345, 'customerConfig': {'Item': {}},
                             'executionsSummary': {
                                 'executionsSince': (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%dT%H:%M:%S'),
                                 'totalExecutions': 0,
                                 'totalRunningorPending': 0, 'succeededExecutions': 0,
                                 'runningExecutions': 0, 'pendingExecutions': 0,
                                 'failedExecutions': 0, 'rejectedExecutions': 0,
                                 'cancelledExecutions': 0}}

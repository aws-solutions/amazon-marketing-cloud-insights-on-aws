# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for MicroServices/WFM/GetWorkflow handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/workflow_manager_service/lambdas/test_GetWorkflow.py
###############################################################################


import os
import sys

import boto3
import pytest
from unittest.mock import MagicMock, patch
from moto import mock_aws


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
    from amc_insights.microservices.workflow_manager_service.lambdas.GetWorkflow.handler import handler

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
            amc_interface_mock.return_value.get_workflow.return_value = MagicMock(success=True, response={
                "workflow": {"query": ["test_sql_query"], "workflowId": 12345}})
            dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
            params = {
                "TableName": os.environ['WORKFLOWS_TABLE_NAME'],
                "KeySchema": [
                    {"AttributeName": "workflowId", "KeyType": "HASH"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "workflowId", "AttributeType": "S"},
                ],
                "BillingMode": "PAY_PER_REQUEST",
            }
            dynamodb.create_table(**params)

            event = handler(test_event, None)

            assert event == {'workflowId': 12345, 'customerConfig': {'Item': {}},
                             'workflow': {'query': ['test_sql_query'], 'workflowId': 12345}}

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for MicroServices/WFM/UpdateWorkflow handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_wfm_update_workflow_handler.py
###############################################################################


import os
import sys

import boto3
import pytest
from unittest.mock import MagicMock, patch
from moto import mock_aws


sys.path.insert(0, "./infrastructure/amc_insights/microservices/workflow_management_service/lambda_layers/wfm_layer/python/")

@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['WORKFLOWS_TABLE_NAME'] = "wf_tbl_name"


@mock_aws
def test_handler():
    from amc_insights.microservices.workflow_manager_service.lambdas.UpdateWorkflow.handler import handler

    customer_config = {

    }

    test_event = {
        "workflowId": 12345,
        "customerConfig": customer_config,
        "workflowRequest": {"workflowDefinition": {"workflowId": 12345}}
    }
    

    with (
        patch("wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIResponse") as amc_response_mock,
        patch("wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs") as amc_interface_mock,
        patch("wfm_utilities.wfm_utilities.Utils")
    ):
        amc_interface_mock.return_value = MagicMock(config={"customerId": 12345})
        with patch("urllib3.PoolManager"):
            amc_response_mock.return_value = MagicMock()
            amc_response_mock.return_value.update_workflow.return_value = MagicMock(success=True, response={})
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
            event = handler(event=test_event, context=MagicMock(function_name="test_name"))
            assert event == {'workflowId': 12345, 'customerConfig': {}, 'workflowRequest': {'workflowDefinition': {'workflowId': 12345}}, 'EXECUTION_RUNNING_LAMBDA_NAME': 'test_name'}
            amc_response_mock.return_value.update_workflow.return_value = MagicMock(success=False, response={})
            test_event["workflowRequest"] = {"workflowDefinition": {"workflowId": 12345}}
            event = handler(event=test_event, context=MagicMock(function_name="test_name"))
            assert event == {'workflowId': 12345, 'customerConfig': {}, 'workflowRequest': {'workflowDefinition': {'workflowId': 12345}}, 'EXECUTION_RUNNING_LAMBDA_NAME': 'test_name'}
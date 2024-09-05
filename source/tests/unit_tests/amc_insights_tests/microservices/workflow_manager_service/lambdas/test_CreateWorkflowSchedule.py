# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for MicroServices/WFM/CreateWorkflowSchedule handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/workflow_manager_service/lambdas/test_CreateWorkflowSchedule.py
###############################################################################
import os
import boto3
import pytest
from unittest.mock import MagicMock
import sys
from moto import mock_aws


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['INVOKE_WORKFLOW_EXECUTION_SM_LAMBDA_ARN'] = f"arn:aws:lambda:{os.environ['AWS_DEFAULT_REGION']}:{os.environ['MOTO_ACCOUNT_ID']}/invoke_workflow_execution_sm"
    os.environ['RULE_PREFIX'] = os.environ["RESOURCE_PREFIX"]

@pytest.fixture()
def _mock_imports():
    mocked_wfm_utilities = MagicMock()
    sys.modules['wfm_utilities'] = mocked_wfm_utilities
    mocked_wfm_utilities.wfm_utilities.Utils.return_value.dynamodb_put_item.return_value = True

    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics
    

@mock_aws
def test_handler(_mock_imports):
    from amc_insights.microservices.workflow_manager_service.lambdas.CreateWorkflowSchedule.handler import handler

    events_client = boto3.client('events')
    
    rule_name = 'test-rule'
    rule_description = 'Test Description'
    schedule_expression = 'cron(0/15 * * * ? *)'
    event = {
        "execution_request": {
            "customerId": 'test_customer',
            "requestType": "createExecution",
            "createExecutionRequest": {
                "timeWindowStart": "FIRSTDAYOFOFFSETMONTH(-2)",
                "timeWindowEnd": "FIRSTDAYOFOFFSETMONTH(-1)",
                "timeWindowType": "EXPLICIT",
                "workflow_executed_date": "now()",
                "timeWindowTimeZone": "America/New_York",
                "workflowId": "test-wfm-1",
                "ignoreDataGaps": "True",
                "workflowExecutionTimeoutSeconds": "86400",
                "parameterValues": {
                    "workflow_executed_date": "now()"
                }
            },
        },
        "rule_description": rule_description,
        "schedule_expression": schedule_expression,
        "rule_name": rule_name
    }

    handler(event, None)
    expected_rule_name = f"{os.environ['RULE_PREFIX']}-wfm-{rule_name}"
    check = events_client.list_rule_names_by_target(TargetArn=os.environ['INVOKE_WORKFLOW_EXECUTION_SM_LAMBDA_ARN'])
    assert check['RuleNames'] == [expected_rule_name]
    check = events_client.describe_rule(Name=expected_rule_name)
    assert check['State'] == 'ENABLED'


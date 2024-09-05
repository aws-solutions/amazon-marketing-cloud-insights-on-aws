# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for MicroServices/WFM/DeleteWorkflowSchedule handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/workflow_manager_service/lambdas/test_DeleteWorkflowSchedule.py
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
    from amc_insights.microservices.workflow_manager_service.lambdas.DeleteWorkflowSchedule.handler import handler

    events_client = boto3.client('events')
    
    # create rule to test deletion in the handler function
    rule_name = f"{os.environ['RULE_PREFIX']}-wfm-test-rule"
    events_client.put_rule(
        Name=rule_name,
        ScheduleExpression='cron(0/15 * * * ? *)',
        State='ENABLED',
        Description='test',
    )
    events_client.put_targets(
        Rule=rule_name,
        Targets=[{
        "Arn": os.environ['INVOKE_WORKFLOW_EXECUTION_SM_LAMBDA_ARN'],
        "Id": "1",
        "Input": "test"
    }]
    )
    
    event = {
        'rule_name': 'test-rule'
    }
    handler(event, None)
    
    check = events_client.list_rule_names_by_target(TargetArn=os.environ['INVOKE_WORKFLOW_EXECUTION_SM_LAMBDA_ARN'])
    assert check['RuleNames'] == []


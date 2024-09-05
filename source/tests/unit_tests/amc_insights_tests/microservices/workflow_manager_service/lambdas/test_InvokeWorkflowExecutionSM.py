# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for InvokeWorkflowExecutionSM
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/workflow_manager_service/lambdas/test_InvokeWorkflowExecutionSM.py

import os
import sys
import unittest
from unittest.mock import patch, Mock

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")
sys.path.insert(0, "./infrastructure/amc_insights/microservices/workflow_manager_service/lambda_layers/wfm_layer/python/")


def mock_stepfunctions_client():
    stepfunctions_client = get_service_client('stepfunctions')
    stepfunctions_client.start_execution = Mock()
    
    return stepfunctions_client

def mock_cloudwatch_client(): # mock cloudwatch for the cloudwatch_metrics module
    cloudwatch_client = get_service_client('cloudwatch')
    cloudwatch_client.put_metric_data = Mock()
    
    return cloudwatch_client

def mock_get_service_client(service_name, *args, **kwargs):
    if service_name == 'stepfunctions': 
        return mock_stepfunctions_client()
    elif service_name == 'cloudwatch':
        return mock_cloudwatch_client()


class TestInvokeWorkflowExecutionSM(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_get_service_client)
    def setUp(self, mock_get_service_client):   
        os.environ['STEP_FUNCTION_STATE_MACHINE_ARN'] = "STEP_FUNCTION_STATE_MACHINE_ARN"
        os.environ['DATASET_WORKFLOW_TABLE'] = "DATASET_WORKFLOW_TABLE"
        os.environ['STACK_NAME'] = "STACK_NAME"
        os.environ['METRICS_NAMESPACE'] = "METRICS_NAMESPACE"
        self.mock_stepfunctions_client = mock_stepfunctions_client()
        self.mock_cloudwatch_client = mock_cloudwatch_client()
        

    def test_handler(self):
        from amc_insights.microservices.workflow_manager_service.lambdas.InvokeWorkflowExecutionSM.handler import handler
        
        # set up our test user input with missing customerId and workflowId values
        test_event = {
            "createExecutionRequest": {
            }
        }
        
        # run the lambda function code
        handler(test_event, None)
        
        # test that input validation error is triggered before state machine is called
        self.mock_stepfunctions_client.start_execution.assert_not_called()
        
if __name__ == '__main__':
    unittest.main()

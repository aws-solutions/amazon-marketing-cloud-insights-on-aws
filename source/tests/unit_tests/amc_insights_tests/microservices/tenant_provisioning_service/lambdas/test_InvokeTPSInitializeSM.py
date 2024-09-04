# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for InvokeTPSInitializeSM/handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/tenant_provisioning_service/lambdas/test_InvokeTPSInitializeSM.py

import os
import sys
import json
import unittest
from unittest.mock import patch, Mock

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")


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


class TestInvokeTPSInitializeSM(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_get_service_client)
    def setUp(self, mock_get_service_client):   
        os.environ['STATE_MACHINE_ARN'] = "STATE_MACHINE_ARN"
        os.environ['DATASET_NAME'] = "test_dataset_name"
        os.environ['TEAM_NAME'] = "test_team_name"
        os.environ['APPLICATION_ACCOUNT'] = "1111111111"
        os.environ['DEFAULT_SNS_TOPIC'] = "test_sns_topic"
        os.environ['RESOURCE_PREFIX'] = "sqs"
        os.environ['APPLICATION_REGION'] = "us-east-1"
        self.mock_stepfunctions_client = mock_stepfunctions_client()
        self.mock_cloudwatch_client = mock_cloudwatch_client()
        
    def test_handler_default(self):
        from amc_insights.microservices.tenant_provisioning_service.lambdas.InvokeTPSInitializeSM.handler import handler
        
        # set up our test user input
        test_event = {
            "customer_details": {
                "amc": {
                        "endpoint_url": "https://abc1234567.execute-api.us-east-1.amazonaws.com/beta",
                        "aws_orange_account_id": "12390",
                        "aws_red_account_id": "33333",
                        "bucket_name": "test_bucket",
                        "instance_id": "amc122345",
                        "amazon_ads_advertiser_id": "12345",
                        "amazon_ads_marketplace_id": "12345"
                    },
                "customer_id": "12345",
                "customer_name": "some_cust_test_name",
            }
        }
        
        # run the lambda function code
        handler(test_event, None)
        
        # capture some values passed to start_execution and assert them below
        _, kwargs = self.mock_stepfunctions_client.start_execution.call_args
        
        # assert that we default to this value when bucket_exists is not passed in by user
        assert json.loads(kwargs['input'])['bucketExists'] == "true"
        # assert that we default to this value when bucket_account is not passed in by user
        assert json.loads(kwargs['input'])['bucketAccount'] == os.environ['APPLICATION_ACCOUNT']
        # assert that we default to this value when bucket_region is not passed in by user
        assert json.loads(kwargs['input'])['bucketRegion'] == os.environ['APPLICATION_REGION']
        
if __name__ == '__main__':
    unittest.main()

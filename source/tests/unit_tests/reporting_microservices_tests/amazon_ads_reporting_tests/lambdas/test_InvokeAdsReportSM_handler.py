# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Amazon Ads Reporting/Lambdas/InvokeAdsReportSM handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name reporting_microservices_tests/amazon_ads_reporting_tests/lambdas/test_InvokeAdsReportSM_handler.py
###############################################################################

import os
import sys
import json
import unittest
import pytest
from unittest.mock import patch, Mock, MagicMock

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")


@pytest.fixture(autouse=True)
def _mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics

def mock_cloudwatch_client(): 
    cloudwatch_client = get_service_client('cloudwatch')
    cloudwatch_client.put_metric_data = Mock()
    return cloudwatch_client

def mock_stepfunctions_client():
    stepfunctions_client = get_service_client('stepfunctions')
    stepfunctions_client.start_execution = Mock()
    
    return stepfunctions_client

def mock_get_service_client(service_name, *args, **kwargs):
    if service_name == 'stepfunctions': 
        return mock_stepfunctions_client()
    elif service_name == 'cloudwatch':
        return mock_cloudwatch_client()


class TestInvokeAdsReportSM(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_get_service_client)
    def setUp(self, mock_get_service_client):   
        os.environ['STEP_FUNCTION_STATE_MACHINE_ARN'] = "STEP_FUNCTION_STATE_MACHINE_ARN"
        os.environ['STACK_NAME'] = "STACK_NAME"
        os.environ['METRICS_NAMESPACE'] = "METRICS_NAMESPACE"

        self.mock_stepfunctions_client = mock_stepfunctions_client()
        self.mock_cloudwatch_client = mock_cloudwatch_client()
        
    def test_handler_defaults(self):
        from reporting_microservices.amazon_ads_reporting.reporting_service.lambdas.InvokeAdsReportSM.handler import handler
        
        # set up our test user input
        test_report_type_id = 'test_report_type_id'
        test_profile_id = '123456789'
        test_event = {
            'profileId': test_profile_id,
            'region': 'North America',
            'requestBody': {
                'configuration': {
                    'reportTypeId': test_report_type_id
                }
            }
        }
        
        # run the lambda function code
        handler(test_event, None)
        
        # capture some values passed to start_execution and assert them below
        _, kwargs = self.mock_stepfunctions_client.start_execution.call_args
        
        # assert that we default to this value when tableName is not passed in by user
        assert json.loads(kwargs['input'])['tableName'] == f"{test_profile_id}-{test_report_type_id}"
        # assert that we add GZIP_JSON file format to every request when not passed in by user
        assert json.loads(kwargs['input'])['requestBody']['configuration']['format'] == "GZIP_JSON"
        
    def test_handler_custom(self):
        from reporting_microservices.amazon_ads_reporting.reporting_service.lambdas.InvokeAdsReportSM.handler import handler
        
        # set up our test user input
        table_name = 'test_table'
        test_event = {
            'profileId': '123456789',
            'region': 'North America',
            'requestBody': {
                'configuration': {
                    'reportTypeId': 'test_report_type_id',
                    'format': "CSV"
                }
            },
            'tableName': table_name
        }
        
        # run the lambda function code
        handler(test_event, None)
        
        # capture some values passed to start_execution and assert them below
        _, kwargs = self.mock_stepfunctions_client.start_execution.call_args
        
        # assert that we use tableName if passed in by the user
        assert json.loads(kwargs['input'])['tableName'] == table_name
        # assert that we override the format to GZIP_JSON if another value is accidentally passed in (CSV, etc.)
        assert json.loads(kwargs['input'])['requestBody']['configuration']['format'] == "GZIP_JSON"
        
        
if __name__ == '__main__':
    unittest.main()

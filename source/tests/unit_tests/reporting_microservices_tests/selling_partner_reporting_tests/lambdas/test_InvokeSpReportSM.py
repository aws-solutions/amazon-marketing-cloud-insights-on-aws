# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Selling Partner Reporting/Lambdas/InvokeSpReportSM handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name reporting_microservices_tests/selling_partner_reporting_tests/lambdas/test_InvokeSpReportSM.py
###############################################################################

import os
import sys
import json
import unittest
import pytest
from unittest.mock import patch, Mock

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")


@pytest.fixture(scope="class")
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


class TestInvokeSpReportSM(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_get_service_client)
    def setUp(self, mock_get_service_client):   
        os.environ['STEP_FUNCTION_STATE_MACHINE_ARN'] = "STEP_FUNCTION_STATE_MACHINE_ARN"
        os.environ['STACK_NAME'] = "STACK_NAME"
        os.environ['METRICS_NAMESPACE'] = "METRICS_NAMESPACE"

        self.mock_stepfunctions_client = mock_stepfunctions_client()
        self.mock_cloudwatch_client = mock_cloudwatch_client()
        
    def test_handler_defaults(self):
        from reporting_microservices.selling_partner_reporting.reporting_service.lambdas.InvokeSpReportSM.handler import handler
        
        test_report_type = 'test_report_type'
        region = 'North America'
        test_event = {
            'region': region,
            'requestBody': {
                'reportType': test_report_type
            }
        }
        
        handler(test_event, None)
        
        _, kwargs = self.mock_stepfunctions_client.start_execution.call_args
        
        # assert that we default to this value when tablePrefix is not passed in by user
        assert json.loads(kwargs['input'])['tablePrefix'] == f"{region}-{test_report_type}".replace(" ","")
        
    def test_handler_custom(self):
        from reporting_microservices.selling_partner_reporting.reporting_service.lambdas.InvokeSpReportSM.handler import handler
        
        table_prefix = 'test_table_prefix'
        test_event = {
            'region': 'North America',
            'requestBody': {
                'reportType': 'test_report_type',
                'marketplaceId': 'test_marketplace_id',
            },
            'tablePrefix': table_prefix
        }
        
        handler(test_event, None)
        
        _, kwargs = self.mock_stepfunctions_client.start_execution.call_args
        
        # assert that we use tablePrefix if passed in by the user
        assert json.loads(kwargs['input'])['tablePrefix'] == table_prefix
        
        
if __name__ == '__main__':
    unittest.main()

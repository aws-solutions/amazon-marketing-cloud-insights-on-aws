# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for cloudwatch_matrics/report.py
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/custom_resource/test_cloudwatch_metrics_report.py

import os
import sys
import unittest
from unittest.mock import patch, Mock, MagicMock

from aws_solutions.core.helpers import get_service_client


def mock_cloudwatch_client():
    cloudwatch_client  = get_service_client('cloudwatch')
    cloudwatch_client.get_metric_statistics = Mock()
    cloudwatch_client.get_metric_statistics.return_value = {}

    return cloudwatch_client

def mock_secretsmanager_client():
    secretsmanager_client = get_service_client('secretsmanager')
    secretsmanager_client.get_secret_value = Mock()
    secretsmanager_client.get_secret_value.return_value = {"SecretString": 1234}
    
    return secretsmanager_client

def mock_get_service_client(service_name, *args, **kwargs):
    if service_name == 'cloudwatch': 
        return mock_cloudwatch_client()
    elif service_name == 'secretsmanager':
        return mock_secretsmanager_client()


class TestCloudwatchMetricsReport(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_get_service_client)
    def setUp(self, mock_get_service_client):   
        os.environ["SOLUTION_ID"] = "SO9999test"
        os.environ["SOLUTION_VERSION"] = "v99.99.99"
        os.environ["METRICS_NAMESPACE"] = "metrics-namespace"
        self.mock_cloudwatch_client = mock_cloudwatch_client()
        self.mock_secretsmanager_client = mock_secretsmanager_client()
        
    @patch.dict('sys.modules', {'requests': MagicMock()})
    def test_send_metrics(self):
        os.environ["SEND_ANONYMIZED_DATA"] = "Yes"
        requests = sys.modules['requests']
        requests.post = Mock()
        from amc_insights.custom_resource.cloudwatch_metrics.lambdas.report import send_metrics
        
        send_metrics()
        
        _, kwargs = self.mock_cloudwatch_client.get_metric_statistics.call_args
        
        assert kwargs['Namespace'] == "metrics-namespace"
        
    @patch.dict('sys.modules', {'requests': MagicMock()})
    def test_event_handler_with_anonymized_data(self):
        os.environ["SEND_ANONYMIZED_DATA"] = "Yes"
        requests = sys.modules['requests']
        requests.post = Mock()
        with patch('amc_insights.custom_resource.cloudwatch_metrics.lambdas.report.send_metrics') as mock_send_metrics:
            from amc_insights.custom_resource.cloudwatch_metrics.lambdas.report import event_handler
                
            event = {"test event"}
            context = {"test context"}
            
            event_handler(event, context)
        
        # assert that when SEND_ANONYMIZED_DATA is Yes, send_metrics is called once
        mock_send_metrics.assert_called_once
        
    @patch.dict('sys.modules', {'requests': MagicMock()})
    def test_event_handler_without_anonymized_data(self):
        os.environ["SEND_ANONYMIZED_DATA"] = "No"
        requests = sys.modules['requests']
        requests.post = Mock()
        with patch('amc_insights.custom_resource.cloudwatch_metrics.lambdas.report.send_metrics') as mock_send_metrics:
            from amc_insights.custom_resource.cloudwatch_metrics.lambdas.report import event_handler
                
            event = {"test event"}
            context = {"test context"}
            
            event_handler(event, context)
        
        # assert that when SEND_ANONYMIZED_DATA is No, send_metrics is not called
        mock_send_metrics.assert_not_called
        
        
if __name__ == '__main__':
    unittest.main()

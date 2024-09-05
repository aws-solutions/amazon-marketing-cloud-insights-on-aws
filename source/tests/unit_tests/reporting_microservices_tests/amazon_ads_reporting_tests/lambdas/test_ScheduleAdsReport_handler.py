# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Amazon Ads Reporting/Lambdas/ScheduleAdsReport handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name reporting_microservices_tests/amazon_ads_reporting_tests/lambdas/test_ScheduleAdsReport_handler.py
###############################################################################

import os
import sys
import unittest
from unittest.mock import patch, Mock

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
from microservice_shared.events import EventsHelper


def mock_cloudwatch_client(): # mock cloudwatch for the cloudwatch_metrics module
    cloudwatch_client = get_service_client('cloudwatch')
    cloudwatch_client.put_metric_data = Mock()
    return cloudwatch_client

def mock_clients():
    cloudwatch_client = mock_cloudwatch_client()
    _helpers_service_clients['cloudwatch'] = cloudwatch_client
    return cloudwatch_client


class ScheduleAdsReport(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_cloudwatch_client)
    def setUp(self, mock_get_service_client):   
        os.environ['INVOKE_ADS_REPORT_SM_LAMBDA_ARN'] = "INVOKE_ADS_REPORT_SM_LAMBDA_ARN"
        os.environ['METRICS_NAMESPACE'] = "METRICS_NAMESPACE"
        os.environ['RESOURCE_PREFIX'] = "RESOURCE_PREFIX"
        os.environ['DATASET'] = "DATASET"
        os.environ['REGION'] = "REGION"
        self.mock_cloudwatch_client = mock_cloudwatch_client()
    
    @patch('microservice_shared.events.EventsHelper')
    def test_handler_defaults(self, mock_EventsHelper):
        from reporting_microservices.amazon_ads_reporting.reporting_service.lambdas.ScheduleAdsReport.handler import handler
        
        rule_name = 'test_rule_name'
        rule_prefix = f"{os.environ['RESOURCE_PREFIX']}-{os.environ['DATASET']}"
        test_event = {
            'rule_name': rule_name
        }
        
        result = handler(test_event, None)
        
        # Assert a properly formated url is returned 
        assert result == f"https://{os.environ['REGION']}.console.aws.amazon.com/events/home?region={os.environ['REGION']}#/eventbus/default/rules/{rule_prefix}-{rule_name}"
        
        
if __name__ == '__main__':
    unittest.main()

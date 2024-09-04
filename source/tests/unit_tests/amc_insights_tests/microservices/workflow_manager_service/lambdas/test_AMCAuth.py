# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for AMCAuth
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/workflow_manager_service/lambdas/test_AMCAuth.py

import os
import sys
import json
import unittest
from unittest.mock import patch, Mock

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")
sys.path.insert(0, "./infrastructure/amc_insights/microservices/workflow_manager_service/lambda_layers/wfm_layer/python/")


def mock_cloudwatch_client(): # mock cloudwatch for the cloudwatch_metrics module
    cloudwatch_client = get_service_client('cloudwatch')
    cloudwatch_client.put_metric_data = Mock()
    return cloudwatch_client

def mock_get_service_client(service_name, *args, **kwargs):
    if service_name == 'cloudwatch':
        return mock_cloudwatch_client()

def mock_secrets_helper(*args, **kwargs):
    secrets_helper = Mock()
    secrets_helper.get_secret.return_value = {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "authorization_code": "test_authorization_code"
    }
    secrets_helper.update_secret = Mock()
    return secrets_helper

def mock_api_helper(*args, **kwargs):
    api_helper = Mock()
    api_helper.send_request.return_value = Mock(status=200, data=json.dumps({
        "refresh_token": "test_refresh_token",
        "access_token": "test_access_token"
    }).encode('utf-8'))
    return api_helper
    

class TestAMCAuth(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_get_service_client)
    def setUp(self, mock_get_service_client):   
        os.environ['AMC_SECRETS_MANAGER'] = "AMC_SECRETS_MANAGER"
        os.environ['RESOURCE_PREFIX'] = "RESOURCE_PREFIX"
        os.environ['METRICS_NAMESPACE'] = "METRICS_NAMESPACE"
        self.mock_cloudwatch_client = mock_cloudwatch_client()
    
    def test_handler(self):
        with patch('microservice_shared.secrets.SecretsHelper', side_effect=mock_secrets_helper) as mock_secrets_helper_class, \
             patch('microservice_shared.api.ApiHelper', side_effect=mock_api_helper) as mock_api_helper_class:
            from amc_insights.microservices.workflow_manager_service.lambdas.AMCAuth.handler import handler
            
            test_event = {
                "auth_id": "test_auth_id"
            }
        
            result = handler(test_event, None)
         
        self.assertEqual(result, test_event)
        
if __name__ == '__main__':
    unittest.main()

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# USAGE:
#   ./run-unit-tests.sh --test-file-name lambda_layer_tests/microservice_shared/test_secrets.py
###############################################################################

import unittest
from unittest.mock import patch, Mock
import sys
import json

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients
sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
from microservice_shared.utilities import LoggerUtil
from microservice_shared.api import ApiHelper

from microservice_shared.secrets import SecretsHelper


def mock_secrets_client():
    secrets_client = get_service_client('secretsmanager')
    secrets_client.get_secret_value = Mock(
        return_value={
            "SecretString": json.dumps({
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "refresh_token": "test_refresh_token",
                "authorization_code": "test_authorization_code"
            })
        }   
    )
    secrets_client.update_secret = Mock(
        return_value=None
    )
    
    return secrets_client

def mock_clients():
    secrets_client = mock_secrets_client()
    _helpers_service_clients['secretsmanager'] = secrets_client
    return secrets_client


class TestSecretsHelper(unittest.TestCase):
    
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_secrets_client)
    def setUp(self, mock_get_service_client):
        self.secret_key = "test_secret_key"
        self.secrets_helper = SecretsHelper(self.secret_key)
        mock_clients()

    @patch.object(ApiHelper, '__init__', return_value=None)
    @patch.object(LoggerUtil, 'create_logger', return_value=Mock())
    def test_init(self, mock_create_logger, mock_api_helper):
        secrets_helper = SecretsHelper(self.secret_key)

        self.assertEqual(secrets_helper.secret_key, self.secret_key)
        mock_create_logger.assert_called_once()
        mock_api_helper.assert_called_once()

    def test_validate_secrets_valid(self):
        secrets = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token"
        }
        self.secrets_helper.validate_secrets(secrets)  # No exception raised

    def test_validate_secrets_invalid(self):
        secrets = {
            "client_id": "test_client_id",
            "client_secret": "",
            "refresh_token": "test_refresh_token"
        }
        with self.assertRaises(ValueError):
            self.secrets_helper.validate_secrets(secrets)

    def test_get_secret(self):
        mock_response = {
            "SecretString": {
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "refresh_token": "test_refresh_token",
                "authorization_code": "test_authorization_code"
            }
        }

        secrets = self.secrets_helper.get_secret()

        self.assertEqual(secrets, mock_response["SecretString"])

    def test_update_secret(self):
        secret_string = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token"
        }
        
        self.secrets_helper.update_secret(secret_string)
        
        expected_secret_string = json.dumps(secret_string)
        
        self.secrets_helper.client.update_secret.assert_called_with(
            SecretId=self.secret_key,
            SecretString=expected_secret_string
        )
            
    @patch.object(ApiHelper, 'send_request')
    def test_get_access_token(self, mock_send_request):
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = json.dumps({
            "access_token": "test_access_token",
            "token_type": "bearer",
            "expires_in": 3600,
            "refresh_token": "test_refresh_token"
        }).encode('utf-8')
        mock_send_request.return_value = mock_response

        access_token = self.secrets_helper.get_access_token()

        expected_access_token = {
            "client_id": "test_client_id",
            "access_token": "test_access_token",
            "token_type": "bearer",
            "expires_in": 3600,
            "refresh_token": "test_refresh_token"
        }
        self.assertEqual(access_token, expected_access_token)
        mock_send_request.assert_called_once()

if __name__ == '__main__':
    unittest.main()

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# USAGE:
#   ./run-unit-tests.sh --test-file-name lambda_layer_tests/microservice_shared/test_api.py
###############################################################################

import unittest
from unittest.mock import patch, Mock
from urllib.parse import urlencode
import sys

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")

from microservice_shared.api import ApiHelper


class TestApiHelper(unittest.TestCase):
    
    @patch('microservice_shared.utilities.LoggerUtil.create_logger', return_value=Mock())
    def setUp(self, mock_create_logger):
        self.logger = mock_create_logger
        self.api_helper = ApiHelper()
    
    def test_encode_query_parameters_to_url(self):
        url = "http://example.com"
        query_parameters = {"param1": "value1", "param2": "value2"}
        expected_url = url + "?" + urlencode(query_parameters)
        encoded_url = self.api_helper.encode_query_parameters_to_url(url, query_parameters)
        self.assertEqual(encoded_url, expected_url)

    @patch('urllib3.PoolManager.request')
    def test_send_request_get(self, mock_request):
        request_url = "http://example.com"
        headers = {"Content-Type": "application/json"}
        http_method = "GET"
        data = None
        query_params = {"param1": "value1"}
        response_mock = Mock()
        response_mock.status = 200
        response_mock.data = b'{"key":"value"}'
        mock_request.return_value = response_mock

        response = self.api_helper.send_request(request_url, headers, http_method, data, query_params)

        self.assertEqual(response, response_mock)
        mock_request.assert_called_once_with(
            method=http_method,
            url=request_url,
            headers=headers,
            body=data,
            fields=query_params,
        )

    @patch('urllib3.PoolManager.request')
    def test_send_request_post(self, mock_request):
        request_url = "http://example.com"
        headers = {"Content-Type": "application/json"}
        http_method = "POST"
        data = '{"key": "value"}'
        query_params = {"param1": "value1"}
        response_mock = Mock()
        response_mock.status = 200
        response_mock.data = b'{"key":"value"}'
        mock_request.return_value = response_mock

        response = self.api_helper.send_request(request_url, headers, http_method, data, query_params)

        self.assertEqual(response, response_mock)
        mock_request.assert_called_once_with(
            method=http_method,
            url=self.api_helper.encode_query_parameters_to_url(request_url, query_params),
            headers=headers,
            body=data,
        )

    @patch('urllib3.PoolManager.request')
    def test_send_request_exception(self, mock_request):
        request_url = "http://example.com"
        headers = {"Content-Type": "application/json"}
        http_method = "POST"
        data = '{"key": "value"}'
        query_params = {"param1": "value1"}
        mock_request.side_effect = Exception("Request failed")

        with self.assertRaises(Exception):
            self.api_helper.send_request(request_url, headers, http_method, data, query_params)

if __name__ == '__main__':
    unittest.main()

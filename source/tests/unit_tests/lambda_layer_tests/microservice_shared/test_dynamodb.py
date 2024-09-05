# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# USAGE:
#   ./run-unit-tests.sh --test-file-name lambda_layer_tests/microservice_shared/test_dynamodb.py
###############################################################################

import unittest
from unittest.mock import patch, Mock
import sys

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
from microservice_shared.utilities import LoggerUtil

from microservice_shared.dynamodb import DynamodbHelper


class TestDynamodbHelper(unittest.TestCase):
    
    def setUp(self):
        self.dynamodb_helper = DynamodbHelper()
        self.item = {
            'id': {'S': '123'},
            'name': {'S': 'Test Item'}
        }
        
    @patch.object(LoggerUtil, 'create_logger', return_value=Mock())
    def test_init(self, mock_create_logger):
        DynamodbHelper()
        mock_create_logger.assert_called_once()

    def test_deserialize_dynamodb_item(self):
        deserialized_item = self.dynamodb_helper.deserialize_dynamodb_item(self.item)
        expected_item = {
            'id': '123',
            'name': 'Test Item'
        }
        self.assertEqual(deserialized_item, expected_item)


if __name__ == '__main__':
    unittest.main()
    
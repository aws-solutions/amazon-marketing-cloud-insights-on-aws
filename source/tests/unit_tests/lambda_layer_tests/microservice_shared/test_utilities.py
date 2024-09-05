# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# USAGE:
#   ./run-unit-tests.sh --test-file-name lambda_layer_tests/microservice_shared/test_utilities.py
###############################################################################

import unittest
from unittest.mock import patch
from decimal import Decimal
import datetime as dt
import logging

from aws_lambda_layers.microservice_layer.python.microservice_shared.utilities import JsonUtil, LoggerUtil, DateUtil, MapUtil


class TestLoggerUtil(unittest.TestCase):
    
    @patch('logging.StreamHandler')
    def test_create_logger(self, mock_handler):
        logger = LoggerUtil.create_logger()
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.level, logging.INFO)
        mock_handler.assert_called_once()

class TestJsonUtil(unittest.TestCase):

    def setUp(self):
        self.json_util = JsonUtil()

    def test_json_encoder_default_decimal(self):
        decimal_value = Decimal('3.14')
        result = self.json_util.json_encoder_default(decimal_value)
        self.assertEqual(result, '3.14')

    def test_json_encoder_default_date(self):
        date_value = dt.date(2023, 5, 1)
        result = self.json_util.json_encoder_default(date_value)
        self.assertEqual(result, '2023-05-01')

    def test_json_encoder_default_datetime(self):
        datetime_value = dt.datetime(2023, 5, 1, 12, 30, 0)
        result = self.json_util.json_encoder_default(datetime_value)
        self.assertEqual(result, '2023-05-01T12:30:00')

    def test_json_encoder_default_non_string(self):
        value = 42
        result = self.json_util.json_encoder_default(value)
        self.assertEqual(result, '42')

    def test_safe_json_loads_valid_json(self):
        json_str = '{"key": "value"}'
        result = JsonUtil.safe_json_loads(json_str)
        self.assertEqual(result, {"key": "value"})

    def test_safe_json_loads_invalid_json(self):
        invalid_json = 'not a json string'
        result = JsonUtil.safe_json_loads(invalid_json)
        self.assertEqual(result, invalid_json)

    def test_is_json_valid_json(self):
        json_str = '{"key": "value"}'
        result = self.json_util.is_json(json_str)
        self.assertTrue(result)

    def test_is_json_invalid_json(self):
        invalid_json = 'not a json string'
        with self.assertLogs(level='ERROR') as cm:
            result = self.json_util.is_json(invalid_json)
        self.assertFalse(result)
        self.assertIn('Expecting value', cm.output[0])
        
class TestDateUtil(unittest.TestCase):
    
    def setUp(self):
        self.date_util = DateUtil()
    
    def test_get_current_utc_iso_timestamp(self):
        result = self.date_util.get_current_utc_iso_timestamp()
        self.assertIsInstance(result, str)
        
class TestMapUtil(unittest.TestCase):
    
    def setUp(self):
        self.map_util = MapUtil()

    def test_map_nested_dicts_modify(self):
        test_dict = {
            'a': 1,
            'b': {
                'c': 2,
                'd': [3, 4]
            },
            'e': 5
        }

        def double(value, **kwargs):
            return value * 2

        self.map_util.map_nested_dicts_modify(test_dict, double)
        expected_dict = {
            'a': 2,
            'b': {
                'c': 4,
                'd': [3, 4, 3, 4]
            },
            'e': 10
        }
        self.assertEqual(test_dict, expected_dict)

if __name__ == '__main__':
    unittest.main()

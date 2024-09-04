# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# USAGE:
#   ./run-unit-tests.sh --test-file-name lambda_layer_tests/microservice_shared/test_dynamic_dates.py
###############################################################################

import sys
import unittest
from unittest.mock import patch
import datetime as dt

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
from aws_lambda_layers.microservice_layer.python.microservice_shared.dynamic_dates import LoggerUtil
from aws_lambda_layers.microservice_layer.python.microservice_shared.dynamic_dates import DynamicDateEvaluator


class TestDynamicDateEvaluator(unittest.TestCase):
    
    def setUp(self):
        self.evaluator = DynamicDateEvaluator()

    def test_get_last_day_of_month(self):
        date = dt.datetime(2024, 8, 15)
        result = self.evaluator.get_last_day_of_month(date)
        self.assertEqual(result, 31)

        date = dt.datetime(2024, 2, 15)  # Leap year
        result = self.evaluator.get_last_day_of_month(date)
        self.assertEqual(result, 29)

    def test_get_offset_value(self):
        result = self.evaluator.get_offset_value('TODAY(10)')
        self.assertEqual(result, 10)
        
        result = self.evaluator.get_offset_value('LASTDAYOFOFFSETMONTH(-3)')
        self.assertEqual(result, -3)

    @patch.object(LoggerUtil, 'create_logger')
    def test_logger_initialization(self, mock_create_logger):
        mock_create_logger.return_value = 'mock_logger'
        evaluator = DynamicDateEvaluator()
        self.assertEqual(evaluator.logger, 'mock_logger')


if __name__ == '__main__':
    unittest.main()
    
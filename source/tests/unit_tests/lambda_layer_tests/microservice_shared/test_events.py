# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# USAGE:
#   ./run-unit-tests.sh --test-file-name lambda_layer_tests/microservice_shared/test_events.py
###############################################################################

import unittest
from unittest.mock import patch, Mock
import sys
import json

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients
sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
from microservice_shared.utilities import LoggerUtil

from microservice_shared.events import EventsHelper


def mock_events_client():
    events_client = get_service_client('events')
    events_client.put_rule = Mock(
        return_value={
            "RuleArn": "test_rule_arn"
        }
    )
    events_client.put_targets = Mock(
        return_value=None
    )
    
    return events_client

def mock_clients():
    secrets_client = mock_events_client()
    _helpers_service_clients['events'] = secrets_client
    return secrets_client


class TestEventsHelper(unittest.TestCase):
    
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_events_client)
    def setUp(self, mock_get_service_client):
        self.events_helper = EventsHelper()
        self.mock_events_client = mock_clients()

    @patch.object(LoggerUtil, 'create_logger', return_value=Mock())
    def test_init(self, mock_create_logger):
        EventsHelper()
        mock_create_logger.assert_called_once()

    def test_create_rule_with_targets(self):
        name = "test_name"
        schedule_expression = "test_expression"
        event_bus_name = "test_event_bus_name"
        targets = ["test_target"]
        
        self.events_helper.create_rule_with_targets(
                name=name,
                schedule_expression=schedule_expression,
                event_bus_name=event_bus_name,
                targets=targets
            ) 
        
        _, kwargs = self.mock_events_client.put_rule.call_args
        
        # assert default values when not passed in
        self.assertEqual(kwargs['State'], "ENABLED")
        self.assertEqual(kwargs['Description'], "")
        assert "Tags" not in kwargs
        
        # assert we call the put_targets method after creating the rule
        self.mock_events_client.put_targets.call_args.assert_called_once

    @patch.object(EventsHelper, 'create_rule_with_targets')
    def test_create_report_schedule(self, mock_create_rule_with_targets):
        event = {
            "rule_name": "test_rule_name",
            "schedule_expression": "test_expression",
            "report_request": {
                "reportTypeId": "test_report_id"   
            }
        }
        target_arn = "test_arn"
        
        self.events_helper.create_report_schedule(
                event=event,
                target_arn=target_arn
        ) 
        
        _, kwargs = mock_create_rule_with_targets.call_args
        
        # assert default values when not passed in
        self.assertEqual(kwargs['event_bus_name'], "default")
        self.assertEqual(kwargs['state'], "ENABLED")
        self.assertEqual(kwargs['description'], "")
        
        # assert proper formatting of target object
        self.assertEqual(kwargs['targets'], [
            {
                "Arn": target_arn,
                "Id": "1",
                "Input": json.dumps(event['report_request'])
            }
        ])

if __name__ == '__main__':
    unittest.main()

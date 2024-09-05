# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Union
import json

from aws_solutions.core.helpers import get_service_client
from microservice_shared.utilities import LoggerUtil

class EventsHelper:
    """
    Helper class for interacting with AWS EventBridge.
    """
    def __init__(self):
        """
        Initializes the EventsHelper instance.
        """
        self.logger = LoggerUtil.create_logger()
        self.event_client = get_service_client('events')
        
    def create_rule_with_targets(
        self,
        name: str,
        schedule_expression: str,
        event_bus_name: str,
        description: str = "",
        state: str = "ENABLED",
        tags: list = None,
        targets: list = None,
    ) :
        """
        Creates an event schedule rule in EventBridge and adds targets to it.

        :param name: The name of the rule.
        :param schedule_expression: The schedule expression that defines when the rule triggers.
        :param event_bus_name: The name of the event bus to associate with the rule.
        :param description: A description for the rule (optional).
        :param state: The state of the rule, either 'ENABLED' or 'DISABLED' (default is 'ENABLED').
        :param tags: A list of tags to associate with the rule (optional).
        :param targets: Targets to be added to the rule (optional).
        """
        self.logger.info("Creating event rule")
        
        if tags:
            self.event_client.put_rule(
                Name=name,
                ScheduleExpression=schedule_expression,
                State=state.upper(),
                Description=description,
                Tags=tags,
                EventBusName=event_bus_name
            )
        else:
            self.event_client.put_rule(
                Name=name,
                ScheduleExpression=schedule_expression,
                State=state.upper(),
                Description=description,
                EventBusName=event_bus_name
            )
        
        if targets:
            self.logger.info("Adding targets to event rule")
            
            self.event_client.put_targets(
                Rule=name,
                Targets=targets
            )
        
    def create_report_schedule(
        self,
        event: dict,
        target_arn: str,
        rule_prefix: str = None
    ) -> Union[str, None]:
        """
        Creates a schedule rule in EventBridge for generating a report.

        :param event: A dictionary containing the details for the schedule rule. 
                    Must include 'rule_name', 'schedule_expression', and 'report_request'.
        :param target_arn: The ARN of the target Lambda function to invoke with the report request.
        :param rule_prefix: A prefix to add to the rule name for uniqueness.

        :return: A string containing an error message if any required fields are missing 
                or if an error occurs during rule creation. Returns None on success.
        """
        # Check that all required fields are present
        required_fields = ["rule_name", "schedule_expression", "report_request"]
        for field in required_fields:
            if field not in event:
                self.logger.error(f"Missing required field: {field}")
                return json.dumps({"Invalid event data: rule_name, schedule_expression, and report_request are required"})
        
        rule_name = event["rule_name"]
        if rule_prefix:
            rule_name = f"{rule_prefix}-{rule_name}"
        report_request = event["report_request"]
        schedule_expression = event["schedule_expression"]
        
        # Assign any optional fields passed in
        state = event.get("state", "ENABLED")
        rule_description = event.get("rule_description", "") # empty string required for put_rule call

        # Create target that will recieve the report_request
        target = {
            "Arn": target_arn,
            "Id": "1",
            "Input": json.dumps(report_request)
        }
        
        self.create_rule_with_targets(
            name=rule_name,
            schedule_expression=schedule_expression,
            description=rule_description,
            state=state,
            event_bus_name="default",
            targets=[target]
        )
            
        
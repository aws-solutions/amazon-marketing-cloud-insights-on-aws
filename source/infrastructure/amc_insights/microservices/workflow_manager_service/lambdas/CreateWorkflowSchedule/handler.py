# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3
import json
import os
from aws_lambda_powertools import Logger

from wfm_utilities import wfm_utilities
from cloudwatch_metrics import metrics

INVOKE_WORKFLOW_EXECUTION_SM_LAMBDA_ARN = os.environ['INVOKE_WORKFLOW_EXECUTION_SM_LAMBDA_ARN']
RULE_PREFIX = os.environ['RULE_PREFIX']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']

# Create a logger instance and pass that to the common utils lambda_function layer class so it can log errors
logger = Logger(service="Workflow Management Service", level="INFO")

Utils = wfm_utilities.Utils(logger)


def events_update_rule(rule: dict, client) -> dict:
    response = client.put_rule(
        Name=rule['Name'],
        ScheduleExpression=rule['ScheduleExpression'],
        State=rule['State'],
        Description=rule['Description'],
        Tags=[
            {
                'Key': 'customerId',
                'Value': rule['customerId']
            },
        ],
        EventBusName=rule['EventBusName']

    )
    Utils.logger.info(response)
    return response


def events_update_target(rule: dict, target: dict, client) -> dict:
    response = client.put_targets(
        Rule=rule['Name'],
        Targets=[target]
    )
    return response


def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="CreateWorkflowSchedule")

    customer_id = event['execution_request']['customerId']
    rule_name = f"{RULE_PREFIX}-wfm-{event['rule_name']}"
    rule_description = event['rule_description']
    execution_request = event['execution_request']
    schedule_expression = event['schedule_expression']

    # Define the rule object
    rule = {
        "Name": rule_name,
        "customerId": customer_id,
        "EventBusName": "default",
        "ScheduleExpression": schedule_expression,
        "Description": rule_description,
        "State": "ENABLED"
    }

    target = {
        "Arn": INVOKE_WORKFLOW_EXECUTION_SM_LAMBDA_ARN,
        "Id": "1",
        "Input": json.dumps(execution_request)
    }

    # Create client for cloudwatch events service
    events_client = boto3.Session().client('events')

    # Create / Update the rule
    update_rule_response = events_update_rule(rule=rule, client=events_client)

    # Check to see if the update succeeded
    if update_rule_response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) in range(200, 299):
        logger.info(
            f"successfully updated rule {rule.get('Name')}: {update_rule_response}"
        )

        update_target_response = events_update_target(
            rule=rule,
            target=target,
            client=events_client
        )

        if update_target_response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) in range(200, 299):
            logger.info(
                f"successfully updated rule target for rule {rule.get('Name')}: {update_target_response}")

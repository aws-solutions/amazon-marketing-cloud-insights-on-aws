# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json

from microservice_shared.utilities import LoggerUtil
from microservice_shared.events import EventsHelper
from cloudwatch_metrics import metrics

INVOKE_ADS_REPORT_SM_LAMBDA_ARN = os.environ['INVOKE_ADS_REPORT_SM_LAMBDA_ARN']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
DATASET = os.environ['DATASET']
REGION = os.environ['REGION']

logger = LoggerUtil.create_logger()
event_helper = EventsHelper()


def handler(event, _):
    """
    AWS Lambda function to create a schedule rule for generating an Ads report in EventBridge.

    This function handles the creation of an EventBridge rule based on the input `event`.
    It logs the incoming event, increments a custom metric, and invokes the
    `create_report_schedule` method to create the rule. If the rule is successfully
    created, a URL to the rule in the AWS Management Console is returned.

    **Event Requirements**:
    The `event` parameter must be a dictionary containing the following fields:
    - `rule_name` (str): The name of the EventBridge rule to be created.
    - `schedule_expression` (str): A cron or rate expression that defines when the rule triggers.
    - `report_request` (dict): The payload to be passed to the target Lambda function when the rule is triggered.
    
    Optional fields:
    - `state` (str): The state of the rule, either 'ENABLED' or 'DISABLED'. Defaults to 'ENABLED'.
    - `rule_description` (str): A description for the rule.

    :param event: The event data passed to the Lambda function, containing details for the schedule rule.
    :param _: The context parameter (not used in this function).

    :return: The URL to the created rule in the AWS Management Console if successful,
             or a JSON object with an error message if the rule creation fails.
    """
    logger.info(f"Event: {event}")
    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="ScheduleAdsReport")

    try:
        rule_prefix = f"{RESOURCE_PREFIX}-{DATASET}"
        rule_name = event.get("rule_name")
        
        event_helper.create_report_schedule(
            event=event,
            rule_prefix=rule_prefix,
            target_arn=INVOKE_ADS_REPORT_SM_LAMBDA_ARN
        )
        
        # construct and return the url if the rule is successfully created
        rule_url = f"https://{os.environ['REGION']}.console.aws.amazon.com/events/home?region={os.environ['REGION']}#/eventbus/default/rules/{rule_prefix}-{rule_name}"
        return rule_url
    
    except Exception as e:
        return json.dumps({"Error creating schedule": str(e)})

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_solutions.core.helpers import get_service_client
import json
import os
from aws_lambda_powertools import Logger
from wfm_utilities import wfm_utilities
from datetime import datetime
from cloudwatch_metrics import metrics

STEP_FUNCTION_STATE_MACHINE_ARN = os.environ['STEP_FUNCTION_STATE_MACHINE_ARN']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']

# Create a logger instance and pass that to the common utils lambda_function layer class so it can log errors
logger = Logger(service="Workflow Management Service", level="INFO")
utils = wfm_utilities.Utils(logger)

client = get_service_client('stepfunctions')


def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="InvokeWorkflowSM")

    logger.info(f'event received:{event}')
    customer_id = event.get('customerId', '')

    # copy the workflowId from the workflowDefinition if it does not exist in workflowRequest
    workflow_request = event.get('workflowRequest', {})
    workflow_definition = workflow_request.get('workflowDefinition', {})
    workflow_id = workflow_request.get('workflowId', workflow_definition.get('workflowId'))
    workflow_request['workflowId'] = workflow_id

    # Create a state machine input dictionary
    state_machine_input = {
        'customerId': customer_id,
        'workflowRequest': workflow_request,
        'workflowStateMachineArn': STEP_FUNCTION_STATE_MACHINE_ARN,
        'executionCreatedDate': datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    }

    response = client.start_execution(
        stateMachineArn=STEP_FUNCTION_STATE_MACHINE_ARN,
        input=json.dumps(state_machine_input, default=utils.json_encoder_default)
    )

    message = f"created state machine execution response : {response} for request {workflow_request}"
    logger.info(message)

    # Record anonymous metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="InvokeWorkflowSM")

    return json.dumps(response, default=utils.json_encoder_default)

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from aws_lambda_powertools import Logger
# Import the common lambda_function layer functions
from wfm_amc_api_interface import wfm_amc_api_interface
from wfm_utilities import wfm_utilities
from cloudwatch_metrics import metrics

WORKFLOWS_TABLE_NAME = os.environ['WORKFLOWS_TABLE_NAME']

# Create a logger instance and pass that to the common utils lambda_function layer class so it can log errors
logger = Logger(service="Workflow Management Service", level="INFO")

utils = wfm_utilities.Utils(logger)

METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']


def handler(event, context):
    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="UpdateWorkflow")

    event['EXECUTION_RUNNING_LAMBDA_NAME'] = context.function_name
    customer_config = event['customerConfig']

    # set up the AMC API Interface
    wfm = wfm_amc_api_interface.AMCAPIs(customer_config, utils)

    # get the execution Request
    workflow_request = event.get('workflowRequest', {})
    workflow_definition = workflow_request.get('workflowDefinition', {}).copy()
    amc_response = wfm.update_workflow(workflow_definition)

    event.update(amc_response.response)
    event["workflowId"] = workflow_definition.get('workflowId', '')

    if amc_response.success:
        utils.dynamodb_put_item(WORKFLOWS_TABLE_NAME, event)

    return event

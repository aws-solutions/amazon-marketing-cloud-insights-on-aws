# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from aws_lambda_powertools import Logger

# Import the common lambda_function layer functions
from wfm_amc_api_interface import wfm_amc_api_interface
from wfm_utilities import wfm_utilities
from cloudwatch_metrics import metrics

EXECUTION_STATUS_TABLE = os.environ['EXECUTION_STATUS_TABLE']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']

# Create a logger instance and pass that to the common utils lambda_function layer class so it can log errors
logger = Logger(service="Workflow Management Service", level="INFO")

utils = wfm_utilities.Utils(logger)


def handler(event, context):
    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="CancelWorkflowExecution")

    event['EXECUTION_RUNNING_LAMBDA_NAME'] = context.function_name
    customer_config = event['customerConfig']

    # set up the AMC API Interface
    wfm = wfm_amc_api_interface.AMCAPIInterface(customer_config, logger, utils)

    # get the execution Request
    execution_request = event.get('executionRequest', {})

    # Try to get the status first to determine if the workflow exists
    amc_response = wfm.cancel_workflow_execution(execution_request['workflowExecutionId'])
    event.update(amc_response.response)
    if amc_response.success:
        message = f"Successfully cancelled execution {event.get('workflowId', '')} {event.get('workflowExecutionId', '')}for customerId: {wfm.config['customerId']}"
        logger.info(message)

        # copy the workflow execution ID into the execution request section of the event.
        execution_request['workflowExecutionId'] = amc_response.response.get('workflowExecutionId')

        utils.dynamodb_put_item(EXECUTION_STATUS_TABLE, event)

    else:
        message = f"Unable to cancel execution {event.get('workflowId', '')} {event.get('workflowExecutionId', '')} for customerId: {wfm.config['customerId']} response code {amc_response.status_code}"
        logger.error(message)

    return event

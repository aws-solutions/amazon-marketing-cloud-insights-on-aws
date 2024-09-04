# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from aws_lambda_powertools import Logger

# Import the common lambda_function layer functions
from wfm_amc_api_interface import wfm_amc_api_interface
from wfm_utilities import wfm_utilities
from cloudwatch_metrics import metrics
from microservice_shared import dynamodb

WORKFLOWS_TABLE_NAME = os.environ['WORKFLOWS_TABLE_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']

logger = Logger(service="Workflow Management Service", level="INFO")
wfm_utils = wfm_utilities.Utils(logger)
dynamodb_helper = dynamodb.DynamodbHelper()


def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="UpdateWorkflow")

    customer_config = dynamodb_helper.deserialize_dynamodb_item(event["customerConfig"]["Item"])

    # set up the AMC API Interface
    wfm = wfm_amc_api_interface.AMCAPIs(customer_config, wfm_utils)

    # get the execution Request
    workflow_request = event.get('workflowRequest', {})
    workflow_definition = workflow_request.get('workflowDefinition', {}).copy()
    amc_response = wfm.update_workflow(workflow_definition)

    event.update(amc_response.response)
    event["workflowId"] = workflow_definition.get('workflowId', '')

    if amc_response.success:
        dynamodb_helper.dynamodb_put_item(WORKFLOWS_TABLE_NAME, event)

    return event

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta
from aws_lambda_powertools import Logger
import os

# Import the common lambda_function layer functions
from wfm_amc_api_interface import wfm_amc_api_interface
from wfm_utilities import wfm_utilities
from cloudwatch_metrics import metrics

# Create a logger instance and pass that to the common utils lambda_function layer class so it can log errors
logger = Logger(service="Workflow Management Service", level="INFO")

Utils = wfm_utilities.Utils(logger)

METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']


def handler(event, context):
    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="GetExecutionSummary")

    event['EXECUTION_RUNNING_LAMBDA_NAME'] = context.function_name
    customer_config = event['customerConfig']

    # set up the AMC API Interface
    wfm = wfm_amc_api_interface.AMCAPIs(customer_config, Utils)

    # get the execution Request
    execution_request = event.get('executionRequest', {})

    executions_since = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%dT%H:%M:%S')

    amc_response = wfm.get_execution_status_by_minimum_create_time(executions_since)
    logger.info(amc_response.response)

    running_executions = 0
    pending_executions = 0
    failed_executions = 0
    rejected_executions = 0
    cancelled_executions = 0
    succeeded_executions = 0

    executions_summary = {
        'executionsSince': executions_since,
        'totalExecutions': 0,
        'totalRunningorPending': running_executions + pending_executions,
        'succeededExecutions': succeeded_executions,
        'runningExecutions': running_executions,
        'pendingExecutions': pending_executions,
        'failedExecutions': failed_executions,
        'rejectedExecutions': rejected_executions,
        'cancelledExecutions': cancelled_executions

    }

    if amc_response.success:
        executions = amc_response.response.get('executions', [])
        running_executions, pending_executions, failed_executions, rejected_executions, cancelled_executions, succeeded_executions = check_execution_status(
            executions)

        executions_summary.update({
            'executionsSince': executions_since,
            'totalExecutions': len(executions),
            'totalRunningorPending': running_executions + pending_executions,
            'succeededExecutions': succeeded_executions,
            'runningExecutions': running_executions,
            'pendingExecutions': pending_executions,
            'failedExecutions': failed_executions,
            'rejectedExecutions': rejected_executions,
            'cancelledExecutions': cancelled_executions

        })

        if execution_request.get('includeDetails'):
            executions_summary['executions'] = executions

        logger.info(executions_summary)
        event['executionsSummary'] = executions_summary

    else:
        message = f"failed to receive execution statuses for customerId: {wfm.customer_config['customerId']}"
        logger.error(message)

    return event


def check_execution_status(executions):
    running_executions = 0
    pending_executions = 0
    failed_executions = 0
    rejected_executions = 0
    cancelled_executions = 0
    succeeded_executions = 0

    for execution in executions:
        execution_status = execution.get('status', '')

        if execution_status == 'RUNNING':
            running_executions += 1

        if execution_status == 'PENDING':
            pending_executions += 1

        if execution_status == 'FAILED':
            failed_executions += 1

        if execution_status == 'REJECTED':
            rejected_executions += 1

        if execution_status == 'CANCELLED':
            cancelled_executions += 1

        if execution_status == 'SUCCEEDED':
            succeeded_executions += 1

    return running_executions, pending_executions, failed_executions, rejected_executions, cancelled_executions, succeeded_executions

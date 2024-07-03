# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3
import os
from aws_lambda_powertools import Logger

from wfm_utilities import wfm_utilities
from cloudwatch_metrics import metrics

RULE_PREFIX = os.environ['RULE_PREFIX']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']

# Create a logger instance and pass that to the common utils lambda_function layer class so it can log errors
logger = Logger(service="Workflow Management Service", level="INFO")

Utils = wfm_utilities.Utils(logger)


def events_get_targets_for_rule(rule_name: str, client):
    paginator = client.get_paginator('list_targets_by_rule')
    response_iterator = paginator.paginate(
        Rule=rule_name,
        PaginationConfig={
            'PageSize': 100
        }
    )
    pages = []
    targets = []
    for page in response_iterator:
        pages.append(page)
        logger.info('Rules page {}'.format(page))
        if 'Targets' in page:
            for target in page['Targets']:
                targets.append(target)

    return targets


def events_remove_target(rule_name, client):
    target_for_rule_response = events_get_targets_for_rule(rule_name, client=client)
    logger.info('target_for_rule_response {}'.format(target_for_rule_response))

    target_ids_to_remove = []
    for target in target_for_rule_response:
        target_ids_to_remove.append(target['Id'])

    if len(target_ids_to_remove) > 0:
        remove_targets_response = client.remove_targets(
            Rule=rule_name,
            Ids=target_ids_to_remove,
            Force=True
        )
        return remove_targets_response
    else:
        logger.info('No targets to remove')


def events_delete_rule(rule_name, client):
    # delete rule
    response = client.delete_rule(
        Name=rule_name,
        Force=False
    )
    return response


def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
        metric_name="DeleteWorkflowSchedule")

    rule_name = f"{RULE_PREFIX}-wfm-{event['rule_name']}"

    # Create client for cloudwatch events service
    events_client = boto3.Session().client('events')

    remove_target_response = events_remove_target(rule_name=rule_name, client=events_client)

    if remove_target_response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) in range(200, 299):
        logger.info(f"successfully deleted rule target for rule {rule_name}: {remove_target_response}")

        delete_rule_response = events_delete_rule(rule_name=rule_name, client=events_client)

        if delete_rule_response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) in range(200, 299):
            logger.info(f"successfully deleted rule {rule_name}: {delete_rule_response}")

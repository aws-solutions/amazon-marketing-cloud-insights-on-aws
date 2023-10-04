# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from aws_lambda_powertools import Logger
from cloudwatch_metrics import metrics
import aws_solutions.core.config

import boto3

logger = Logger(service="Tenant Provisioning Service", level="INFO")

METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
STACK_NAME = os.environ['RESOURCE_PREFIX']

in_progress_codes = ['CREATE_COMPLETE',
                     'CREATE_IN_PROGRESS',
                     'REVIEW_IN_PROGRESS',
                     'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                     'UPDATE_COMPLETE',
                     'UPDATE_IN_PROGRESS']


def get_cfn_client(event):
    config = aws_solutions.core.config.botocore_config
    region = event.get('bucketRegion')
    session = boto3.Session(region_name=region)
    cfn = session.client('cloudformation', config=config)
    return cfn


def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="AddAMCInstanceCheck")

    if (event.get('body', {}).get("stackId", {}).get("StackId", None) != None):
        cfn = get_cfn_client(event)
        try:
            logger.info('event: {}'.format(json.dumps(event, indent=2)))
            stack_name = event['body']['stackId']['StackId']

            logger.info('Checking Stack {}'.format(stack_name))
            try:
                logger.info('Getting Stack Creation Status')
                stack_resp = cfn.describe_stacks(
                    StackName=stack_name
                )
                logger.info(stack_resp)
                stack_status = stack_resp['Stacks'][0]['StackStatus']
                logger.info('Stack {} state is: {}'.format(stack_name, stack_status))

                if stack_status in in_progress_codes:
                    logger.info('In progress code: {}'.format(stack_status))
                    resp = stack_status
                else:
                    logger.info(f'Stack response code: {stack_status}')
                    resp = 'FAILED'
            except Exception as e:
                logger.info('DescribesStacks {} failed'.format(stack_name))
                logger.info(str(e))
                resp = 'FAILED'
        except Exception as e:
            logger.info('DescribesStacks {} failed'.format(stack_name))
            logger.info(str(e))
            resp = 'FAILED'
        return resp
    else:
        logger.info('Nothing to check missing input parameters')
        resp = 'CREATE_COMPLETE'
        return resp

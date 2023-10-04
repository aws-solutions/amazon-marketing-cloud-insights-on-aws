# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from aws_lambda_powertools import Logger
from datalake_library.transforms import TransformHandler
from datalake_library import octagon
from datalake_library.octagon import peh
from cloudwatch_metrics import metrics

logger = Logger(service="SDLF pipeline stage B", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']


def lambda_handler(event, context):
    """Calls custom job waiter developed by user

    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket, Key(s) and Job Details
    """
    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SdlfHeavyTransformCheckJob")
    try:
        logger.info('Fetching event data from previous step')
        team = event['body']['team']
        stage = event['body']['pipeline_stage']
        dataset = event['body']['dataset']
        job_details = event['body']['job']['jobDetails']
        processed_keys_path = event['body']['job']['processedKeysPath']

        component = context.function_name.split('-')[-2].title()
    except Exception as e:
        logger.error("Fatal error: unable to fetch data from Lambda event or context", exc_info=True)
        raise e

    try:
        logger.info('Initializing Octagon client')
        octagon_client = (
            octagon.OctagonClient()
            .with_run_lambda(True)
            .with_configuration_instance(event['body']['env'], resource_prefix)
            .build()
        )
    except Exception as e:
        logger.error("Fatal error: octagon client initialization fail", exc_info=True)
        raise e

    try:
        logger.info('Checking Job Status with user custom code')
        transform_handler = TransformHandler().stage_transform(resource_prefix, team, dataset, stage)
        response = transform_handler().check_job_status(processed_keys_path, job_details)  # custom user code called
        response['peh_id'] = event['body']['job']['peh_id']

        if event['body']['job']['jobDetails']['jobStatus'] == 'FAILED':
            peh.PipelineExecutionHistoryAPI(
                octagon_client).retrieve_pipeline_execution(response['peh_id'])
            octagon_client.end_pipeline_execution_failed(component=component,
                                                         issue_comment="{} {} Error: Check Job Logs".format(stage,
                                                                                                            component))
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        peh.PipelineExecutionHistoryAPI(octagon_client).retrieve_pipeline_execution(
            event['body']['job']['peh_id'])
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="{} {} Error: {}".format(stage, component, repr(e)))
        raise e
    return response

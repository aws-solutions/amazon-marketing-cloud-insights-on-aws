# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from aws_lambda_powertools import Logger
from datalake_library.transforms import TransformHandler
from datalake_library import octagon
from cloudwatch_metrics import metrics

logger = Logger(service="SDLF pipeline stage B", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']


def lambda_handler(event, context):
    """Calls custom transform developed by user

    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket and Key(s)
    """
    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SdlfHeavyTransformProcessObject")

    try:
        logger.info('Fetching event data from previous step')
        bucket = event['body']['bucket']
        keys_to_process = event['body']['keysToProcess']
        team = event['body']['team']
        pipeline = event['body']['pipeline']
        stage = event['body']['pipeline_stage']
        dataset = event['body']['dataset']

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
        peh_id = octagon_client.start_pipeline_execution(
            pipeline_name='{}-{}-stage-{}'.format(team,
                                                  pipeline, stage[-1].lower()),
            comment=event
        )

        # Call custom transform created by user and process the file
        logger.info('Calling user custom processing code')
        transform_handler = TransformHandler().stage_transform(resource_prefix, team, dataset, stage)
        response = transform_handler().transform_object(
            resource_prefix, bucket, keys_to_process, team, dataset)  # custom user code called
        response['peh_id'] = peh_id
        # remove_content_tmp()
        octagon_client.update_pipeline_execution(
            status="{} {} Processing".format(stage, component), component=component)
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="{} {} Error: {}".format(stage, component, repr(e)))
        # remove_content_tmp()
        raise e
    return response

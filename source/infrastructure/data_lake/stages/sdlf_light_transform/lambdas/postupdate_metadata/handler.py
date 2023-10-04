# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_lambda_powertools import Logger
from datalake_library.configuration import DynamoConfiguration, SQSConfiguration
from datalake_library.interfaces import DynamoInterface, SQSInterface, S3Interface
from datalake_library import octagon
from datalake_library.octagon import peh
import os
from cloudwatch_metrics import metrics

logger = Logger(service="SDLF pipeline stage A", level="INFO", utc=True)

stage_bucket = os.environ['stage_bucket']
resource_prefix = os.environ["RESOURCE_PREFIX"]
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']


def lambda_handler(event, context):
    """Updates the S3 objects metadata catalog

    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with outcome of the process
    """
    metrics.Metrics(METRICS_NAMESPACE, resource_prefix, logger).put_metrics_count_value_1(
        metric_name="SdlfLightTransformPostUpdateMetadata")

    try:
        logger.info('Fetching event data from previous step')
        processed_keys = event['body']['processedKeys']
        team = event['body']['team']
        pipeline = event['body']['pipeline']
        stage = event['body']['pipeline_stage']
        dataset = event['body']['dataset']
        peh_id = event['body']['peh_id']

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
        peh.PipelineExecutionHistoryAPI(
            octagon_client).retrieve_pipeline_execution(peh_id)

        logger.info('Initializing DynamoDB config and Interface')
        dynamo_config = DynamoConfiguration(resource_prefix)
        dynamo_interface = DynamoInterface(dynamo_config)

        logger.info('Storing metadata to DynamoDB')
        bucket = stage_bucket
        for key in processed_keys:
            object_metadata = {
                'bucket': bucket,
                'key': key,
                'size': S3Interface().get_size(bucket, key),
                'last_modified_date': S3Interface().get_last_modified(bucket, key),
                'env': event['body']['env'],
                'team': team,
                'pipeline': pipeline,
                'dataset': dataset,
                'stage': 'stage',
                'pipeline_stage': stage,
                'peh_id': peh_id
            }

            dynamo_interface.update_object_metadata_catalog(object_metadata)

        logger.info('Sending messages to next SQS queue if it exists')
        sqs_config = SQSConfiguration(resource_prefix, team, dataset, ''.join(
            [stage[:-1], chr(ord(stage[-1]) + 1)]))
        sqs_interface = SQSInterface(sqs_config.get_stage_queue_name)
        sqs_interface.send_batch_messages_to_fifo_queue(
            processed_keys, 10, '{}-{}'.format(team, dataset))

        octagon_client.update_pipeline_execution(status="{} {} Processing".format(stage, component),
                                                 component=component)
        octagon_client.end_pipeline_execution_success()
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="{} {} Error: {}".format(stage, component, repr(e)))
        raise e
    return 200

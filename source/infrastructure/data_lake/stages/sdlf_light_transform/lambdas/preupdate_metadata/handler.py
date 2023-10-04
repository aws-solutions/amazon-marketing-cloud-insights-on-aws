# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from aws_lambda_powertools import Logger
from datalake_library.configuration import DynamoConfiguration
from datalake_library.interfaces import DynamoInterface
from datalake_library import octagon
from cloudwatch_metrics import metrics

logger = Logger(service="SDLF pipeline stage A", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']


def lambda_handler(event, context):
    """Updates the objects metadata catalog

    Arguments:
        event {dict} -- Dictionary with details on S3 event
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket and Key
    """
    metrics.Metrics(METRICS_NAMESPACE, resource_prefix, logger).put_metrics_count_value_1(
        metric_name="SdlfLightTransformPreUpdateMetadata")

    try:
        logger.info('Fetching event data from previous step')
        object_metadata = json.loads(event)
        stage = object_metadata['pipeline_stage']

        logger.info('Initializing Octagon client')
        component = context.function_name.split('-')[-2].title()
    except Exception as e:
        logger.error("Fatal error: unable to fetch data from Lambda event or context", exc_info=True)
        raise e

    try:
        logger.info('Initializing Octagon client')
        octagon_client = (
            octagon.OctagonClient()
            .with_run_lambda(True)
            .with_configuration_instance(object_metadata['env'], resource_prefix)
            .build()
        )
    except Exception as e:
        logger.error("Fatal error: octagon client initialization fail", exc_info=True)
        raise e

    try:
        object_metadata['peh_id'] = octagon_client.start_pipeline_execution(
            pipeline_name='{}-{}-stage-{}'.format(object_metadata['team'],
                                                  object_metadata['pipeline'],
                                                  stage[-1].lower()),
            comment=event
        )
        # Add business metadata (e.g. object_metadata['project'] = 'xyz')

        logger.info('Initializing DynamoDB config and Interface')
        dynamo_config = DynamoConfiguration(resource_prefix)
        dynamo_interface = DynamoInterface(dynamo_config)

        logger.info('Storing metadata to DynamoDB')
        dynamo_interface.update_object_metadata_catalog(object_metadata)

        logger.info(
            'Passing arguments to the next function of the state machine')
        octagon_client.update_pipeline_execution(
            status="{} {} Processing".format(stage, component), component=component)
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="{} {} Error: {}".format(stage, component, repr(e)))
        raise e
    return {
        'statusCode': 200,
        'body': object_metadata
    }

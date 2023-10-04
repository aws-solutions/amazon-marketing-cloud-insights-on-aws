# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from aws_lambda_powertools import Logger
from datalake_library.configuration import DynamoConfiguration, SQSConfiguration, StateMachineConfiguration, \
    S3Configuration
from datalake_library.interfaces import DynamoInterface
from datalake_library.interfaces import SQSInterface
from datalake_library.interfaces import StatesInterface
from aws_solutions.core.helpers import get_service_client
from cloudwatch_metrics import metrics

logger = Logger(service="SDLF pipeline stage B", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']

def lambda_handler(event, _):
    """Checks if any items need processing and triggers state machine
    Arguments:
        event {dict} -- Dictionary with no relevant details
        context {dict} -- Dictionary with details on Lambda context 
    """
    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SdlfHeavyTransformRouting")

    try:
        team = event['team']
        pipeline = event['pipeline']
        stage = event['pipeline_stage']
        dataset = event['dataset']
        env = event['env']
        stage_bucket = S3Configuration(resource_prefix).stage_bucket
        dynamo_config = DynamoConfiguration(resource_prefix)
        dynamo_interface = DynamoInterface(dynamo_config)
        transform_info = dynamo_interface.get_transform_table_item(
            '{}-{}'.format(team, dataset))
        MIN_ITEMS_TO_PROCESS = int(
            transform_info['min_items_process']['stage_{}'.format(stage[-1].lower())])
        MAX_ITEMS_TO_PROCESS = int(
            transform_info['max_items_process']['stage_{}'.format(stage[-1].lower())])
        sqs_config = SQSConfiguration(resource_prefix, team, dataset, stage)
        queue_interface = SQSInterface(sqs_config.get_stage_queue_name)
        keys_to_process = []

        logger.info(
            'Querying {}-{} objects waiting for processing'.format(team, dataset))
        keys_to_process = queue_interface.receive_min_max_messages(
            MIN_ITEMS_TO_PROCESS, MAX_ITEMS_TO_PROCESS)
        # If no keys to process, break
        if not keys_to_process:
            return

        logger.info('{} Objects ready for processing'.format(
            len(keys_to_process)))
        keys_to_process = list(set(keys_to_process))

        response = {
            'statusCode': 200,
            'body': {
                "bucket": stage_bucket,
                "keysToProcess": keys_to_process,
                "team": team,
                "pipeline": pipeline,
                "pipeline_stage": stage,
                "dataset": dataset,
                "env": env
            }
        }
        logger.info('Starting State Machine Execution')
        state_config = StateMachineConfiguration(resource_prefix, team, pipeline, stage)
        StatesInterface().run_state_machine(
            state_config.get_stage_state_machine_arn, response)
        # record State Machine invocation to CloudWatch metric
        metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SdlfHeavyTransformRoutingSM")
    except Exception as e:
        # If failure send to DLQ
        if keys_to_process:
            dlq_interface = SQSInterface(sqs_config.get_stage_dlq_name)
            dlq_interface.send_message_to_fifo_queue(
                json.dumps(response), 'failed')
        logger.error("Fatal error", exc_info=True)
        raise e

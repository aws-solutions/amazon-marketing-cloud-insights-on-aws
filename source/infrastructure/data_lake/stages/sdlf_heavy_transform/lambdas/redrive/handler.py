# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json

from aws_lambda_powertools import Logger
from datalake_library.configuration import SQSConfiguration, StateMachineConfiguration
from datalake_library.interfaces import StatesInterface
from datalake_library.interfaces import SQSInterface
from cloudwatch_metrics import metrics

logger = Logger(service="SDLF pipeline stage B", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']

def lambda_handler(event, _):
    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SdlfHeavyTransformRedrive")
    try:
        team = os.environ['TEAM']
        pipeline = os.environ['PIPELINE']
        dataset = event['dataset']
        stage = os.environ['STAGE']
        state_config = StateMachineConfiguration(resource_prefix, team, pipeline, stage)
        sqs_config = SQSConfiguration(resource_prefix, team, dataset, stage)
        dlq_interface = SQSInterface(sqs_config.get_stage_dlq_name)

        messages = dlq_interface.receive_messages(1)
        if not messages:
            logger.info('No messages found in {}'.format(
                sqs_config.get_stage_dlq_name))
            return

        logger.info('Received {} messages'.format(len(messages)))
        for message in messages:
            logger.info('Starting State Machine Execution')
            if isinstance(message.body, str):
                response = json.loads(message.body)
            StatesInterface().run_state_machine(
                state_config.get_stage_state_machine_arn, response)
            # record State Machine invocation to CloudWatch metric
            metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SdlfHeavyTransformRedriveSM")
            message.delete()
            logger.info('Delete message succeeded')
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
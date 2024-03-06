# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from datalake_library.configuration import StateMachineConfiguration
from datalake_library.interfaces import StatesInterface
from aws_lambda_powertools import Logger
from cloudwatch_metrics import metrics
logger = Logger(service="SDLF pipeline stage A", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']


def lambda_handler(event, _):
    try:
        logger.info('Received {} messages'.format(len(event['Records'])))
        for record in event['Records']:
            logger.info('Starting State Machine Execution')
            event_body = json.loads(record['body'])
            state_config = StateMachineConfiguration(
                resource_prefix,
                event_body['team'],
                event_body['pipeline'],
                event_body['pipeline_stage'])
            StatesInterface().run_state_machine(
                state_config.get_stage_state_machine_arn, record['body'])
            # Record anonymized metric
            metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SdlfLightTransformSM")
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

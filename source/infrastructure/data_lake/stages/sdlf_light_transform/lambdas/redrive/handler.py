# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from aws_lambda_powertools import Logger
from datalake_library.configuration import SQSConfiguration
from datalake_library.interfaces import SQSInterface
from cloudwatch_metrics import metrics

logger = Logger(service="SDLF pipeline stage A", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
team = os.environ['TEAM']
pipeline = os.environ['PIPELINE']
stage = os.environ['STAGE']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']


def lambda_handler(event, _):  # NOSONAR
    metrics.Metrics(METRICS_NAMESPACE, resource_prefix, logger).put_metrics_count_value_1(
        metric_name="SdlfLightTransformRedrive")
    try:
        sqs_config = SQSConfiguration(resource_prefix, team, pipeline, stage)
        dlq_interface = SQSInterface(sqs_config.get_stage_dlq_name)
        messages = dlq_interface.receive_messages(1)
        if not messages:
            logger.info('No messages found in {}'.format(
                sqs_config.get_stage_dlq_name))
            return

        logger.info('Received {} messages'.format(len(messages)))
        queue_interface = SQSInterface(sqs_config.get_stage_queue_name)
        for message in messages:
            queue_interface.send_message_to_fifo_queue(message.body, 'redrive')
            message.delete()
            logger.info('Delete message succeeded')
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

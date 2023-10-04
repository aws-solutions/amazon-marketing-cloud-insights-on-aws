# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from aws_lambda_powertools import Logger
from datalake_library.configuration import SQSConfiguration
from datalake_library.interfaces import SQSInterface
from cloudwatch_metrics import metrics

logger = Logger(service="SDLF pipeline stage A", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']


def lambda_handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, resource_prefix, logger).put_metrics_count_value_1(
        metric_name="SdlfLightTransformError")

    try:
        if isinstance(event, str):
            event = json.loads(event)
        sqs_config = SQSConfiguration(resource_prefix, event['team'], event['pipeline'], event['pipeline_stage'])
        sqs_interface = SQSInterface(sqs_config.get_stage_dlq_name)

        logger.info('Execution Failed. Sending original payload to DLQ')
        sqs_interface.send_message_to_fifo_queue(json.dumps(event), 'failed')
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

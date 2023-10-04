# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_lambda_powertools import Logger # Ensure Lambda has an AWS Lambda Powertools configured


def init_logger(log_level=None):
    if not log_level:
        log_level = 'INFO'
    logger = Logger(service="Data Lake Library", level=log_level, utc=True)
    return logger

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
import uuid
from crhelper import CfnResource
from aws_lambda_powertools import Logger
from aws_solutions.core.helpers import get_service_client

logger = Logger(service='Stack uuid secret for operational metrics', level="INFO")

helper = CfnResource()

STACK_NAME = os.environ['STACK_NAME']
secret_name = f"{STACK_NAME}-anonymized-metrics-uuid"


def event_handler(event, context):
    """
    This function is the entry point for the Lambda-backed custom resource.
    """
    logger.info(event)
    helper(event, context)


@helper.create
def on_create(event, _):
    """
    Lambda entry point. Print the event first.
    """
    logger.info(f"Create event input: {json.dumps(event)}")
    create_uuid()


def create_uuid():
    """
    This function is responsible for creating the Secrets Manager uuid for anonymized metrics.
    """

    secrets_manager_client = get_service_client("secretsmanager")
    secret_value = str(uuid.uuid4())

    secrets_manager_client.create_secret(
        Name=secret_name,
        SecretString=secret_value
    )

    logger.info("Secret created successfully!")


@helper.delete
def on_delete(event, _):
    """
    This function is responsible for deleting the Secrets Manager uuid.
    """
    logger.info(f"Delete event input: {json.dumps(event)}")
    logger.info(f"Resource marked for deletion: {secret_name}")
    delete_secret()


def delete_secret():
    secrets_manager_client = get_service_client("secretsmanager")

    # delete the secret
    secrets_manager_client.delete_secret(
        SecretId=secret_name,
        ForceDeleteWithoutRecovery=True
    )
    logger.info("UUID secret deleted successfully.")

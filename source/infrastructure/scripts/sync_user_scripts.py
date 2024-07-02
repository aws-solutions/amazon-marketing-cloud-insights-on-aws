# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from crhelper import CfnResource
from aws_solutions.core.helpers import get_service_client
from aws_lambda_powertools import Logger

logger = Logger(service='Sync user scripts', level="INFO")

CLEANUP_SCRIPTS_DIR = "cleanup_scripts"
DIRS = [
    CLEANUP_SCRIPTS_DIR
    ]

helper = CfnResource(log_level="ERROR", boto_level="ERROR")


def event_handler(event, context):
    """
    This is the Lambda custom resource entry point.
    """
    logger.info(event)
    helper(event, context)


@helper.create
@helper.update
def on_create_or_update(event, _) -> None:
    """
    This function handles the move of user scripts to S3 artifacts
    """
    resource_properties = event["ResourceProperties"]
    try:
        upload_bucket_contents(resource_properties)
    except Exception as err:
        logger.error(err)
        raise err
    logger.info(
        f'Move user scripts to S3 artifacts {resource_properties["artifacts_bucket_name"]} {resource_properties["artifacts_key_prefix"]}')


@helper.delete
def on_delete(event, _):
    """
    This function takes no action when the custom resource is deleted as the bucket and contents are retained.
    """
    logger.info("Custom Resource marked for deletion. No Action needed.")



def upload_bucket_contents(resource_properties) -> None:
    s3_client = get_service_client("s3")
    for directory in DIRS:
        for file in list_files_to_sync(directory):
            artifacts_bucket_name, object_key = get_bucket_name_and_key(resource_properties, file)
            s3_client.upload_file(
                file,
                artifacts_bucket_name,
                object_key,
            )
            logger.info(f"Uploaded {object_key}")


def get_bucket_name_and_key(resource_properties, file):
    artifacts_bucket_name: str = resource_properties["artifacts_bucket_name"]
    artifacts_key_prefix: str = resource_properties["artifacts_key_prefix"]
    object_key = f"{artifacts_key_prefix}{file}"
    return artifacts_bucket_name, object_key


def list_files_to_sync(root_dir):
    files = []

    def walk_directory(directory):
        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)
            if os.path.isdir(file_path):
                walk_directory(file_path)
            else:
                files.append(file_path)

    walk_directory(root_dir)

    return files

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from crhelper import CfnResource
from aws_solutions.core.helpers import get_service_client
from aws_lambda_powertools import Logger

logger = Logger(service='Sync SDLF heavy transform glue script', level="INFO")

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
    This function is responsible for placing the glue script of sdlf heavy transform to S3 artifacts bucket.
    """
    resource_properties = event["ResourceProperties"]
    try:
        upload_bucket_contents(resource_properties)
    except Exception as err:
        logger.error(err)
        raise err
    logger.info(
        f'Move glue script of sdlf heavy transform to S3 artifacts bucket {resource_properties["artifacts_bucket_name"]} {resource_properties["artifacts_object_key"]}')


@helper.delete
def on_delete(event, _):
    """
    This function takes no action when the custom resource is deleted as the bucket and contents are retained.
    """
    logger.info("Custom Resource marked for deletion. No Action needed.")


def delete_bucket_contents(resource_properties) -> None:
    s3_client = get_service_client("s3")
    artifacts_bucket_name, object_key = resource_properties["artifacts_bucket_name"], resource_properties[
        "artifacts_object_key"]
    s3_client.delete_object(
        Bucket=artifacts_bucket_name,
        Key=object_key
    )
    logger.info(f"Deleted {object_key}")


def upload_bucket_contents(resource_properties) -> None:
    s3_client = get_service_client("s3")
    artifacts_bucket_name, object_key = resource_properties["artifacts_bucket_name"], resource_properties[
        "artifacts_object_key"]
    sdlf_heavy_transform_glue_script_file = resource_properties["glue_script_file"]
    s3_client.upload_file(
        sdlf_heavy_transform_glue_script_file,
        artifacts_bucket_name,
        object_key,
    )
    logger.info(f"Uploaded {object_key}")

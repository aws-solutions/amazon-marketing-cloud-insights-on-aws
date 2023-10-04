# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os

from crhelper import CfnResource
from aws_solutions.core.helpers import get_service_client
from aws_lambda_powertools import Logger
from aws_solutions.extended.resource_lookup import ResourceLookup

logger = Logger(service='Set S3 and Lambda data event', level="INFO")

CLOUD_TRAIL_ARN = os.environ["CLOUD_TRAIL_ARN"]
ARTIFACTS_BUCKET_ARN = os.environ["ARTIFACTS_BUCKET_ARN"]
RAW_BUCKET_LOGICAL_ID = os.environ["RAW_BUCKET_LOGICAL_ID"]
STAGE_BUCKET_LOGICAL_ID = os.environ["STAGE_BUCKET_LOGICAL_ID"]
ATHENA_BUCKET_LOGICAL_ID = os.environ["ATHENA_BUCKET_LOGICAL_ID"]
LAMBDA_FUNCTION_ARNS_START_WITH = os.environ["LAMBDA_FUNCTION_ARNS_START_WITH"]
STACK_NAME = os.environ["STACK_NAME"]
DATA_LAKE_ENABLED = os.environ["DATA_LAKE_ENABLED"]

helper = CfnResource()


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
    This function sets S3 and Lambda data event in CloudTrail
    """
    logger.info(f"Add S3 and Lambda data event in CloudTrail in {event}")

    client = get_service_client('cloudtrail')

    bucket_event_arns = {
        "Field": "resources.ARN",
        "StartsWith": [
            f"{ARTIFACTS_BUCKET_ARN}/"
        ]
    }
    if DATA_LAKE_ENABLED == "Yes":
        raw_bucket_id = ResourceLookup(
            logical_id=RAW_BUCKET_LOGICAL_ID,
            stack_name=STACK_NAME).physical_id
        stage_bucket_id = ResourceLookup(
            logical_id=STAGE_BUCKET_LOGICAL_ID,
            stack_name=STACK_NAME).physical_id
        athena_bucket_id = ResourceLookup(
            logical_id=ATHENA_BUCKET_LOGICAL_ID,
            stack_name=STACK_NAME).physical_id

        bucket_event_arns["StartsWith"].extend(
            [
                f"arn:aws:s3:::{raw_bucket_id}/",
                f"arn:aws:s3:::{stage_bucket_id}/",
                f"arn:aws:s3:::{athena_bucket_id}/"
            ]
        )

    client.put_event_selectors(
        TrailName=CLOUD_TRAIL_ARN,
        AdvancedEventSelectors=[
            {
                "Name": "S3EventSelector",
                "FieldSelectors": [
                    {
                        "Field": "eventCategory",
                        "Equals": [
                            "Data"
                        ]
                    },
                    {
                        "Field": "resources.type",
                        "Equals": [
                            "AWS::S3::Object"
                        ]
                    },
                    bucket_event_arns
                ]
            },
            {
                "Name": "LambdaEventSelector",
                "FieldSelectors": [
                    {
                        "Field": "eventCategory",
                        "Equals": [
                            "Data"
                        ]
                    },
                    {
                        "Field": "resources.type",
                        "Equals": [
                            "AWS::Lambda::Function"
                        ]
                    },
                    {
                        "Field": "resources.ARN",
                        "StartsWith": [
                            LAMBDA_FUNCTION_ARNS_START_WITH
                        ]
                    }
                ]
            }
        ]
    )


@helper.delete
def on_delete(event, _):
    logger.info(f"Delete event input: {json.dumps(event)}. Nothing to action.")

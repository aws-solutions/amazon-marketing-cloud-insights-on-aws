# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from aws_solutions.core.helpers import get_service_client
import os
from aws_lambda_powertools import Logger
from decimal import Decimal
import re
from cloudwatch_metrics import metrics

STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']
DATASET_NAME = os.environ['DATASET_NAME']
TEAM_NAME = os.environ['TEAM_NAME']
APPLICATION_ACCOUNT = os.environ['APPLICATION_ACCOUNT']
DEFAULT_SNS_TOPIC = os.environ['DEFAULT_SNS_TOPIC']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
STACK_NAME = os.environ['STACK_NAME']
APPLICATION_REGION = os.environ['APPLICATION_REGION']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']

logger = Logger(service="Tenant Provisioning Service", level="INFO")

client = get_service_client('stepfunctions')


def json_encoder_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)

    if not isinstance(obj, str):
        return str(obj)

def get_amc_region(endpoint_url):
    match = re.search(r'(?<=\.execute-api\.)[a-z0-9-]+(?=\.amazonaws\.com)', endpoint_url)
    if match:
        return match.group(0)
    else:
        logger.info("The AMC API endpoint provided was invalid. Check value passed.")

def format_payload(customer_details):
    amc_api_endpoint = customer_details["amc"]["endpoint_url"]
    customer_id = customer_details['customer_id']
    amc_region = get_amc_region(amc_api_endpoint)
    sns_topic = DEFAULT_SNS_TOPIC

    payload = {
        # Customer info for microservices
        "TenantName": customer_id,
        "customerName": customer_details['customer_name'],
        "createSnsTopic": "false",
        "snsTopicArn": sns_topic,

        # Customer info for AMC instance
        "amcOrangeAwsAccount":customer_details["amc"]["aws_orange_account_id"],
        "BucketName":customer_details["amc"]["bucket_name"],
        "amcDatasetName":DATASET_NAME,
        "amcApiEndpoint": amc_api_endpoint,
        "amcTeamName":TEAM_NAME,
        "amcRegion": amc_region,
        "amcRedAwsAccount":customer_details["amc"]["aws_red_account_id"],

        # Customer info for S3 bucket deployment
        "bucketExists":customer_details.get('bucket_exists', "true"),
        "bucketAccount":customer_details.get('bucket_account', APPLICATION_ACCOUNT),
        "bucketRegion":customer_details.get('bucket_region', APPLICATION_REGION)
        }
    return payload

def handler(event, _):
    logger.info(f'event received:{event}')
    customer_details_raw = event['customer_details']

    customer_details_formatted = format_payload(customer_details_raw)

    response = client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(customer_details_formatted)
    )

    message = f"created state machine execution response : {response} for customer {customer_details_formatted['TenantName']}"
    logger.info(message)

    # Record anonymized metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="InvokeTPSInitializeSM")

    return json.dumps(response, default=json_encoder_default)
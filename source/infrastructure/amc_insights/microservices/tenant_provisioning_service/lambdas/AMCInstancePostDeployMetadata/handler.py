# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_solutions.core.helpers import get_service_resource, get_service_client
from aws_solutions.extended.resource_lookup import ResourceLookup
import os
import json
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from cloudwatch_metrics import metrics

logger = Logger(service="Tenant Provisioning Service", level="INFO")

dynamodb = get_service_resource('dynamodb')
cloudtrail = get_service_client('cloudtrail')
s3_client = get_service_client("s3")

DATA_LAKE_ENABLED = os.environ['DATA_LAKE_ENABLED']
WFM_CUSTOMER_CONFIG_TABLE = os.environ['WFM_CUSTOMER_CONFIG_TABLE']
TPS_CUSTOMER_CONFIG_TABLE = os.environ['TPS_CUSTOMER_CONFIG_TABLE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
AWS_ACCOUNT_ID = os.environ['AWS_ACCOUNT_ID']
APPLICATION_REGION = os.environ['APPLICATION_REGION']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
SDLF_CUSTOMER_CONFIG_LOGICAL_ID = os.environ['SDLF_CUSTOMER_CONFIG_LOGICAL_ID']
LOGGING_BUCKET_NAME = os.environ["LOGGING_BUCKET_NAME"]


def put_item(table, item):
    try:
        response = table.put_item(
            Item=item
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info(e.response['Error']['Message'])
        else:
            raise
    else:
        return response


def enable_bucket_sever_access_logs(amc_bucket_name, access_logs_bucket_name):
    res = s3_client.put_bucket_logging(
        Bucket=amc_bucket_name,
        BucketLoggingStatus={
            'LoggingEnabled': {
                'TargetBucket': access_logs_bucket_name,
                'TargetPrefix': f"{amc_bucket_name}-access-logs/"
            }
        }
    )
    return res


def check_and_assign_server_access_logs(event):
    amc_bucket_region = event["bucketRegion"]
    cloudformation_client = get_service_client("cloudformation", region_name=amc_bucket_region)
    cross_region_logs_stack_name = f"{RESOURCE_PREFIX}-tps-{event['bucketRegion']}-access-logs"
    response = cloudformation_client.describe_stacks(StackName=cross_region_logs_stack_name)
    for stack in response['Stacks']:
        for output in stack.get('Outputs', []):
            if output['OutputKey'] == "S3LogsBucketName":
                access_logs_bucket_name = output['OutputValue']
    logger.info(f"Enabling server access logs for amc bucket in {access_logs_bucket_name}")
    s3_response = enable_bucket_sever_access_logs(amc_bucket_name=event['BucketName'],
                                                  access_logs_bucket_name=access_logs_bucket_name)
    return s3_response


def put_amc_bucket_policy(amc_orange_aws_account, bucket_account, bucket_name):
    amc_bucket_policy = {
        "Version": "2012-10-17",
        "Id": "BucketDeliveryPolicy",
        "Statement": [
            {
                "Sid": "BucketDelivery",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{amc_orange_aws_account}:root"
                },
                "Action": [
                    "s3:PutObject",
                    "s3:PutObjectAcl"
                ],
                "Resource": f"arn:aws:s3:::{bucket_name}/*"
            },
            {
                "Sid": "BucketOwnerAccess",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{bucket_account}:root"
                },
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}/*",
                    f"arn:aws:s3:::{bucket_name}"
                ]
            }
        ]
    }
    try:
        response = s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(amc_bucket_policy)
        )
        return response
    except Exception as error:
        logger.exception(
            f"""
            Failed to add bucket policy to the AMC bucket {bucket_name}, error: {error}.
            
            Users can manually paste the following policy
            {json.dumps(amc_bucket_policy)}
            into AMC S3 bucket {bucket_name}
            """
        )
        raise


def check_and_assign_bucket_policy(event):
    amc_orange_aws_account = event.get("amcOrangeAwsAccount")
    bucket_name = event.get("BucketName")
    bucket_account = event.get("bucketAccount")

    try:
        response = s3_client.get_bucket_policy(
            Bucket=bucket_name,
        )

    except s3_client.exceptions.from_code('NoSuchBucketPolicy'):
        logger.info(f"Add bucket policy to AMC S3 bucket {bucket_name}")
        response = put_amc_bucket_policy(amc_orange_aws_account, bucket_account, bucket_name)

    return response


def handler(event, _):
    try:
        metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
            metric_name="AddAMCInstancePostDeployMetadata")

        if event['body']['stackStatus'] in ['CREATE_COMPLETE', 'UPDATE_COMPLETE']:
            logger.info('Status is {}'.format(event['body']['stackStatus']))

            response_list = []

            if (event['bucketAccount'] == AWS_ACCOUNT_ID) and (event['bucketExists'] == "false"):
                # Enable sever access logs for an AMC bucket created by this solution.
                response_list.append(
                    check_and_assign_server_access_logs(event=event)
                )
            else:
                # If bucket exists in separate account or bucket exists in same account,
                # AMC bucket is not created by this solution, skip access logging configuration
                logger.info(
                    f"AMC Bucket exists {event['bucketExists']} in {event['bucketAccount']}. Do not enable access logs")

            if (event['bucketAccount'] == AWS_ACCOUNT_ID) and (event['bucketExists'] == "true"):
                # Verify that an AMC bucket policy is in place. If no policy exists, create and assign one.
                bucket_policy_response = check_and_assign_bucket_policy(event)
                response_list.append(bucket_policy_response)

            logger.info('Updating Metadata in DDB')

            logger.info('Updating Tenant Provisioning Customer Config table')
            tps_customer_table = dynamodb.Table(TPS_CUSTOMER_CONFIG_TABLE)
            tps_item = {
                "customerId": event['TenantName'],
                "customerName": event['customerName'],
                "amcOrangeAwsAccount": event['amcOrangeAwsAccount'],
                "BucketName": event['BucketName'],
                "amcDatasetName": event['amcDatasetName'],
                "amcTeamName": event['amcTeamName'],
                "bucketExists": event['bucketExists'],
                "bucketAccount": event['bucketAccount'],
                "bucketRegion": event['bucketRegion'],
                "amcInstanceId": event['amcInstanceId'],
                "amcAmazonAdsAdvertiserId": event['amcAmazonAdsAdvertiserId'],
                "amcAmazonAdsMarketplaceId": event['amcAmazonAdsMarketplaceId'],
            }
            tps_response = put_item(tps_customer_table, tps_item)
            response_list.append(tps_response)

            logger.info('Checking For Data Lake Customer Config table')
            if DATA_LAKE_ENABLED == "No":
                logger.info("Skipping update to SDLF since data lake is not enabled")

            ## Updating SDLF customer config table for AMC datasets
            else:
                logger.info('Updating SDLF Customer Config table')

                table_name = ResourceLookup(logical_id=SDLF_CUSTOMER_CONFIG_LOGICAL_ID,
                                            stack_name=RESOURCE_PREFIX).physical_id

                sdlf_customer_table = dynamodb.Table(table_name)

                sdlf_item = {
                    "customer_hash_key": event['TenantName'],
                    "dataset": event['amcDatasetName'],
                    "team": event['amcTeamName'],
                    "hash_key": event['BucketName'],
                    "prefix": event['TenantName']
                }
                sdlf_response = put_item(sdlf_customer_table, sdlf_item)
                response_list.append(sdlf_response)

            # Update WFM customer config table
            logger.info('Updating Workflow Management Customer Config table')

            wfm_customer_table = dynamodb.Table(WFM_CUSTOMER_CONFIG_TABLE)
            wfm_item = {
                "customerId": event['TenantName'],
                "amcInstanceId": event['amcInstanceId'],
                "amcAmazonAdsAdvertiserId": event['amcAmazonAdsAdvertiserId'],
                "amcAmazonAdsMarketplaceId": event['amcAmazonAdsMarketplaceId'],
                "outputSNSTopicArn": event["snsTopicArn"]
            }
            wfm_response = put_item(wfm_customer_table, wfm_item)
            response_list.append(wfm_response)

            return response_list

        else:
            logger.info('Status is {}'.format(event['body']['stackStatus']))
            logger.info('Skipping Metadata Update in DDB')
            response = 'Skipping Metadata Update in DDB'
            return response
    except Exception as e:
        logger.error(e)
        raise e

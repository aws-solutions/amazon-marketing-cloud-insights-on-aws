# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_solutions.core.helpers import get_service_resource, get_service_client
from aws_solutions.extended.resource_lookup import ResourceLookup
import os
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from cloudwatch_metrics import metrics

logger = Logger(service="Tenant Provisioning Service", level="INFO")

dynamodb = get_service_resource('dynamodb')
cloudtrail = get_service_client('cloudtrail')

DATA_LAKE_ENABLED = os.environ['DATA_LAKE_ENABLED']
WFM_CUSTOMER_CONFIG_TABLE = os.environ['WFM_CUSTOMER_CONFIG_TABLE']
TPS_CUSTOMER_CONFIG_TABLE = os.environ['TPS_CUSTOMER_CONFIG_TABLE']
RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
AWS_ACCOUNT_ID = os.environ['AWS_ACCOUNT_ID']
APPLICATION_REGION = os.environ['APPLICATION_REGION']
API_INVOKE_ROLE_STANDARD = os.environ['API_INVOKE_ROLE_STANDARD']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
SDLF_CUSTOMER_CONFIG_LOGICAL_ID = os.environ['SDLF_CUSTOMER_CONFIG_LOGICAL_ID']
CLOUD_TRAIL_ARN = os.environ["CLOUD_TRAIL_ARN"]


def return_api_invoke_role_arn(event):
    if event['amcRedAwsAccount'] == AWS_ACCOUNT_ID:
        role_name = API_INVOKE_ROLE_STANDARD
    else:
        role_name = f"{RESOURCE_PREFIX}-{APPLICATION_REGION}-{event['TenantName']}-invokeAmcApiRole"

    return f"arn:aws:iam::{event['amcRedAwsAccount']}:role/{role_name}"


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
    
def scan_table(table_name):
    dynamodb_client = get_service_client("dynamodb")
    try:
        results = []
        last_evaluated_key = None
        while True:
            if last_evaluated_key:
                response = dynamodb_client.scan(
                    TableName=table_name,
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                response = dynamodb_client.scan(TableName=table_name)
            last_evaluated_key = response.get('LastEvaluatedKey')
            results.extend(response['Items'])
            if not last_evaluated_key:
                break
        return results
    except Exception as e:
        logger.info(e)

def add_bucket_to_trail(bucket_name, trail):
    table_items = scan_table(table_name=TPS_CUSTOMER_CONFIG_TABLE)
    bucket_list = [f"arn:aws:s3:::{bucket_name}/"]
    if len(table_items) > 0:
        for item in table_items:
            bucket = item['BucketName']['S']
            bucket_arn = f"arn:aws:s3:::{bucket}/"
            bucket_list.append(bucket_arn)

    response = cloudtrail.get_event_selectors(TrailName=trail)
    event_selectors = response['AdvancedEventSelectors']
    for i in event_selectors:
        if i['Name'] == "S3EventSelector":
            for y in i['FieldSelectors']:
                if y['Field'] == 'resources.ARN':
                    y['StartsWith'] = list(set(bucket_list) | set(y['StartsWith']))
    response = cloudtrail.put_event_selectors(
        TrailName=trail,
        AdvancedEventSelectors=event_selectors
    )
    return response


def handler(event, _):
    response = None
    try:
        metrics.Metrics(METRICS_NAMESPACE, RESOURCE_PREFIX, logger).put_metrics_count_value_1(
            metric_name="AddAMCInstancePostDeployMetadata")

        if event['body']['stackStatus'] in ['CREATE_COMPLETE', 'UPDATE_COMPLETE']:
            logger.info('Status is {}'.format(event['body']['stackStatus']))
            logger.info('Updating Metadata in DDB')

            response_list = []

            if event['bucketAccount'] == AWS_ACCOUNT_ID:
                # update cloudtrail to track amc bucket
                logger.info('Adding AMC bucket to CloudTrail')
                trail_response = add_bucket_to_trail(bucket_name=event['BucketName'], trail=CLOUD_TRAIL_ARN)
                response_list.append(trail_response)

            logger.info('Updating Tenant Provisioning Customer Config table')
            tps_customer_table = dynamodb.Table(TPS_CUSTOMER_CONFIG_TABLE)
            tps_item = {
                "customerId": event['TenantName'],
                "customerName": event['customerName'],
                "amcOrangeAwsAccount": event['amcOrangeAwsAccount'],
                "BucketName": event['BucketName'],
                "amcDatasetName": event['amcDatasetName'],
                "amcApiEndpoint": event['amcApiEndpoint'],
                "amcTeamName": event['amcTeamName'],
                "amcRegion": event['amcRegion'],
                "bucketExists": event['bucketExists'],
                "bucketAccount": event['bucketAccount'],
                "bucketRegion": event['bucketRegion']
            }
            tps_response = put_item(tps_customer_table, tps_item)
            response_list.append(tps_response)

            logger.info('Checking For Data Lake Customer Config table')
            if DATA_LAKE_ENABLED == "No":
                logger.info("Skipping update to SDLF since data lake is not enabled")

            ## Updating SDLF customer config table for AMC datasets
            else:
                logger.info('Updating SDLF Customer Config table')

                table_name = ResourceLookup(logical_id=SDLF_CUSTOMER_CONFIG_LOGICAL_ID, stack_name=RESOURCE_PREFIX).physical_id

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
                "amcApiEndpoint": event['amcApiEndpoint'],
                "amcRegion": event["amcRegion"],
                "invokeAmcApiRoleArn": return_api_invoke_role_arn(event),
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

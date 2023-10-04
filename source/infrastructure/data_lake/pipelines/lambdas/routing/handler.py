# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
from datetime import datetime
import uuid
from urllib.parse import unquote_plus
from aws_solutions.core.helpers import get_service_client, get_service_resource
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from cloudwatch_metrics import metrics

logger = Logger(service="Routes to the right team and pipeline", level="INFO", utc=True)

resource_prefix = os.environ["RESOURCE_PREFIX"]
environment_id = os.environ['ENV']
SDLF_CUSTOMER_CONFIG = os.environ['SDLF_CUSTOMER_CONFIG']
OCTAGON_DATASET_TABLE_NAME = os.environ['OCTAGON_DATASET_TABLE_NAME']
OCTAGON_METADATA_TABLE_NAME = os.environ['OCTAGON_METADATA_TABLE_NAME']
STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']

sqs = get_service_resource("sqs")
ssm = get_service_client("ssm")
dynamodb = get_service_resource("dynamodb")

dataset_table = dynamodb.Table(OCTAGON_DATASET_TABLE_NAME)
catalog_table = dynamodb.Table(OCTAGON_METADATA_TABLE_NAME)

cloudtrail_detail_type = ['AWS API Call via CloudTrail']
eventbridge_detail_type = ['Object Created', 'Object Deleted']

def parse_s3_event(s3_event):
    logger.info('Parsing S3 Event')
    # the event structure can come in 2 different formats depending on if 
    # the source is CloudTrail (main account) or EventBridge (cross-account)
    if s3_event['detail-type'] in cloudtrail_detail_type:
        return {
            'bucket': s3_event["detail"]["requestParameters"]["bucketName"],
            'key': s3_event["detail"]["requestParameters"]["key"],
            'timestamp': int(round(datetime.utcnow().timestamp() * 1000, 0)),
            'last_modified_date': s3_event['time'].split('.')[0] + '+00:00'
        }
    elif s3_event['detail-type'] in eventbridge_detail_type:
        return {
            'bucket': s3_event["detail"]["bucket"]["name"],
            'key': s3_event["detail"]["object"]["key"],
            'timestamp': int(round(datetime.utcnow().timestamp() * 1000, 0)),
            'last_modified_date': s3_event['time'].split('.')[0] + '+00:00'
        }
    else:
        raise KeyError("Unrecognized event source. Check EventBridge rule.")


def get_item(table, team, dataset):
    try:
        response = table.get_item(
            Key={
                'name': '{}-{}'.format(team, dataset)
            }
        )
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise e
    else:
        item = response['Item']
        return item['pipeline']


def delete_item(table, key):
    try:
        response = table.delete_item(
            Key=key
        )
    except ClientError as e:
        logger.error('Fatal error', exc_info=True)
        raise e
    else:
        return response


def put_item(table, item, key):
    try:
        response = table.put_item(
            Item=item,
            ConditionExpression=f"attribute_not_exists({key})",
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info(e.response['Error']['Message'])
        else:
            raise
    else:
        return response


def catalog_item(s3_event, message):
    try:
        event_type = s3_event['detail-type']
        if event_type in cloudtrail_detail_type:
            operation = s3_event["detail"]["eventName"]
        elif event_type in eventbridge_detail_type:
            operation = event_type
        if operation in ['DeleteObject', 'Object Deleted']:
            item_id = 's3://{}/{}'.format(
                message['bucket'],
                unquote_plus(message['key'])
            )
            delete_item(catalog_table, {'id': item_id})
        else:
            message['id'] = f"s3://{message['bucket']}/{message['key']}"
            message['stage'] = message['bucket'].split('-')[-1]
            if message['stage'] not in ['raw', 'stage', 'analytics']:
                message['stage'] = 'raw'
            put_item(catalog_table, message, 'id')
    except ClientError as e:
        logger.info(e.response['Error']['Message'])
    else:
        return message


def lambda_handler(event, context):
    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="DatalakeRouting")
    try:
        logger.info(f"Event: {event}, context: {context}")

        message = parse_s3_event(event)
        message = catalog_item(event, message)

        if message['stage'] == 'raw':
            team = message['key'].split('/')[0]
            dataset = message['key'].split('/')[1]

            logger.info(
                'team: {}; dataset: {}; bucket: {}; key: {}'.format(team, dataset, message['bucket'], message['key']))

            try:
                pipeline = get_item(dataset_table, team, dataset)
            except Exception as e:
                logger.info('Exception thrown')
                logger.info(str(e))
                logger.info('Checking if ingestion is from outside data lake...')

                config_table = dynamodb.Table(SDLF_CUSTOMER_CONFIG)

                response = config_table.query(
                    IndexName='amc-index',
                    Select='ALL_PROJECTED_ATTRIBUTES',
                    KeyConditionExpression=Key('hash_key').eq(message['bucket'])
                )

                dataset = response['Items'][0]['dataset']
                team = response['Items'][0]['team']

                pipeline = get_item(dataset_table, team, dataset)

            message['team'] = team
            message['dataset'] = dataset
            message['pipeline'] = pipeline
            message['env'] = environment_id
            message['pipeline_stage'] = 'StageA'

        logger.info(
            'Sending event to {}-{} pipeline queue for processing'.format(team, pipeline))
        queue = sqs.get_queue_by_name(QueueName='{}-{}-{}-queue-a.fifo'.format(
            resource_prefix,
            team,
            pipeline
        ))
        queue.send_message(MessageBody=json.dumps(
            message), MessageGroupId='{}-{}'.format(team, dataset),
            MessageDeduplicationId=str(uuid.uuid1()))
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

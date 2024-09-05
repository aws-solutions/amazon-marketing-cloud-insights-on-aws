# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import urllib3

from microservice_shared.utilities import LoggerUtil
from aws_solutions.core.helpers import get_service_client
from cloudwatch_metrics import metrics

RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
ADS_REPORT_BUCKET = os.environ['ADS_REPORT_BUCKET']
ADS_REPORT_BUCKET_KMS_KEY_ID = os.environ['ADS_REPORT_BUCKET_KMS_KEY_ID']
TEAM = os.environ['TEAM']
DATASET = os.environ['DATASET']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
STACK_NAME = os.environ['STACK_NAME']

logger = LoggerUtil.create_logger()


def handler(event, _):
    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="AdsDownloadReport")

    logger.info(f"Event: {event}")
    
    table_name = event["tableName"]
    report_id = event["reportId"]
    report_url = event["url"]
    filename = f"report-{report_id}"
    s3_key = f"{TEAM}/{DATASET}/{table_name}/{filename}.json.gz"

    # Download report with 'Content-Type': 'application/vnd.createasyncreportrequest.v3+json'
    try:
        s3 = get_service_client("s3")

        method = "GET"
        report_download_response = urllib3.PoolManager().request(method, report_url)

        s3.put_object(
            Body=report_download_response.data, 
            Bucket=ADS_REPORT_BUCKET,
            Key=s3_key,
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=ADS_REPORT_BUCKET_KMS_KEY_ID,
            Metadata={
                'timestamp': event.get('timestamp')
            }
        )

    except Exception as error:
        logger.error(f"Failed to download report from {report_url} to {ADS_REPORT_BUCKET}")
        logger.error(error)
        raise error

    return event

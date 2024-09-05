# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import urllib3
import gzip
import io

from microservice_shared.utilities import LoggerUtil
from aws_solutions.core.helpers import get_service_client
from cloudwatch_metrics import metrics

RESOURCE_PREFIX = os.environ['RESOURCE_PREFIX']
SP_REPORT_BUCKET = os.environ['SP_REPORT_BUCKET']
SP_REPORT_BUCKET_KMS_KEY_ID = os.environ['SP_REPORT_BUCKET_KMS_KEY_ID']
TEAM = os.environ['TEAM']
DATASET = os.environ['DATASET']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
STACK_NAME = os.environ['STACK_NAME']

logger = LoggerUtil.create_logger()

def get_file_extension(event):
    """Get the file extension based on the compression type for the report file"""
    # Default to json if no compression is specified
    if "compressionAlgorithm" not in event:
        return "json"
    # Check compression type and return proper extension
    compression_type = event.get("compressionAlgorithm").upper()
    if compression_type == "GZIP":
        return "json.gz"
    # Raise error if unsupported value
    raise ValueError("Unsupported compression algorithm")

def set_error_message(event, error_message):
    event['errorMessage'] = error_message
    event['success'] = False

def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SellersPartnerDownloadReport")
    logger.info(f"Event: {event}")
    
    # we check the event to see if the report was FATAL or DONE.
    # if FATAL, that means there was an error processing and we unpack the error message here
    processing_status = event.get('processingStatus')
    if processing_status == 'FATAL':
        logger.error("Report processing failed. Retrieving error message")
        try:
            url = event['url']
            report_response = urllib3.PoolManager().request("GET", url)
            # we expect the file to always be gzip compressed but check anyways
            if event.get("compressionAlgorithm").upper() == 'GZIP':
                with gzip.GzipFile(fileobj=io.BytesIO(report_response.data)) as gz:
                    report_error = gz.read().decode("utf-8")
            else:
                # if uncompressed we handle here
                report_error = report_response.data.decode("utf-8")
            error_message = f"Report processing failed: {report_error}"
            logger.error(error_message)
            set_error_message(event, error_message)
        except Exception as e:
            error_message = f"Failed to download error message: {e}"
            logger.error(error_message)
            set_error_message(event, error_message)
        return event
        
    elif processing_status != "DONE":
        error_message = f"Unexpected processing status: {processing_status}"
        logger.error(error_message)
        set_error_message(event, error_message)
        return event
        
    # if DONE, that means processing was successful and we upload our report to S3 for processing
    table_prefix = event["tablePrefix"]
    report_id = event["reportId"]
    report_url = event["url"]
    filename = f"report-{report_id}"

    s3_key = f"{TEAM}/{DATASET}/{table_prefix}/{filename}.{get_file_extension(event)}"

    try:
        s3 = get_service_client("s3")

        method = "GET"
        report_download_response = urllib3.PoolManager().request(method, report_url)

        s3.put_object(
            Body=report_download_response.data, 
            Bucket=SP_REPORT_BUCKET,
            Key=s3_key,
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=SP_REPORT_BUCKET_KMS_KEY_ID,
            Metadata={
                'timestamp': event.get('timestamp')
            }
        )

    except Exception as error:
        error_message = f"Failed to download report from {report_url} to {SP_REPORT_BUCKET}: {error}"
        logger.error(error_message)
        set_error_message(event, error_message)
        return event

    event['success'] = True
    return event

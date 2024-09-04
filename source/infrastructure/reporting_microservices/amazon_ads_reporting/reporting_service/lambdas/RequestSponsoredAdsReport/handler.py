# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from microservice_shared.utilities import LoggerUtil, DateUtil
from amazon_ads_api_interface import amazon_ads_api_interface
from cloudwatch_metrics import metrics

logger = LoggerUtil.create_logger()

STACK_NAME = os.environ['STACK_NAME']
METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']


def handler(event, _):
    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="RequestSponsoredAdsReport")

    logger.info(f"Event: {event}")
    
    profile_id = event['profileId']
    auth_id = event.get('authId')
    region = event['region']
    request_body = event['requestBody']

    amazon_ads_reporting = amazon_ads_api_interface.AmazonAdsAPIs(
        region=region
        , auth_id=auth_id
    )
    
    amazon_ads_response = amazon_ads_reporting.request_sponsored_ads_v3_reporting(
        version_3_reporting_data=request_body
        , profile_id=profile_id
    )
    
    # A successful response will return reportId which gets added to the event and passed to the check status step
    event.update(amazon_ads_response.response)
    
    # generate a timestamp that will be added to each record during Stage B Glue processing
    try:
        timestamp = DateUtil.get_current_utc_iso_timestamp()
        event['timestamp'] = timestamp
        logger.info(f"Report timestamp: {timestamp}")
    except Exception as e:
        logger.error(f"Error generating report timestamp: {e}")

    return event

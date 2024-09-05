# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from microservice_shared.utilities import LoggerUtil
from amazon_ads_api_interface import amazon_ads_api_interface
from cloudwatch_metrics import metrics

logger = LoggerUtil.create_logger()

METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
STACK_NAME = os.environ['STACK_NAME']


def handler(event, _):
    # record Lambda invocation to CloudWatch metric
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="AdsCheckReportStatus")

    logger.info(f"Event: {event}")
    
    profile_id = event['profileId']
    auth_id = event.get('authId')
    region = event['region']
    report_id = event['reportId']
    
    amazon_ads_reporting = amazon_ads_api_interface.AmazonAdsAPIs(
        region=region,
        auth_id=auth_id
    )

    amazon_ads_response = amazon_ads_reporting.report_status(
        report_id=report_id
        , profile_id=profile_id
    )
    
    # Once the report is ready it will return a url in the response which gets addded to the event and passed to the download step
    event.update(amazon_ads_response.response)

    return event

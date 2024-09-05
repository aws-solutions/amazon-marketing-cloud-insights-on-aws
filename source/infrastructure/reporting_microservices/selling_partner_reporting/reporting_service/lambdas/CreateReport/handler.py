# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This Lambda creates a report using the Selling Partner API.

PREREQUISITES:
A valid OAuth refresh token for the SP-API must be stored in SELLING_PARTNER_SECRETS_MANAGER using key "refresh_token".

INPUT:
The Lambda even object must contain the following two arguments:

    region (str): API endpoint region. Must be "North America", "Europe", or "Far East".

    requestBody (dict): Report creation params per https://developer-docs.amazon.com/sp-api/docs/reports-api-v2021-06-30-reference#createreportspecification

Sample event object:

{
    "region": "North America",
    "requestBody": {
        "marketplaceIds": ["ATVPDKIKX0DER"],
        "reportType": "GET_VENDOR_SALES_REPORT",
        "reportOptions": {
            "reportPeriod": "YEAR",
            "distributorView": "SOURCING",
            "sellingProgram": "RETAIL"
        },
        "dataStartTime": "2023-01-01T00:00:00+00:00",
        "dataEndTime": "2023-12-31T00:00:00+00:00"
    }
}

OUTPUT:
A successful invocation will respond with an object in the following structure:

{
    "region": "<region>",
    "requestBody": {
    "marketplaceIds": [
        "<marketplace_id>"
    ],
    "reportType": "<report_type>",
    "reportOptions": {
        "reportPeriod": "<report_period>",
        "distributorView": "<distributor_view>",
        "sellingProgram": "<selling_program>"
    },
    "dataStartTime": "<start_time>",
    "dataEndTime": "<end_time>"
    },
    "responseReceivedTime": "<response_received_time>",
    "responseStatus": "<response_status>",
    "statusCode": <status_code>,
    "requestURL": "<request_url>",
    "reportId": "<report_id>"
}
"""

import os
from microservice_shared.utilities import LoggerUtil, DateUtil
from selling_partner_api_interface import selling_partner_api_interface
from cloudwatch_metrics import metrics

METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
STACK_NAME = os.environ['STACK_NAME']

logger = LoggerUtil.create_logger()

def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SellerPartnerCreateReport")
    logger.info(f"Event: {event}")
    
    region = event['region']
    request_body = event['requestBody']
    sp_api = selling_partner_api_interface.SellingPartnerAPI(
        region=region
        , auth_id=None
    )

    response = sp_api.create_report(
        request_body=request_body
    )

    event.update(response.response)
    
    # generate a timestamp that will be added to each record during Stage B Glue processing
    try:
        timestamp = DateUtil.get_current_utc_iso_timestamp()
        event['timestamp'] = timestamp
        logger.info(f"Report timestamp: {timestamp}")
    except Exception as e:
        logger.error(f"Error generating report timestamp: {e}")

    return event

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This Lambda returns report details (including the reportDocumentId, if available) for the specified report.

PREREQUISITES:
A valid OAuth access token for the SP-API must be stored in SELLING_PARTNER_SECRETS_MANAGER using key "access_token".

INPUT:
The Lambda event object must contain the following two arguments:

    region (str): API endpoint region. Must be "North America", "Europe", or "Far East".

    requestBody (dict): Request parameters according to the following schema: https://developer-docs.amazon.com/sp-api/docs/reports-api-v2021-06-30-reference#get-reports2021-06-30reportsreportid

Sample event object:

{
    "region": "North America",
    "reportId": "50031019933"
}

OUTPUT:
A successful invocation will respond with an object in the following structure:

{
    "region": "<region>",
    "reportId": "<report_id>",
    "responseReceivedTime": "<response_received_time>",
    "responseStatus": "<response_status>",
    "statusCode": <status_code>,
    "requestURL": "<request_url>",
    "reportType": "<report_type>",
    "processingEndTime": "<processing_end_time>",
    "processingStatus": "<processing_status>",
    "marketplaceIds": [
        "<marketplace_id>"
    ],
    "reportDocumentId": "<report_document_id>",
    "dataEndTime": "<data_end_time>",
    "createdTime": "<created_time>",
    "processingStartTime": "<processing_start_time>",
    "dataStartTime": "<data_start_time>"
}

"""

import os
from microservice_shared.utilities import LoggerUtil
from selling_partner_api_interface import selling_partner_api_interface
from cloudwatch_metrics import metrics

METRICS_NAMESPACE = os.environ['METRICS_NAMESPACE']
STACK_NAME = os.environ['STACK_NAME']

logger = LoggerUtil.create_logger()


def handler(event, _):
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SellersPartnerGetReportStatus")
    logger.info(f"Event: {event}")
    
    region = event['region']
    report_id = event['reportId']
    sp_api = selling_partner_api_interface.SellingPartnerAPI(
        region=region
        , auth_id=None
    )

    response = sp_api.get_report_status(
        report_id=report_id
    )

    event.update(response.response)

    return event

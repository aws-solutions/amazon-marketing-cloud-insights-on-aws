# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This Lambda returns the information required for retrieving a report document's contents.

PREREQUISITES:
A valid OAuth access token for the SP-API must be stored in SELLING_PARTNER_SECRETS_MANAGER using key "access_token".

INPUT:
The Lambda event object must contain the following two arguments:

    region (str): API endpoint region. Must be "North America", "Europe", or "Far East".

    reportDocumentId (str): Report Document Id returned from GetReportStatus

Sample event object:

{
    "region": "North America",
    "reportDocumentId": "amzn1.spdoc.1.4.na...."
}

OUTPUT:
A successful invocation will respond with an object in the following structure:

{
    "region": "<region>",
    "reportDocumentId": "<report_document_id>",
    "responseReceivedTime": "<response_received_time>",
    "responseStatus": "<response_status>",
    "statusCode": <status_code>,
    "requestURL": "<request_url>",
    "compressionAlgorithm": "<compression_algorithm>",
    "url": "<presigned_url>"
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
    metrics.Metrics(METRICS_NAMESPACE, STACK_NAME, logger).put_metrics_count_value_1(metric_name="SellersPartnerGetReportDocument")
    logger.info(f"Event: {event}")
    
    region = event['region']
    report_document_id = event['reportDocumentId']
    sp_api = selling_partner_api_interface.SellingPartnerAPI(
        region=region
        , auth_id=None
    )

    response = sp_api.get_report_document(
        report_document_id=report_document_id
    )

    event.update(response.response)

    return event

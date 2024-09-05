# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for source/infrastructure/reporting_microservices/selling_partner_reporting/reporting_service/lambda_layers/selling_partner_reporting_layer/python/selling_partner_api_interface/selling_partner_api_interface.py
#
# USAGE:
#   ./run-unit-tests.sh --test-file-name reporting_microservices_tests/selling_partner_reporting_tests/lambda_layers/test_selling_partner_api_interface.py
###############################################################################
import json
import sys

import pytest
from unittest.mock import patch, MagicMock

from moto import mock_aws

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
from microservice_shared.api import ApiHelper, RequestParams
from microservice_shared.utilities import JsonUtil

sys.path.insert(0, "./infrastructure/reporting_microservices/selling_partner_reporting/reporting_service/lambda_layers/selling_partner_reporting_layer/python")
from selling_partner_api_interface.selling_partner_api_interface import SellingPartnerReportingUrlBuilder, SellingPartnerAPI, SellingPartnerReportingAPIResponse


def test_SellingPartnerReportingUrlBuilder_get_base_url():
    url = SellingPartnerReportingUrlBuilder(region='North America').base_url
    expected_url = 'https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/'
    assert url == expected_url
    url = SellingPartnerReportingUrlBuilder(region='Europe').base_url
    expected_url = 'https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/'
    assert url == expected_url
    url = SellingPartnerReportingUrlBuilder(region='Far East').base_url
    expected_url = 'https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/'
    assert url == expected_url

def test_SellingPartnerReportingUrlBuilder_get_report_status_url():
    report_id = '123456789'
    url = SellingPartnerReportingUrlBuilder(region='North America').get_report_status_url(report_id=report_id)
    expected_url = 'https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/123456789'
    assert url == expected_url

def test_SellingPartnerReportingUrlBuilder_get_report_document_url():
    report_document_id = 'amzn1.spdoc.1.4.na.123456789'
    url = SellingPartnerReportingUrlBuilder(region='North America').get_report_document_url(report_document_id=report_document_id)
    expected_url = 'https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/amzn1.spdoc.1.4.na.123456789'
    assert url == expected_url


@pytest.fixture
def auth_parameters():
    return {
        'access_token': 'test_access_token'
    }


def test_SellingPartnerAPI_create_report(auth_parameters):
    with (
        patch(
            "selling_partner_api_interface.selling_partner_api_interface.SellingPartnerAPI.process_request")
        as process_request_mock,
        patch(
            "selling_partner_api_interface.selling_partner_api_interface.SellingPartnerAPI.get_auth_parameters")
        as get_auth_parameters_mock
    ):

        # prepare the request_body arg to resemble the input expected by SP-API
        # call create_report and assert that it returned the response from process_request
        region='North America'
        reportId=12345678
        selling_partner_api = SellingPartnerAPI(region=region)

        # mock get_auth_parameters so that it returns an access key
        get_auth_parameters_mock.return_value = auth_parameters

        # mock process_request so that it returns the SP-API response structure

        response_mock = MagicMock(
            status=200,
            data=json.dumps(
                {
                    "region": region,
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
                    "responseStatus": "202",
                    "statusCode": 202,
                    "requestURL": "/reports/2021-06-30/reports",
                    "reportId": reportId
                }
            ).encode("utf-8")
        )
        process_request_mock.return_value = SellingPartnerReportingAPIResponse(response_mock)
        request_body={
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

        response = selling_partner_api.create_report(
            request_body=request_body
        )

        assert response.status_code == 200

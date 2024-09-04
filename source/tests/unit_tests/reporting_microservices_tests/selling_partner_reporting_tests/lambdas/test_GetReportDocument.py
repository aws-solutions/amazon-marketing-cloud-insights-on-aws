# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Selling Partner Reporting/Lambdas/GetReportDocument handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name reporting_microservices_tests/selling_partner_reporting_tests/lambdas/test_GetReportDocument.py
###############################################################################

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")

sys.path.insert(0, "./infrastructure/reporting_microservices/selling_partner_reporting/reporting_service/lambda_layers/selling_partner_reporting_layer/python/")
from selling_partner_api_interface.selling_partner_api_interface import SellingPartnerAPI, SellingPartnerReportingAPIResponse


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['STACK_NAME'] = "STACK_NAME"
    os.environ['METRICS_NAMESPACE'] = "METRICS_NAMESPACE"


@patch('selling_partner_api_interface.selling_partner_api_interface.SellingPartnerAPI')
@patch('selling_partner_api_interface.selling_partner_api_interface.SellingPartnerReportingAPIResponse')
def test_handler_success(mock_selling_partner_api_response, mock_selling_partner_api):
    from reporting_microservices.selling_partner_reporting.reporting_service.lambdas.GetReportDocument.handler import handler

    mock_event = {
        'region': "North America",
        'reportDocumentId': 123456
    }
    mock_context = MagicMock()

    mock_mock_selling_partner_api_instance = mock_selling_partner_api.return_value
    mock_mock_selling_partner_api_instance.get_report_document.return_value = MagicMock(
        response={'url': "test-url"})

    result = handler(mock_event, mock_context)

    assert result['url'] == "test-url"
    mock_selling_partner_api.assert_called_once_with(region="North America", auth_id=None)
    mock_mock_selling_partner_api_instance.get_report_document.assert_called_once_with(report_document_id=123456)


@patch('selling_partner_api_interface.selling_partner_api_interface.SellingPartnerAPI')
def test_handler_missing_event_details(mock_selling_partner_api):
    from reporting_microservices.selling_partner_reporting.reporting_service.lambdas.GetReportDocument.handler import handler

    mock_event = {}
    mock_context = MagicMock()

    with pytest.raises(KeyError):
        handler(mock_event, mock_context)

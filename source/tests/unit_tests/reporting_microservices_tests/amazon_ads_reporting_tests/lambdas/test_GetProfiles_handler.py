# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Amazon Ads Reporting/Lambdas/CheckReportStatus handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name reporting_microservices_tests/amazon_ads_reporting_tests/lambdas/test_GetProfiles_handler.py
###############################################################################

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")

sys.path.insert(0, "./infrastructure/reporting_microservices/amazon_ads_reporting/reporting_service/lambda_layers/amazon_ads_reporting_layer/python/")
from amazon_ads_api_interface.amazon_ads_api_interface import AmazonAdsAPIs, AmazonAdsReportingAPIResponse


@pytest.fixture(autouse=True)
def _mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['STACK_NAME'] = "STACK_NAME"
    os.environ['METRICS_NAMESPACE'] = "METRICS_NAMESPACE"


@patch('amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsAPIs')
@patch('amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsReportingAPIResponse')
def test_handler_success(mock_amazon_ads_api_response, mock_amazon_ads_api):
    from reporting_microservices.amazon_ads_reporting.reporting_service.lambdas.GetProfiles.handler import handler

    mock_event = {
        'region': "North America"
    }
    mock_context = MagicMock()

    mock_amazon_ads_api_instance = mock_amazon_ads_api.return_value
    
    check_return = [
        {
            'ProfileId': 123456,
            'countryCode': "US",
            'currencyCode': "USD",
            'accountInfo': {
                "marketplaceStringId": "123456789"
            }
        }]
    
    mock_amazon_ads_api_instance.get_profiles_by_region.return_value = check_return
    

    result = handler(mock_event, mock_context)

    assert result == { "North America":[
                        {
                            'ProfileId': 123456,
                            'countryCode': "US",
                            'currencyCode': "USD",
                            'accountInfo': {
                                "marketplaceStringId": "123456789"
                            }
                        }]
                    }
    mock_amazon_ads_api.assert_called_once_with(region="North America", auth_id=None)
    mock_amazon_ads_api_instance.get_profiles_by_region.assert_called_once()


@patch('amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsAPIs')
def test_handler_missing_event_details(mock_amazon_ads_api):
    from reporting_microservices.amazon_ads_reporting.reporting_service.lambdas.GetProfiles.handler import handler

    mock_event = {}
    mock_context = MagicMock()

    with pytest.raises(ValueError):
        handler(mock_event, mock_context)

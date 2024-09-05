# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Amazon Ads Reporting/Lambdas Layers/amazon_ads_api_interface.py
# USAGE:
#   ./run-unit-tests.sh --test-file-name reporting_microservices_tests/amazon_ads_reporting_tests/lambda_layers/test_amazon_ads_api_interface.py
###############################################################################
import json
import sys

import pytest
from unittest.mock import patch, MagicMock

from moto import mock_aws

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
from microservice_shared.api import ApiHelper, RequestParams
from microservice_shared.utilities import JsonUtil

sys.path.insert(0, "./infrastructure/reporting_microservices/amazon_ads_reporting/reporting_service/lambda_layers/amazon_ads_reporting_layer/python")
from amazon_ads_api_interface.amazon_ads_api_interface import AmazonAdsReportingUrlBuilder, AmazonAdsAPIs, AmazonAdsReportingAPIResponse


def test_AmazonAdsReportingUrlBuilder_get_report_status_url():
    report_id = '123456789'

    url = AmazonAdsReportingUrlBuilder(region='North America').get_report_status_url(report_id=report_id)

    expected_url = 'https://advertising-api.amazon.com/reporting/reports/123456789'
    assert url == expected_url


def test_AmazonAdsReportingUrlBuilder_get_sponsored_ads_v3_reporting_url():
    url = AmazonAdsReportingUrlBuilder(region='Europe').get_sponsored_ads_v3_reporting_url()
    
    expected_url = 'https://advertising-api-eu.amazon.com/reporting/reports'
    assert url == expected_url
    
def test_AmazonAdsReportingUrlBuilder_get_base_url():
    # expect failure with invalid region
    with pytest.raises(ValueError):
        AmazonAdsReportingUrlBuilder(region='')


@pytest.fixture
def ads_parameters():
    return {
        'client_id': 'test_client_id', 
        'access_token': 'test_access_token'
    }


def test_AmazonAdsAPIs_request_sponsored_ads_v3_reporting(ads_parameters):
    with (
        patch(
            "amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsAPIs.process_request")
        as process_request_mock,
        patch(
            "amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsAPIs.get_ads_parameters")
        as get_ads_parameters_mock
    ):
        amazon_ads_apis = AmazonAdsAPIs(region='North America')

        get_ads_parameters_mock.return_value = ads_parameters

        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"
        process_request_mock.return_value = AmazonAdsReportingAPIResponse(response_mock)

        amc_response = amazon_ads_apis.request_sponsored_ads_v3_reporting(
            version_3_reporting_data={"test1": "testdata"},
            profile_id="123456789"
        )
        
        expected_request_params = RequestParams(
            request_url=AmazonAdsReportingUrlBuilder('North America').get_sponsored_ads_v3_reporting_url(),
            http_method="POST",
            payload=json.dumps({"test1": "testdata"}, default=JsonUtil().json_encoder_default),
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)
        assert amc_response.status_code == 200
        assert amc_response.response_status == "open"


def test_AmazonAdsAPIs_report_status(ads_parameters):
    with (
        patch(
            "amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsAPIs.process_request")
        as process_request_mock,
        patch(
            "amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsAPIs.get_ads_parameters")
        as get_ads_parameters_mock
    ):
        amazon_ads_apis = AmazonAdsAPIs(region='North America')

        get_ads_parameters_mock.return_value = ads_parameters

        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"
        process_request_mock.return_value = AmazonAdsReportingAPIResponse(response_mock)

        amc_response = amazon_ads_apis.report_status(
            report_id="12345",
            profile_id="123456789"
        )
        
        expected_request_params = RequestParams(
            request_url=AmazonAdsReportingUrlBuilder('North America').get_report_status_url('12345'),
            http_method="GET",
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)
        assert amc_response.status_code == 200
        assert amc_response.response_status == "open"


def test_AmazonAdsAPIs_get_profiles_by_region(ads_parameters):
    with (
        patch(
            "amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsAPIs.process_request")
        as process_request_mock,
        patch(
            "amazon_ads_api_interface.amazon_ads_api_interface.AmazonAdsAPIs.get_ads_parameters")
        as get_ads_parameters_mock
    ):
        amazon_ads_apis = AmazonAdsAPIs(region='North America')

        get_ads_parameters_mock.return_value = ads_parameters

        process_request_mock.return_value = [{
            'ProfileId': 123456,
            'countryCode': "US",
            'currencyCode': "USD",
            'accountInfo': {
                "marketplaceStringId": "123456789"
            }
        }]

        amazon_ads_apis.get_profiles_by_region()
        
        expected_request_params = RequestParams(
            request_url=AmazonAdsReportingUrlBuilder('North America').get_profiles_url(),
            http_method="GET",
        )

        process_request_mock.assert_called_with(ads_request=expected_request_params, kwargs=ads_parameters, return_raw=True)
        

@mock_aws
def test_AmazonAdsReportingAPIResponse():
    utils_mock = MagicMock()
    utils_mock.is_json.return_value = True
    response_mock = MagicMock(status=200, data=json.dumps({"status": "open"}).encode("utf-8"))
    response_mock.geturl.return_value = "https://test-url.com"

    amc_class = AmazonAdsReportingAPIResponse(response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.success == True

    response_mock = MagicMock(status=202, data=json.dumps({"status": "open"}).encode("utf-8"))
    amc_class = AmazonAdsReportingAPIResponse(response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.success == True

    response_mock = MagicMock(status=400, data=json.dumps({"status": "open"}).encode("utf-8"))
    amc_class = AmazonAdsReportingAPIResponse(response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.response['responseStatus'] == "FAILED"
    assert amc_class.success == False

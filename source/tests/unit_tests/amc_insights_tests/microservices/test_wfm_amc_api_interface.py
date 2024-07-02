# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for WorkflowManagementService/AMCAPIResponse & Interface.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_wfm_amc_api_interface.py
###############################################################################


import os
import sys
import json
from unittest.mock import MagicMock, patch, Mock

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0,
                "./infrastructure/amc_insights/microservices/workflow_manager_service/lambda_layers/wfm_layer/python/")
from amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface import \
    AMCAPIResponse, AMCAPIs, AMCRequests, safe_json_loads, send_request, get_access_token, verify_amc_request
from wfm_utilities import wfm_utilities


@mock_aws
def test_AMCAPIResponse():
    utils_mock = MagicMock()
    utils_mock.is_json.return_value = True
    response_mock = MagicMock(status=200, data=json.dumps({"status": "open"}).encode("utf-8"))
    response_mock.geturl.return_value = "https://test-url.com"

    amc_class = AMCAPIResponse(response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.success == True

    response_mock = MagicMock(status=202, data=json.dumps({"status": "open"}).encode("utf-8"))
    amc_class = AMCAPIResponse(response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.success == True

    response_mock = MagicMock(status=400, data=json.dumps({"status": "open"}).encode("utf-8"))
    amc_class = AMCAPIResponse(response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.response['responseStatus'] == "FAILED"
    assert amc_class.success == False

    status = "FAILED"
    amc_class.update_response_status(status)
    assert amc_class.response["responseStatus"] == status

    amc_class.update_response({"statusCode": "500"})
    assert amc_class.response["statusCode"] == "500"


@pytest.fixture
def amc_apis():
    customer_config = {
        "amcAmazonAdsMarketplaceId": "test_marketplace_id",
        "amcAmazonAdsAdvertiserId": "test_advertiser_id",
        "amcInstanceId": "test_amc_instance_id",
        "customerId": "test_customer"
    }
    logger = MagicMock()
    utils_obj = wfm_utilities.Utils(logger)
    amc_apis = AMCAPIs(customer_config=customer_config, wfm_utils=utils_obj)
    return amc_apis


@pytest.fixture
def ads_parameters():
    return {'client_id': 'test_client_id', 'access_token': 'test_access_token',
            'instance_id': 'test_amc_instance_id',
            'marketplace_id': 'test_marketplace_id', 'advertiser_id': 'test_advertiser_id'}


@mock_aws
def test_AMCAPIs_get_execution_status_by_workflow_execution_id(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request")
        as process_request_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock
    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        response_mock = MagicMock(status=200, data=json.dumps({"status": "open"}).encode("utf-8"))
        response_mock.geturl.return_value = "https://test-url.com"
        get_ads_parameters_mock.return_value = ads_parameters

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        workflow_execution_id = "123456"
        amc_response = amc_apis.get_execution_status_by_workflow_execution_id(workflow_execution_id)

        process_request_mock.assert_called_with(ads_parameters)
        #
        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"


def test_AMCAPIs_get_execution_status_by_minimum_create_time(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request")
        as process_request_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock
    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        get_ads_parameters_mock.return_value = ads_parameters
        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open", "executions": [{"workflowId": "111111"}]}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        amc_response = amc_apis.get_execution_status_by_minimum_create_time()

        process_request_mock.assert_called_with(ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response["executions"] == [{"workflowId": "111111"}]


def test_AMCAPIs_create_workflow(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request")
        as process_request_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock
    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        get_ads_parameters_mock.return_value = ads_parameters
        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        amc_response = amc_apis.create_workflow(workflow_definition={"workflowId": 12345},
                                                update_if_already_exists=False)

        process_request_mock.assert_called_with(ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "CREATED"


def test_AMCAPIs_update_workflow(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request")
        as process_request_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock
    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        get_ads_parameters_mock.return_value = ads_parameters
        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        amc_response = amc_apis.update_workflow(workflow_definition={"workflowId": 12345})

        process_request_mock.assert_called_with(ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "UPDATED"


def test_AMCAPIs_delete_workflow(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request")
        as process_request_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock
    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        get_ads_parameters_mock.return_value = ads_parameters
        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        amc_response = amc_apis.delete_workflow(workflow_id="12345")

        process_request_mock.assert_called_with(ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "DELETED"


def test_AMCAPIs_get_workflow(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request") as process_request_mock

    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        get_ads_parameters_mock.return_value = ads_parameters
        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        amc_response = amc_apis.get_workflow(workflow_id="12345")

        process_request_mock.assert_called_with(ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "RECEIVED"


def test_AMCAPIs_get_workflows(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request") as process_request_mock

    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        get_ads_parameters_mock.return_value = ads_parameters
        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        amc_response = amc_apis.get_workflows()

        process_request_mock.assert_called_with(ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "RECEIVED"


def test_AMCAPIs_create_workflow_execution(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request") as process_request_mock

    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        get_ads_parameters_mock.return_value = ads_parameters
        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        create_execution_request = {
            "parameterValues": {
                "test_time": "TODAY()"
            },
            "timeWindowStart": "TODAY()",
            "timeWindowEnd": "TODAY()"
        }
        amc_response = amc_apis.create_workflow_execution(create_execution_request=create_execution_request)

        process_request_mock.assert_called_with(ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"


def test_AMCAPIs_cancel_workflow_execution(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCRequests.process_request") as process_request_mock

    ):
        utils_mock = MagicMock()
        utils_mock.is_json.return_value = True
        get_ads_parameters_mock.return_value = ads_parameters
        response_mock = MagicMock(
            status=200,
            data=json.dumps({"status": "CANCELLED"}).encode(
                "utf-8")
        )
        response_mock.geturl.return_value = "https://test-url.com"

        process_request_mock.return_value = AMCAPIResponse(response_mock)

        amc_response = amc_apis.cancel_workflow_execution(workflow_execution_id="12345")

        process_request_mock.assert_called_with(ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_status == "CANCELLED"


def test_safe_json():
    assert safe_json_loads("test") == "test"
    assert safe_json_loads(json.dumps({"test": "123"})) == {"test": "123"}


def test_send_request():
    with (
        patch(
            "urllib3.PoolManager.request")
        as request_mock
    ):
        request_mock.return_value = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode(
                "utf-8")
        )

        send_request(request_url="https://test-url.com", headers={"header": "test_header_1"}, http_method="GET",
                     data="test_data", query_params={"query_parameter": "test_query_parameter"})

        request_mock.assert_called_with(url="https://test-url.com", headers={"header": "test_header_1"}, method="GET",
                                        body="test_data", fields={"query_parameter": "test_query_parameter"})

        send_request(request_url="https://test-url.com", headers={"header": "test_header_1"}, http_method="POST",
                     data="test_data", query_params={"query_parameter_1": "test_query_parameter_1",
                                                     "query_parameter_2": "test_query_parameter_2"})

        request_mock.assert_called_with(
            url="https://test-url.com?query_parameter_1=test_query_parameter_1&query_parameter_2=test_query_parameter_2",
            headers={"header": "test_header_1"}, method="POST",
            body="test_data")


@mock_aws
def test_get_access_token():
    secret_key = os.environ["AMC_SECRETS_MANAGER"]
    secret_values = {
        'client_id': 'test_client_id',
        'access_token': 'test_access_token_old',
        'client_secret': 'test_client_secret',
        'refresh_token': 'test_refresh_token',
        'authorization_code': 'test_authorization_code',
    }
    client = boto3.client("secretsmanager")
    client.create_secret(
        Name=secret_key,
        SecretString=json.dumps(secret_values)
    )

    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.send_request")
        as send_request_mock
    ):
        send_request_mock.return_value = MagicMock(
            status=200,
            data=json.dumps({"status": "open", "access_token": "test_access_token_new"}).encode("utf-8")
        )
        res = get_access_token()

        actual_secrets_res = client.get_secret_value(
            SecretId=secret_key,
        )
        actual_secrets_values = json.loads(actual_secrets_res["SecretString"])
        assert actual_secrets_values["access_token"] == "test_access_token_new"
        assert res == {'client_id': 'test_client_id', 'status': 'open', 'access_token': 'test_access_token_new'}

        with pytest.raises(RuntimeError):
            send_request_mock.return_value = MagicMock(
                status=401,
                data=json.dumps({"status": "unauthorized"}).encode("utf-8")
            )
            get_access_token()


def test_AMCRequests_process_request_using_valid_access_token(ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.send_request")
        as send_request_mock
    ):
        send_request_mock.return_value = MagicMock(
            status=200,
            data=json.dumps({"status": "open"}).encode("utf-8")
        )

        amc_request = AMCRequests(
            amc_path="/an_amc_request",
            http_method="GET",
            request_parameters={"query_param_a_key": "query_param_a_value"},
            payload="some data"
        )
        actual_amc_response = amc_request.process_request(ads_parameters)

        send_request_mock.assert_called_with(
            request_url="https://advertising-api.amazon.com/amc/reporting/test_amc_instance_id/an_amc_request",
            headers={'Amazon-Advertising-API-ClientId': 'test_client_id', 'Authorization': 'Bearer test_access_token',
                     'Content-Type': 'application/json', 'Amazon-Advertising-API-AdvertiserId': 'test_advertiser_id',
                     'Amazon-Advertising-API-MarketplaceId': 'test_marketplace_id',  
                     "x-amzn-service-name": "amazon-marketing-cloud-insights-on-aws",
                     "x-amzn-service-version": "v99.99.99"},
            http_method="GET",
            data="some data",
            query_params={'query_param_a_key': 'query_param_a_value'},
        )
        assert actual_amc_response.status_code == 200


def test_AMCRequests_process_request_using_bad_access_token(ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.send_request")
        as send_request_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.verify_amc_request")
        as verify_amc_request_mock,
    ):
        send_request_mock.return_value = MagicMock(
            status=401,
            data=json.dumps({"status": "unauthorized"}).encode("utf-8")
        )
        verify_amc_request_mock.return_value = {"access_token": "an_new_access_token"}

        amc_request = AMCRequests(
            amc_path="/an_amc_request",
            http_method="GET",
            request_parameters={"query_param_a_key": "query_param_a_value"},
            payload="some data"
        )
        amc_request.process_request(ads_parameters)

        send_request_mock.assert_called_with(
            request_url="https://advertising-api.amazon.com/amc/reporting/test_amc_instance_id/an_amc_request",
            headers={'Amazon-Advertising-API-ClientId': 'test_client_id', 'Authorization': 'Bearer an_new_access_token',
                     'Content-Type': 'application/json', 'Amazon-Advertising-API-AdvertiserId': 'test_advertiser_id',
                     'Amazon-Advertising-API-MarketplaceId': 'test_marketplace_id',
                     "x-amzn-service-name": "amazon-marketing-cloud-insights-on-aws",
                     "x-amzn-service-version": "v99.99.99"
                     },
            http_method="GET",
            data="some data",
            query_params={'query_param_a_key': 'query_param_a_value'},
        )

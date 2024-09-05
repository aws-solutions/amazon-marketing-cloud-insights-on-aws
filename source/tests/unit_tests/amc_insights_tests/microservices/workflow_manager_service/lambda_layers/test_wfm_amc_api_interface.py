# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for WorkflowManagementService/AMCAPIResponse & Interface.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/workflow_management_service/lambda_layers/test_wfm_amc_api_interface.py
###############################################################################

import sys
import json
from unittest.mock import MagicMock, patch
import pytest
from moto import mock_aws

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/amc_insights/microservices/workflow_manager_service/lambda_layers/wfm_layer/python/")
from amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface import \
    AMCAPIResponse, AMCAPIs
from wfm_utilities import wfm_utilities
from microservice_shared.api import ApiHelper, RequestParams
from microservice_shared.utilities import JsonUtil


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
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
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
        
        expected_request_params = RequestParams(
            http_method="GET",
            request_path=f"/workflowExecutions/{workflow_execution_id}",
            request_parameters = {
                "includeWorkflow": False
            }
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)
        #
        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"


def test_AMCAPIs_get_execution_status_by_minimum_create_time(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
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

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response["executions"] == [{"workflowId": "111111"}]


def test_AMCAPIs_create_workflow(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
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
        
        expected_request_params = RequestParams(
            request_path="/workflows",
            http_method="POST",
            payload=json.dumps(
                {"workflowId": 12345}, default=JsonUtil().json_encoder_default)
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "CREATED"


def test_AMCAPIs_update_workflow(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
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
        
        expected_request_params = RequestParams(
            request_path="/workflows/12345",
            http_method="PUT",
            payload=json.dumps(
                {"workflowId": 12345}, default=JsonUtil().json_encoder_default)
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "UPDATED"


def test_AMCAPIs_delete_workflow(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
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
        
        expected_request_params = RequestParams(
            request_path=f"/workflows/12345",
            http_method="DELETE",
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "DELETED"


def test_AMCAPIs_get_workflow(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
        as process_request_mock,
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
        
        expected_request_params = RequestParams(
            request_path=f"/workflows/12345",
            http_method="GET",
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "RECEIVED"


def test_AMCAPIs_get_workflows(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
        as process_request_mock,
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
        
        expected_request_params = RequestParams(
            request_path=f"/workflows/12345",
            http_method="GET",
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"
        assert amc_response.response_status == "RECEIVED"


def test_AMCAPIs_create_workflow_execution(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
        as process_request_mock,
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
        
        expected_request_params = RequestParams(
            request_path="/workflowExecutions",
            http_method="POST",
            payload=json.dumps(create_execution_request),
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_message == "open"


def test_AMCAPIs_cancel_workflow_execution(amc_apis, ads_parameters):
    with (
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.get_ads_parameters")
        as get_ads_parameters_mock,
        patch(
            "amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIs.process_request")
        as process_request_mock,
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
        
        expected_request_params = RequestParams(
            request_path=f"/workflowExecutions/12345",
            http_method="PUT",
            payload=json.dumps({"status": "CANCELLED"}),
        )

        process_request_mock.assert_called_with(expected_request_params, ads_parameters)

        assert amc_response.status_code == 200
        assert amc_response.response_status == "CANCELLED"

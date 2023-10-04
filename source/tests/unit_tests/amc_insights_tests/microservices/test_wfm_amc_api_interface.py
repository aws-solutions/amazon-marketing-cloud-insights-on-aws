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
import logging
from unittest.mock import MagicMock, patch, call
from moto import mock_dynamodb, mock_sts, mock_apigateway, mock_iam


sys.path.insert(0, "./infrastructure/amc_insights/microservices/workflow_manager_service/lambda_layers/wfm_layer/python/")
from amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface import AMCAPIResponse, AMCAPIInterface
from wfm_utilities import wfm_utilities


@mock_dynamodb
def test_AMCAPIResponse():
    utils_mock = MagicMock()
    utils_mock.is_json.return_value = True
    logger_mock = MagicMock()
    response_mock = MagicMock(status=200, data=json.dumps({"status": "open"}).encode("utf-8"))
    response_mock.geturl.return_value = "https://test-url.com"

    amc_class = AMCAPIResponse(utils=utils_mock, logger=logger_mock, response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.success == True

    response_mock = MagicMock(status=202, data=json.dumps({"status": "open"}).encode("utf-8"))
    amc_class = AMCAPIResponse(utils=utils_mock, logger=logger_mock, response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.success == True

    response_mock = MagicMock(status=400, data=json.dumps({"status": "open"}).encode("utf-8"))
    amc_class = AMCAPIResponse(utils=utils_mock, logger=logger_mock, response=response_mock)

    assert amc_class.response_text == response_mock.data.decode('utf-8')
    assert amc_class.response['responseStatus'] == "FAILED"
    assert amc_class.success == False

    status = "FAILED"
    amc_class.update_response_status(status)
    assert amc_class.response["responseStatus"] == status

    amc_class.update_response({"statusCode": "500"})
    assert amc_class.response["statusCode"] == "500"



@mock_dynamodb
@mock_sts
@mock_iam
@mock_apigateway
def test_AMCAPIInterface():
    logger = MagicMock()
    test_arn = "arn:aws:iam::123456789012:role/Test/test_12345/test_role"

    config = {
        "invokeAmcApiRoleArn": test_arn,
        "amcApiEndpoint": "https://test-url.com",
        "amcRegion": os.environ["AWS_DEFAULT_REGION"]
    }
    utils_obj = wfm_utilities.Utils(logger)
    amc_interface = AMCAPIInterface(config=config, logger=logger, utils=utils_obj)
    assert amc_interface.logger == logger
    assert amc_interface.utils == utils_obj
    assert amc_interface.config == config

    should_fail = amc_interface.boto3_get_session_for_role(customer_role_arn="bad_arn")
    assert should_fail == False

    should_pass = amc_interface.boto3_get_session_for_role(customer_role_arn=test_arn)
    assert should_pass is None

    apigateway = amc_interface.apigateway_get_signed_headers(
        request_method="GET", 
        request_endpoint_url="https://test-url.com", 
        request_body="{}",
        region="us-east-1"
    )
    

    with (
          patch("amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIResponse") as amc_response_mock,
          patch("amc_insights.microservices.workflow_manager_service.lambda_layers.wfm_layer.python.wfm_amc_api_interface.wfm_amc_api_interface.AMCAPIInterface.apigateway_get_signed_headers") as amc_api_interface_headers_mock
    ):
        patch_urlib3_poolmanager = "urllib3.PoolManager"
        amc_api_interface_headers_mock.return_value = apigateway
        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            amc_interface.send_amc_api_request(
                request_method="POST", 
                request_body="{}",
                url="https://test-url.com"
            )
            mock_pool_manager.assert_called()
            amc_response_mock.assert_called()
            amc_api_interface_headers_mock.assert_called()
            mock_amc_request.request.assert_called_once_with('POST', 'https://test-url.com', body='{}', headers=apigateway)

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            workflow_id = "123456"
            amc_interface.get_execution_status_by_workflow_id(
                workflow_id=workflow_id
            )
            mock_amc_request.request.assert_called_once_with('GET', f"{config['amcApiEndpoint']}/?workflowId={workflow_id}/", body="", headers=apigateway)

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            workflow_execution_id = "67890"

            amc_interface.get_execution_status_by_workflow_execution_id(
                workflow_execution_id=workflow_execution_id
            )
            mock_pool_manager.assert_called()
            mock_amc_request.request.assert_called_once_with('GET', f"{config['amcApiEndpoint']}/workflowExecutions/{workflow_execution_id}", body="", headers=apigateway)

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            workflow_definition = {
                    "workflowId": 12345
            }
            
            amc_interface.create_workflow(
                workflow_definition=workflow_definition,
                update_if_already_exists=False
            )
            mock_pool_manager.assert_called()
            mock_amc_request.request.assert_called_once_with('POST', f"{config['amcApiEndpoint']}/workflows", body='{"workflowId": 12345}', headers=apigateway)

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            amc_interface.create_workflow(
                workflow_definition=workflow_definition,
                update_if_already_exists=True
            )
            mock_pool_manager.assert_called()
            mock_amc_request.request.assert_called_once_with('POST', f"{config['amcApiEndpoint']}/workflows", body='{"workflowId": 12345}', headers=apigateway)
        
        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            workflow_definition = {
                "workflowId": 12345
            }
            amc_interface.update_workflow(
                workflow_definition=workflow_definition
            )
            mock_pool_manager.assert_called()
            mock_amc_request.request.assert_called_once_with('PUT', f"{config['amcApiEndpoint']}/workflows/{workflow_definition['workflowId']}", body='{"workflowId": 12345}', headers=apigateway)

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            workflow_id = "123456"
            
            amc_interface.delete_workflow(
                workflow_id=workflow_id
            )
            mock_pool_manager.assert_called()
            mock_amc_request.request.assert_called_with('DELETE', f"{config['amcApiEndpoint']}/workflows/{workflow_id}", body="", headers=apigateway)
            

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            workflow_id = "123456"
            
            amc_interface.get_workflow(
                workflow_id=workflow_id
            )
            mock_pool_manager.assert_called()
            mock_amc_request.request.assert_called_once_with('GET', f"{config['amcApiEndpoint']}/workflows/{workflow_id}", body="", headers=apigateway)

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            amc_interface.get_workflows()
            mock_pool_manager.assert_called()
            mock_amc_request.request.assert_called_once_with('GET', f"{config['amcApiEndpoint']}/workflows", body="", headers=apigateway)

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            create_execution_request = {
                "parameterValues": {
                    "test_time": "TODAY()"
                },
                "timeWindowStart": "TODAY()",
                "timeWindowEnd": "TODAY()"
            }
            
            test_time = wfm_utilities.Utils(logger).get_current_date_with_offset(0)
            amc_interface.create_workflow_execution(create_execution_request)
            mock_pool_manager.assert_called()
            test_body = {"parameterValues": {"test_time": test_time},"timeWindowStart": test_time, "timeWindowEnd": test_time}
            mock_amc_request.request.assert_called_once_with('POST', f"{config['amcApiEndpoint']}/workflowExecutions", body=json.dumps(test_body), headers=apigateway)

            create_execution_request = {
                "parameterValues": {
                    "test_time": "NOW()"
                },
                "timeWindowStart": "NOW()",
                "timeWindowEnd": "NOW()"
            }
            amc_interface.create_workflow_execution(create_execution_request)
            mock_amc_request.request.assert_called()

        with patch(patch_urlib3_poolmanager) as mock_pool_manager:
            mock_amc_request = mock_pool_manager.return_value
            workflow_execution_id = "89043"
            
            amc_interface.cancel_workflow_execution(workflow_execution_id)
            mock_pool_manager.assert_called()
            mock_amc_request.request.assert_has_calls(
                [
                    call('GET', f"{config['amcApiEndpoint']}/workflowExecutions/{workflow_execution_id}", headers=apigateway, body=""),
                    call('DELETE', f"{config['amcApiEndpoint']}/workflowExecutions/{workflow_execution_id}", headers=apigateway, body=""),
                    call('GET', f"{config['amcApiEndpoint']}/workflowExecutions/{workflow_execution_id}", headers=apigateway, body="")
                ]
            )
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for InvokeWorkflowExecutionSM/handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_invoke_workflow_execution_sm_handler.py


import os
from unittest.mock import Mock, MagicMock

import boto3
import pytest
import json
import sys
import contextlib
from datetime import datetime
from moto import mock_dynamodb, mock_stepfunctions, mock_iam

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0,
                "./infrastructure/amc_insights/microservices/workflow_manager_service/lambda_layers/wfm_layer/python/")


def _global_configs():
    return {
        "customer_id": "12345",
        "dataset_id": "67890",
        "wf_config_datetime_format": "%Y-%m-%dT%H:%M:%S.%fZ",
        "wf_config_datetime_format2": "%Y-%m-%dT%H:%M:%SZ",
    }


@pytest.fixture
def global_configs():
    return _global_configs()


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ[
        'STEP_FUNCTION_STATE_MACHINE_ARN'] = f'arn:aws:states:us-east-1:{os.environ["MOTO_ACCOUNT_ID"]}:stateMachine:test_role_step_function'
    os.environ['DATASET_WORKFLOW_TABLE'] = "test_data_set_table"


@pytest.fixture
def parsed_message(global_configs):
    return {
        "uploadRequest": {
            "requestType": "uploadData",
            "dataSetId": global_configs["dataset_id"]
        }
    }


@pytest.fixture()
def _mock_cloudwatch_client():
    cloudwatch_client = get_service_client('cloudwatch')
    cloudwatch_client.put_metric_data = Mock(
        return_value={}
    )
    return cloudwatch_client


@pytest.fixture()
def _mock_secrets_manager_client():
    secrets_manager_client = get_service_client('secretsmanager')
    secrets_manager_client.get_secret_value = Mock(
        return_value={'SecretString': '123456'}
    )
    return secrets_manager_client


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_cloudwatch_client, _mock_secrets_manager_client):
    monkeypatch.setitem(_helpers_service_clients, 'cloudwatch', _mock_cloudwatch_client)
    monkeypatch.setitem(_helpers_service_clients, 'secretsmanager', _mock_secrets_manager_client)


@contextlib.contextmanager
def create_workflow_table():
    dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
    params = {
        "TableName": os.environ['DATASET_WORKFLOW_TABLE'],
        "KeySchema": [
            {"AttributeName": "customerId", "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "customerId", "AttributeType": "S"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }
    table = dynamodb.create_table(**params)

    item = {
        "customerId": _global_configs()["customer_id"],
        "datasetId": _global_configs()["dataset_id"],
        "workflowId": "test_role_step_function"
    }
    table.put_item(
        TableName=os.environ["DATASET_WORKFLOW_TABLE"], Item=item
    )
    yield


@contextlib.contextmanager
def create_wf_state_machine():
    test_role = "StepFunctionLambdaBasicExecution"
    iam_client = boto3.client('iam')
    iam_client.create_role(RoleName=test_role, AssumeRolePolicyDocument=json.dumps({
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {
                "Service": "states.amazonaws.com"
            }
        }],
        "Version": "2012-10-17"
    }))

    iam_client.attach_role_policy(
        RoleName=test_role,
        PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaRole'
    )

    test_role_arn = iam_client.get_role(RoleName=test_role)

    client = boto3.client('stepfunctions')

    client.create_state_machine(
        name="test_role_step_function",
        definition="test_def",
        roleArn=test_role_arn['Role']['Arn'],
    )
    yield


@pytest.fixture()
def __mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


@mock_dynamodb
def test_dynamodb_get_wf_config(global_configs, __mock_imports):
    with create_workflow_table():
        from amc_insights.microservices.workflow_manager_service.lambdas.InvokeWorkflowExecutionSM.handler import \
            dynamodb_get_wf_config

        wf_config_list = dynamodb_get_wf_config(dynamodb_table_name=os.environ['DATASET_WORKFLOW_TABLE'],
                                                customer_id=global_configs["customer_id"],
                                                dataset_id=global_configs["dataset_id"])

        assert wf_config_list[0]["customerId"] == global_configs["customer_id"]
        assert wf_config_list[0]["datasetId"] == global_configs["dataset_id"]


def test_get_config_details(parsed_message, global_configs):
    from amc_insights.microservices.workflow_manager_service.lambdas.InvokeWorkflowExecutionSM.handler import \
        get_config_details
    test_wf_config = {
        'timeWindowStart': datetime.today().strftime(global_configs["wf_config_datetime_format"]),
        'timeWindowEnd': datetime.now().strftime(global_configs["wf_config_datetime_format"]),
    }

    wf_config_datetime_format = global_configs["wf_config_datetime_format"]
    wf_config_datetime_format2 = global_configs["wf_config_datetime_format2"]
    parsed_message["uploadRequest"]['timeWindowStart'] = datetime.today().strftime(wf_config_datetime_format2)
    parsed_message["uploadRequest"]['timeWindowEnd'] = datetime.now().strftime(wf_config_datetime_format2)

    config_start, config_end, dus_start, dus_end = get_config_details(
        wf_config=test_wf_config,
        wf_config_datetime_format=wf_config_datetime_format,
        parsed_message=parsed_message
    )

    assert config_start == datetime.strptime(
        test_wf_config['timeWindowStart'], wf_config_datetime_format
    )
    assert config_end == datetime.strptime(
        test_wf_config['timeWindowEnd'], wf_config_datetime_format
    )
    assert dus_start == datetime.strptime(
        parsed_message['uploadRequest']['timeWindowStart'],
        wf_config_datetime_format2
    )
    assert dus_end == datetime.strptime(
        parsed_message['uploadRequest']['timeWindowEnd'],
        wf_config_datetime_format2
    )


@mock_stepfunctions
@mock_iam
def test_start_state_machine(global_configs, parsed_message):
    with create_wf_state_machine():
        from amc_insights.microservices.workflow_manager_service.lambdas.InvokeWorkflowExecutionSM.handler import \
            start_state_machine

        test_wf_config = {
            'timeWindowStart': datetime.today().strftime(global_configs["wf_config_datetime_format"]),
            'timeWindowEnd': datetime.now().strftime(global_configs["wf_config_datetime_format"]),
        }

        wf_config_datetime_format = global_configs["wf_config_datetime_format"]
        wf_config_datetime_format2 = global_configs["wf_config_datetime_format2"]
        parsed_message["uploadRequest"]['timeWindowStart'] = datetime.today().strftime(wf_config_datetime_format2)
        parsed_message["uploadRequest"]['timeWindowEnd'] = datetime.now().strftime(wf_config_datetime_format2)

        test_item = {
            **test_wf_config,
            "customerId": global_configs["customer_id"],
            "datasetId": global_configs["dataset_id"],
            "workflowId": "test_role_step_function"
        }
        start_state_machine(wf_config_list=[test_item], wf_config_datetime_format=wf_config_datetime_format,
                            parsed_message=parsed_message, customer_id=global_configs["customer_id"])

        # test exception
        no_date_parsed_message = {
            "uploadRequest": {
                "requestType": "uploadData",
                "dataSetId": global_configs["dataset_id"]
            }
        }

        test_item = {
            **test_wf_config,
            "customerId": global_configs["customer_id"],
            "datasetId": global_configs["dataset_id"],
        }
        start_state_machine(wf_config_list=[test_item], wf_config_datetime_format=wf_config_datetime_format,
                            parsed_message=no_date_parsed_message, customer_id=global_configs["customer_id"])


@mock_dynamodb
@mock_stepfunctions
@mock_iam
def test_handler(global_configs, _mock_clients):
    with create_workflow_table(), create_wf_state_machine():
        from amc_insights.microservices.workflow_manager_service.lambdas.InvokeWorkflowExecutionSM.handler import \
            handler

        test_wf_config = {
            'timeWindowStart': datetime.today().strftime(global_configs["wf_config_datetime_format"]),
            'timeWindowEnd': datetime.now().strftime(global_configs["wf_config_datetime_format"]),
        }
        wf_config_datetime_format2 = global_configs["wf_config_datetime_format2"]

        test_event = {
            "Records": [
                {
                    "Sns": {
                        "Message": json.dumps({
                            "uploadRequest": {
                                "requestType": "uploadData",
                                "dataSetId": global_configs["dataset_id"],
                                "timeWindowStart": datetime.today().strftime(wf_config_datetime_format2),
                                "timeWindowEnd": datetime.today().strftime(wf_config_datetime_format2),
                            },
                            "executionStatus": "Upload Request Sent",
                            "responseStatus": "Succeeded",
                            "customerId": global_configs["customer_id"],
                            **test_wf_config
                        })
                    }
                }
            ]
        }

        handler(event=test_event, _=None)

        test_event = {
            "Message": {
                "uploadRequest": {
                    "requestType": "uploadData",
                    "dataSetId": global_configs["dataset_id"],
                    "timeWindowStart": datetime.today().strftime(wf_config_datetime_format2),
                    "timeWindowEnd": datetime.today().strftime(wf_config_datetime_format2),
                },
                "executionStatus": "Upload Request Sent",
                "responseStatus": "Succeeded",
                "customerId": global_configs["customer_id"],
                **test_wf_config
            },
            "requestType": "notCancelExecution",
            "createExecutionRequest": {
                "workflowId": "test_role_step_function"
            },
        }
        resp = handler(event=test_event, _=None)
        assert "arn:aws:states:us-east-1:111111111111:execution:test_role_step_function" in json.loads(resp)[
            "executionArn"]

        test_event = {
            "Message": {
                "uploadRequest": {
                    "requestType": "uploadData",
                    "dataSetId": global_configs["dataset_id"],
                    "timeWindowStart": datetime.today().strftime(wf_config_datetime_format2),
                    "timeWindowEnd": datetime.today().strftime(wf_config_datetime_format2),
                },
                "executionStatus": "Upload Request Sent",
                "responseStatus": "Succeeded",
                "customerId": global_configs["customer_id"],
                **test_wf_config
            },
            "requestType": "cancelExecution",
            "createExecutionRequest": {
                "workflowId": "test_role_step_function"
            },
        }
        resp = handler(event=test_event, _=None)
        assert "arn:aws:states:us-east-1:111111111111:execution:test_role_step_function" in json.loads(resp)[
            "executionArn"]

        test_event = {
            "Records": [
                {
                    "Sns": {
                        "Message": json.dumps({
                            "uploadRequest": {
                                "requestType": "uploadData",
                                "dataSetId": global_configs["dataset_id"],
                                "timeWindowStart": datetime.today().strftime(wf_config_datetime_format2),
                                "timeWindowEnd": datetime.today().strftime(wf_config_datetime_format2),
                            },
                            "executionStatus": "Upload Request Sent",
                            "responseStatus": "FAILED",
                            "customerId": global_configs["customer_id"],
                            **test_wf_config
                        })
                    }
                }
            ]
        }

        handler(event=test_event, _=None)


@mock_dynamodb
@mock_stepfunctions
@mock_iam
def test_handler_empty_table(global_configs, _mock_clients):
    with create_wf_state_machine():
        from amc_insights.microservices.workflow_manager_service.lambdas.InvokeWorkflowExecutionSM.handler import \
            handler

        dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
        params = {
            "TableName": os.environ['DATASET_WORKFLOW_TABLE'],
            "KeySchema": [
                {"AttributeName": "customerId", "KeyType": "HASH"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "customerId", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        dynamodb.create_table(**params)

        test_wf_config = {
            'timeWindowStart': datetime.today().strftime(global_configs["wf_config_datetime_format"]),
            'timeWindowEnd': datetime.now().strftime(global_configs["wf_config_datetime_format"]),
        }
        wf_config_datetime_format2 = global_configs["wf_config_datetime_format2"]

        test_event = {
            "Records": [
                {
                    "Sns": {
                        "Message": json.dumps({
                            "uploadRequest": {
                                "requestType": "uploadData",
                                "dataSetId": global_configs["dataset_id"],
                                "timeWindowStart": datetime.today().strftime(wf_config_datetime_format2),
                                "timeWindowEnd": datetime.today().strftime(wf_config_datetime_format2),
                            },
                            "executionStatus": "Upload Request Sent",
                            "responseStatus": "Succeeded",
                            "customerId": global_configs["customer_id"],
                            **test_wf_config
                        })
                    }
                }
            ]
        }

        handler(event=test_event, _=None)

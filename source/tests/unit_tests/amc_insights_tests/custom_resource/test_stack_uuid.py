# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for stack_uuid.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/custom_resource/test_stack_uuid.py
###############################################################################


import pytest
import os
import boto3
from unittest.mock import patch
from moto import mock_secretsmanager


@pytest.fixture(autouse=True)
def apply_handler_env():
    os.environ['STACK_NAME'] = "test-stack"


@patch("amc_insights.custom_resource.anonymous_operational_metrics.lambdas.stack_uuid.create_uuid")
def test_on_create(create_uuid_mock):
    from amc_insights.custom_resource.anonymous_operational_metrics.lambdas.stack_uuid import on_create

    on_create({}, None)
    create_uuid_mock.assert_called_once()


@patch("amc_insights.custom_resource.anonymous_operational_metrics.lambdas.stack_uuid.delete_secret")
def test_on_delete(delete_secret_mock):
    from amc_insights.custom_resource.anonymous_operational_metrics.lambdas.stack_uuid import on_delete

    on_delete({"PhysicalResourceId": "1234"}, None)
    delete_secret_mock.assert_called_once()


@patch("crhelper.CfnResource")
@patch("amc_insights.custom_resource.anonymous_operational_metrics.lambdas.stack_uuid.helper")
def test_event_handler(helper_mock, _):
    from amc_insights.custom_resource.anonymous_operational_metrics.lambdas.stack_uuid import event_handler

    event_handler({}, None)
    helper_mock.assert_called_once()


@patch("crhelper.CfnResource")
@mock_secretsmanager
def test_create_uuid(cfn_mock):
    session = boto3.session.Session(region_name=os.environ["AWS_REGION"])
    client = session.client("secretsmanager")
    secret_id = f"{os.environ['STACK_NAME']}-anonymous-metrics-uuid"
    from amc_insights.custom_resource.anonymous_operational_metrics.lambdas.stack_uuid import create_uuid

    create_uuid()
    res = client.get_secret_value(
        SecretId=secret_id,
    )
    res["SecretString"] is not None


@patch("crhelper.CfnResource.delete")
@mock_secretsmanager
def test_delete_secret(cfn_mock):
    session = boto3.session.Session(region_name=os.environ["AWS_REGION"])
    client = session.client("secretsmanager")
    secret_id = f"{os.environ['STACK_NAME']}-anonymous-metrics-uuid"
    from amc_insights.custom_resource.anonymous_operational_metrics.lambdas.stack_uuid import delete_secret

    client.create_secret(
        Name=secret_id,
        SecretString="some-secret"
    )

    delete_secret()

    with pytest.raises(Exception) as ex:
        client.get_secret_value(
            SecretId=secret_id,
        )
    assert "Secrets Manager can't find the specified secret" in str(ex.value)
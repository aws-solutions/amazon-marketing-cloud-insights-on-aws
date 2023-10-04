# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for sync_platform_manager.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/custom_resource/test_trail_data_events.py
###############################################################################

from unittest.mock import Mock, patch
import pytest
import os
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients


@pytest.fixture(autouse=True)
def handler_env():
    os.environ['CLOUD_TRAIL_ARN'] = 'cloud_trail_arn'
    os.environ['ARTIFACTS_BUCKET_ARN'] = 'artifacts_bucket_arn'
    os.environ['LAMBDA_FUNCTION_ARNS_START_WITH'] = 'lambda_arn'
    os.environ['RAW_BUCKET_LOGICAL_ID'] = 'RAW_BUCKET_LOGICAL_ID'
    os.environ['STAGE_BUCKET_LOGICAL_ID'] = 'STAGE_BUCKET_LOGICAL_ID'
    os.environ['ATHENA_BUCKET_LOGICAL_ID'] = 'ATHENA_BUCKET_LOGICAL_ID'
    os.environ['DATA_LAKE_ENABLED'] = 'Yes'


@pytest.fixture()
def _mock_cloudtrail_client():
    s3_client = get_service_client('cloudtrail')
    s3_client.put_event_selectors = Mock()
    return s3_client


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_cloudtrail_client):
    monkeypatch.setitem(_helpers_service_clients, 'cloudtrail', _mock_cloudtrail_client)


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "ResourceProperties": {},
            "RequestType": "Update",
        }
    ],
)

@patch('aws_solutions.extended.resource_lookup.ResourceLookup.get_physical_id')
def test_on_create_or_update(mock_get_physical_id, lambda_event, _mock_clients):
    from amc_insights.custom_resource.cloudtrail.lambdas.trail_data_events import on_create_or_update

    mock_get_physical_id.return_value = 'physical_id'

    on_create_or_update(lambda_event, None)
    _helpers_service_clients['cloudtrail'].put_event_selectors.assert_called_once()


@patch("amc_insights.custom_resource.cloudtrail.lambdas.trail_data_events.helper")
def test_event_handler(mock_helper):
    from amc_insights.custom_resource.cloudtrail.lambdas.trail_data_events import event_handler
    event_handler({}, None)
    mock_helper.assert_called_with({}, None)

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for sync_amc_initialize_template.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/custom_resource/test_sync_amc_initialize_template.py
###############################################################################

from unittest.mock import Mock, patch, MagicMock
import sys
import pytest
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients


@pytest.fixture()
def __mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics


@pytest.fixture()
def _mock_s3_client():
    s3_client = get_service_client('s3')
    s3_client.upload_file = Mock(
        return_value={
        }
    )
    s3_client.delete_object = Mock(
        return_value={

        }
    )
    return s3_client


@pytest.fixture()
def _mock_clients(monkeypatch, _mock_s3_client):
    monkeypatch.setitem(_helpers_service_clients, 's3', _mock_s3_client)


@pytest.fixture()
def _mock_functions(monkeypatch):
    def mock_list_files_to_sync_return(dir):
        return ["file.yaml"]

    monkeypatch.setattr(
        "amc_insights.custom_resource.tenant_provisioning_service.lambdas.sync_amc_initialize_template.list_files_to_sync",
        mock_list_files_to_sync_return)


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "ResourceProperties":
                {
                    "artifacts_bucket_name": "artifacts_bucket",
                    "artifacts_key_prefix": "tps/scripts/adtech",
                },
            "RequestType": "Update",
        }
    ],
)
def test_on_create_or_update(lambda_event, __mock_imports, _mock_clients, _mock_functions, caplog):
    from amc_insights.custom_resource.tenant_provisioning_service.lambdas.sync_amc_initialize_template import \
        on_create_or_update
    on_create_or_update(lambda_event, None)
    _helpers_service_clients['s3'].upload_file.assert_called_once()

@patch("amc_insights.custom_resource.tenant_provisioning_service.lambdas.sync_amc_initialize_template.helper")
def test_event_handler(mock_helper, __mock_imports):
    from amc_insights.custom_resource.tenant_provisioning_service.lambdas.sync_amc_initialize_template import event_handler
    event_handler({}, None)
    mock_helper.assert_called_with({}, None)


def test_list_files_to_sync(__mock_imports):
    from amc_insights.custom_resource.tenant_provisioning_service.lambdas.sync_amc_initialize_template import list_files_to_sync
    path = "amc_insights.custom_resource.tenant_provisioning_service.lambdas.sync_amc_initialize_template"
    with patch(f'{path}.os.listdir') as mock_listdir, patch(f'{path}.os.path.isdir') as mock_isdir:
        mock_listdir.return_value = ["test_file_2", "test_file"]
        mock_isdir.side_effect = [False, False]

        assert list_files_to_sync("some_dir") == ['some_dir/test_file_2', 'some_dir/test_file']

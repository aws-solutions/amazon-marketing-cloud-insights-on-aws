# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for sync_sdlf_heavy_transform_glue_script.
# USAGE:
#   ./run-unit-tests.sh --test-file-name glue/test_sync_sdlf_heavy_transform_glue_script.py
###############################################################################

from unittest.mock import Mock, patch, MagicMock
import pytest
import sys
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
        "data_lake.glue.lambdas.sync_sdlf_heavy_transform_glue_script.list_files_to_sync",
        mock_list_files_to_sync_return)


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "ResourceProperties":
                {
                    "artifacts_bucket_name": "artifacts_bucket",
                    "artifacts_key_prefix": "data_lake/sdlf_heavy_transform/glue/",
                },
            "RequestType": "Update",
        }
    ],
)
def test_on_create_or_update(lambda_event, __mock_imports, _mock_clients, _mock_functions):
    from data_lake.glue.lambdas.sync_sdlf_heavy_transform_glue_script import on_create_or_update
    on_create_or_update(lambda_event, None)
    _helpers_service_clients['s3'].upload_file.assert_called_once()


def test_on_create_or_update_exception(__mock_imports):
    from data_lake.glue.lambdas.sync_sdlf_heavy_transform_glue_script import on_create_or_update
    with pytest.raises(Exception):
        on_create_or_update({"ResourceProperties": "12345"}, None)


@patch("data_lake.glue.lambdas.sync_sdlf_heavy_transform_glue_script.helper")
def test_event_handler(mock_helper, __mock_imports):
    from data_lake.glue.lambdas.sync_sdlf_heavy_transform_glue_script import event_handler
    event_handler({}, None)
    mock_helper.assert_called_with({}, None)


def test_list_files_to_sync(__mock_imports):
    from data_lake.glue.lambdas.sync_sdlf_heavy_transform_glue_script import list_files_to_sync
    path = "data_lake.glue.lambdas.sync_sdlf_heavy_transform_glue_script"
    with patch(f'{path}.os.listdir') as mock_listdir, patch(f'{path}.os.path.isdir') as mock_isdir:
        mock_listdir.return_value = ["test_file_2", "test_file"]
        mock_isdir.side_effect = [False, False]

        assert list_files_to_sync("some_dir") == ['some_dir/test_file_2', 'some_dir/test_file']

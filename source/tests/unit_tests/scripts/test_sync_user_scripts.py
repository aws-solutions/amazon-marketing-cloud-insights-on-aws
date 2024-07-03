# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Scripts/sync_user_scripts.
# USAGE:
#   ./run-unit-tests.sh --test-file-name scripts/test_sync_user_scripts.py


import contextlib
import json
import os
import boto3
import pytest
from unittest.mock import patch,  Mock
from aws_solutions.core.helpers import get_service_client


_res_prop = {
        "artifacts_bucket_name": "sometestbucket",
        "artifacts_key_prefix": "test_key"
    }

@pytest.fixture
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

@pytest.fixture
def resource_properties():
    return _res_prop


@contextlib.contextmanager
def mock_dir_files():
    path = "scripts.sync_user_scripts"
    with patch(f'{path}.os.listdir') as mock_listdir, patch(f'{path}.os.path.isdir') as mock_isdir:
        mock_listdir.return_value = ["test_cleanup_scripts", "test_upgrade_scripts", "test_scripts"]
        mock_isdir.return_value = False
        yield


@mock_dir_files()
def test_on_create_or_update(resource_properties):
    with patch("scripts.sync_user_scripts.upload_bucket_contents") as upload_bucket_contents_mock:
        from scripts.sync_user_scripts import on_create_or_update

        on_create_or_update({"ResourceProperties":resource_properties}, None)
        upload_bucket_contents_mock.assert_called_once()

    # cover exception
    with pytest.raises(Exception):
        on_create_or_update({"ResourceProperties": {}}, None)



@patch("scripts.sync_user_scripts.helper")
def test_event_handler(helper_mock):
    from scripts.sync_user_scripts import event_handler

    event_handler({}, None)
    helper_mock.assert_called_once()


@mock_dir_files()
def test_delete_bucket_contents(resource_properties, _mock_s3_client):

    from scripts.sync_user_scripts import delete_bucket_contents
    delete_bucket_contents(resource_properties)


@mock_dir_files()
def test_upload_bucket_contents(resource_properties, _mock_s3_client):

    from scripts.sync_user_scripts import upload_bucket_contents
    upload_bucket_contents(resource_properties)


def test_get_bucket_name_and_key(resource_properties):
    from scripts.sync_user_scripts import get_bucket_name_and_key

    artifacts_bucket_name, object_key = get_bucket_name_and_key(
        resource_properties=resource_properties,
        file="test"
    )

    assert artifacts_bucket_name == resource_properties["artifacts_bucket_name"]
    assert object_key == f"{resource_properties['artifacts_key_prefix']}test"


@mock_dir_files()
def test_list_files_to_sync():
    from scripts.sync_user_scripts import list_files_to_sync

    assert list_files_to_sync("") == ['test_cleanup_scripts', 'test_upgrade_scripts', 'test_scripts']
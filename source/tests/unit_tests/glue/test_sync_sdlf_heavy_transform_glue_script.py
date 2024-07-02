# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest.mock import Mock
import pytest
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients
from data_lake.glue.lambdas.sync_sdlf_heavy_transform_glue_script import on_create_or_update, on_delete

_glue_path = "amc_insights/custom_resource/sdlf_datasets/glue/sdlf_heavy_transform"
_glue_script_path = f"{_glue_path}/adtech/amc/main.py"
_glue_script_local_file_path = f"../../../infrastructure/data_lake/glue/sdlf_heavy_transform/adtech/amc/main.py"


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


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "ResourceProperties":
                {
                    "artifacts_bucket_name": "artifacts_bucket",
                    "artifacts_object_key": _glue_script_path,
                    "glue_script_file": _glue_script_local_file_path,
                },
            "RequestType": "Update",
        }
    ],
)
def test_on_create_or_update(lambda_event, _mock_clients, caplog):
    on_create_or_update(lambda_event, None)
    assert 'Move glue script of sdlf heavy transform to S3 artifacts bucket' in caplog.text

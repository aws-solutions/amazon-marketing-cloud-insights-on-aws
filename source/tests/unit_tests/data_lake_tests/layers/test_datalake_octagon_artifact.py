# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock
from pytest import fixture
from data_lake.lambda_layers.data_lake_library.python.datalake_library.octagon.artifact import ArtifactAPI, Artifact

SOURCE_INFO = {
    "source_type": "source_type",
    "source_service_arn": "source_service_arn",
    "source_location_pointer": "source_location_pointer"
}

TARGET_INFO = {
    "target_type": "target_type",
    "target_service_arn": "target_service_arn",
    "target_location_pointers": ["target_location_pointers"]
}


def test_artifact_source_info():
    artifact = Artifact(dataset="dataset")
    artifact.with_source_info(**SOURCE_INFO)
    assert artifact.source_type == SOURCE_INFO["source_type"]
    assert artifact.source_service_arn == SOURCE_INFO["source_service_arn"]
    assert artifact.source_location_pointer == SOURCE_INFO[
        "source_location_pointer"]


def test_artifact_target_info():
    artifact = Artifact(dataset="dataset")
    artifact.with_target_info(**TARGET_INFO)
    assert artifact.target_type == TARGET_INFO["target_type"]
    assert artifact.target_service_arn == TARGET_INFO["target_service_arn"]
    assert artifact.target_locations == TARGET_INFO["target_location_pointers"]


def test_artifact_get_ddb_item():
    artifact = Artifact(dataset="dataset")
    artifact.with_source_info(**SOURCE_INFO)
    artifact.with_target_info(**TARGET_INFO)
    assert artifact.get_ddb_item() == {
        "dataset": artifact.dataset,
        "source_type": artifact.source_type,
        "source_service_arn": artifact.source_service_arn,
        "source_location_pointer": artifact.source_location_pointer,
        "target_type": artifact.target_type,
        "target_service_arn": artifact.target_service_arn,
        "target_location_pointers": artifact.target_locations,
    }


@fixture()
def artifact_api():
    mock_client = MagicMock()
    mock_client.dynamodb = MagicMock()
    mock_client.dynamodb.Table = MagicMock()
    mock_client.dynamodb.Table().return_value = MagicMock()
    mock_client.dynamodb.Table().return_value().get_item = MagicMock()
    mock_client.dynamodb.Table().return_value().get_item().return_value = {
        "id": "mock_id"
    }
    mock_client.is_pipeline_set = MagicMock()
    mock_client.is_pipeline_set().return_value = True
    mock_client.config = MagicMock()
    mock_client.config.get_artifacts_ttl = MagicMock()
    mock_client.config.get_artifacts_ttl.return_value = 1
    return ArtifactAPI(mock_client)


def test_artifact_api(artifact_api):
    assert artifact_api.logger is not None
    assert artifact_api.client is not None
    assert artifact_api.artifacts_table is not None
    assert artifact_api.artifacts_ttl is not None


def test_artifact_api_register_artifact(artifact_api):
    artifact = Artifact(dataset="dataset")
    artifact.get_ddb_item = MagicMock()
    test_id = artifact_api.register_artifact(artifact)
    assert test_id is not None


def test_artifact_api_get_artifact(artifact_api):
    item = artifact_api.get_artifact("mock-id")
    assert item is not None

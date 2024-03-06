# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for data_lake/lambda_layers/data_lake_library/python/datalake_library/transforms/dataset_mappings
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/layers/transforms/test_dataset_mappings_test.py


import os
import json
import contextlib
from unittest.mock import patch, mock_open, MagicMock

import pytest
import boto3
from moto import mock_aws
from botocore.exceptions import ClientError

from aws_solutions.core.helpers import get_service_client
from data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.dataset_mappings import main


MAP_FILE_LOC = 'dataset_mappings.json'


@contextlib.contextmanager
def mock_sev_path():
    table_name_prefix = "test_env_name"
    team_name = "test_team_name"
    test_mappings = [{
        "name": "some_name",
        "transforms": {
            "stuff": "some_transforms"
        }
    }]
    with (
            patch('sys.argv', ["", team_name, table_name_prefix]),
            patch(
                "data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.dataset_mappings.open", mock_open(read_data=json.dumps(test_mappings))
            ) as mock_file
        ):
        yield {
            "mock_file": mock_file, 
            "table_name_prefix": table_name_prefix,
            "team_name": team_name,
            "test_mappings": test_mappings
        }


@mock_aws
def test_main(aws_credentials):
     with mock_sev_path() as mock_vars:
        expected_transform = {'stuff': 'some_transforms'}
        dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
        table_name = 'octagon-Datasets-{}'.format(mock_vars["table_name_prefix"])
        params = {
            "TableName": table_name,
            "KeySchema": [
                {"AttributeName": "name", "KeyType": "HASH"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "name", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        table = dynamodb.create_table(**params)
        table.put_item(
            TableName=table_name,
            Item={"name": '{}-{}'.format(
            mock_vars["team_name"], mock_vars["test_mappings"][0]["name"]
            ), "transforms": expected_transform}
        )

        main()

        table = dynamodb.Table(table_name)
        table_result = table.scan()
        assert table_result["Items"] is not None
        assert table_result["Items"][0]["name"] == '{}-{}'.format(mock_vars["team_name"], mock_vars["test_mappings"][0]["name"])
        assert table_result["Items"][0]["transforms"] == expected_transform
        mock_vars["mock_file"].assert_called_with(MAP_FILE_LOC)


@mock_aws
def test_main_client_error(aws_credentials):
    with mock_sev_path() as mock_vars:
        error_response = {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "Some test error",
                }
        }
        mock_update_item = MagicMock()
        mock_update_item.side_effect = ClientError(error_response=error_response, operation_name="update_item")
        dynamo_client = get_service_client('dynamodb')
        dynamo_client.update_item = mock_update_item
        main()
        mock_vars["mock_file"].assert_called_with(MAP_FILE_LOC)


@mock_aws
def test_main_exception(aws_credentials):
    with mock_sev_path() as mock_vars:
        mock_update_item = MagicMock()
        mock_update_item.side_effect = Exception("test exception")
        dynamo_client = get_service_client('dynamodb')
        dynamo_client.update_item = mock_update_item
        with pytest.raises(Exception):
            main()
        mock_vars["mock_file"].assert_called_with(MAP_FILE_LOC)

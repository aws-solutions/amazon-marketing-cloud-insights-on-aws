# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import itertools
from unittest.mock import MagicMock
from pytest import fixture
from data_lake.lambda_layers.data_lake_library.python.datalake_library.octagon.dynamodb import clean_table


@fixture
def dynamodb_service():
    table = MagicMock()
    table.scan = MagicMock()
    table.scan.side_effect = [{
        "Count": 1,
        "Items": [{
            "id": {}, "dd": {}
        }]
    }, {
        "Count": 0
    }]
    writer = MagicMock()
    writer.delete_item = MagicMock()
    writer.delete_item.side_effect = itertools.repeat(True)
    table.batch_writer = MagicMock()
    table.batch_writer.side_effect = itertools.repeat(writer)
    dynamodb = MagicMock()
    dynamodb.Table = MagicMock()
    dynamodb.Table.side_effect = itertools.repeat(table)
    return dynamodb


def test_dynamodb_clean_table_pk(dynamodb_service):
    table_name = "mock_table"
    pk_name = "id"
    clean_table(dynamodb_service, table_name, pk_name)
    table = dynamodb_service.Table
    table.assert_called_once_with("mock_table")
    table().scan.assert_called()
    batch_writer = table().batch_writer
    batch_writer.assert_called()


def test_dynamodb_clean_table_sk(dynamodb_service):
    table_name = "mock_table"
    pk_name = "id"
    sk_name = "dd"
    clean_table(dynamodb_service, table_name, pk_name, sk_name)
    table = dynamodb_service.Table
    table.assert_called_once_with("mock_table")
    table().scan.assert_called()
    batch_writer = table().batch_writer
    batch_writer.assert_called()

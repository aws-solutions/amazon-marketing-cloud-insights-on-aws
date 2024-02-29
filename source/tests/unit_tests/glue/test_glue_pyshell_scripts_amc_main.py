# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for glue/sdlf_heavy_transform/main.
# USAGE:
#   ./run-unit-tests.sh --test-file-name glue/test_glue_pyshell_scripts_amc_main.py


import json
import os
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock
import sys


@pytest.fixture(autouse=True)
def _mock_imports():
    mocked_awsglue = MagicMock()
    sys.modules['awsglue.utils'] = mocked_awsglue
    mocked_awsglue.getResolvedOptions.return_value = {
        'SOLUTION_ID': os.environ["SOLUTION_ID"],
        'SOLUTION_VERSION': os.environ["SOLUTION_VERSION"],
        "JOB_NAME": "job",
        "SOURCE_LOCATION": "test",
        "SOURCE_LOCATIONS": "test",
        "OUTPUT_LOCATION": "test",
        "SILVER_CATALOG": "test",
        "GOLD_CATALOG": "test",
        "KMS_KEY": "123456",
        "RESOURCE_PREFIX": os.environ["RESOURCE_PREFIX"],
        "METRICS_NAMESPACE": os.environ["METRICS_NAMESPACE"]
    }

    mocked_awswrangler = MagicMock()
    sys.modules['awswrangler'] = mocked_awswrangler
    mocked_awswrangler.catalog.sanitize_table_name.return_value="glue_target_table"

@pytest.fixture(autouse=True)
def record_metric_mock():
    with patch("data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main.record_metric") as _fixture:
        yield _fixture

@pytest.fixture
def fake_glue_table_attrs():
    return {
        'StorageDescriptor': {
            'Columns': [
                {
                    'Name': 'test_name',
                    'Type': 'test_name',
                    'Comment': 'test_name',
                    'Parameters': {
                        'test_name': 'test_name'
                    }
                },
            ],
        },
        'PartitionKeys': [
            {
                'Name': 'string',
                'Type': 'string',
                'Comment': 'string',
                'Parameters': {
                    'string': 'string'
                }
            },
        ],
        'TableType': 'string',
        'Parameters': {
            'string': 'string'
        },
    }


def test_get_bucket_and_key_from_s3_uri(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import get_bucket_and_key_from_s3_uri
    output_bucket, output_key = get_bucket_and_key_from_s3_uri(
        "s3://prefix-raw-bucket/post-stage/adtech/datasetA/file")
    assert output_bucket == "prefix-raw-bucket"
    assert output_key == "post-stage/adtech/datasetA/file"


def test_athena_sanitize_name(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import athena_sanitize_name
    assert "test_name" == athena_sanitize_name("test_name")


def test_add_partitions(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import add_partitions

    add_partitions(
        outputfilebasepath="test_path",
        silver_catalog="test_silver",
        list_partns=[
            {
                "value": 123,
                "orgcolnm": "some-org"
            }
        ],
        target_table_name="test_table"
    )

    # cover test exception
    add_partitions(
            outputfilebasepath="test_path",
            silver_catalog="test_silver",
            list_partns=[
                {
                    "value": 123,
                    "orgcolnm": "some-org"
                }
            ],
            target_table_name="test_table"
        )
    

def test_get_partition_values(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import get_partition_values

    list_partns, cust_hash = get_partition_values(source_file_partitioned_path="test_customer_hash=12345/123=test123")

    assert list_partns[0]["orgcolnm"] == 'test_customer_hash'
    assert list_partns[1]["orgcolnm"] == '123'
    assert list_partns[0]["value"] == '12345'
    assert list_partns[1]["value"] == 'test123'
    assert cust_hash == "12345"


def test_add_tags_lf(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import add_tags_lf

    cust_hash_tag_dict = {
        "some_tag_key": "some_tag_value"
    }

    database_name = ""
    table_name = ""

    with mock_aws():
        client = boto3.client("lakeformation", region_name=os.environ["AWS_DEFAULT_REGION"])
        client.create_lf_tag(
            TagKey=list(cust_hash_tag_dict.keys())[0],
            TagValues=[
                list(cust_hash_tag_dict.values())[0],
            ]
        )

        add_tags_lf(cust_hash_tag_dict=cust_hash_tag_dict, database_name=database_name, table_name=table_name)
        assert "some_tag_key" == client.get_lf_tag(TagKey=list(cust_hash_tag_dict.keys())[0])["TagKey"]
        assert ['some_tag_value', 'some_tag_value'] == client.get_lf_tag(TagKey=list(cust_hash_tag_dict.keys())[0])["TagValues"]


@mock_aws
def test_create_update_tbl(_mock_imports, fake_glue_table_attrs):

    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import create_update_tbl

    client = boto3.client("glue", region_name=os.environ["AWS_DEFAULT_REGION"])
    client.create_database(DatabaseInput={"Name": "glue_dbname"})
    client.create_table(DatabaseName="glue_dbname", TableInput={"Name": "glue_target_table", **fake_glue_table_attrs})
    create_update_tbl(
        csvdf=MagicMock(),
        csv_schema={"csv_key": "csv_value"},
        tbl_schema={"tbl_key": "tbl_value"},
        silver_catalog="glue_dbname",
        target_table_name="glue_target_table",
        list_partns=[{"santcolnm": ""}],
        outputfilebasepath="some_path",
        table_exist=1,
        cust_hash="some_cust_hash",
        pandas_athena_datatypes={}
    )

    create_update_tbl(
        csvdf=MagicMock(),
        csv_schema={"csv_key": "csv_value"},
        tbl_schema={"tbl_key": "tbl_value"},
        silver_catalog="glue_dbname",
        target_table_name="glue_target_table",
        list_partns=[{"santcolnm": ""}],
        outputfilebasepath="some_path",
        table_exist=2,
        cust_hash="some_cust_hash",
        pandas_athena_datatypes={}
    )


def test_check_filtered_row(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import check_filtered_row

    mock_csvdf = MagicMock(shape=[0])
    mock_csvdf.filtered.str.lower.return_value = "true"
    test_data = {True: MagicMock(shape=[0]), False: MagicMock(shape=[0])}
    mock_csvdf.__getitem__.side_effect = test_data.__getitem__
    mock_csvdf.to_csv.return_value = MagicMock()

    check_filtered_row(mock_csvdf)
    mock_csvdf.filtered.str.lower.assert_called()


def test_check_override_match(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import check_override_match

    check_override_match(column="test_column", csvdf=MagicMock())


def test_cast_nonstring_column(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import cast_nonstring_column

    cast_nonstring_column(only_nonstring_schema={"test_column": "column_value"}, csvdf=MagicMock(), column="test_column")


@mock_aws
def test_read_schema(_mock_imports, fake_glue_table_attrs):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import read_schema

    client = boto3.client("glue", region_name=os.environ["AWS_DEFAULT_REGION"])
    client.create_database(DatabaseInput={"Name": "glue_dbname"})
    client.create_table(DatabaseName="glue_dbname", TableInput={"Name": "glue_target_table", **fake_glue_table_attrs})
    mock_csvdf = MagicMock(columns=[0])
    test = [MagicMock(dtype="test_type")]
    mock_csvdf.side_effect = test

    read_schema(
        target_table_name="glue_target_table",
        silver_catalog="glue_dbname",
        csv_schema=["test_csv"],
        csvdf=mock_csvdf
    )


def test_general_schema_conversion(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import general_schema_conversion

    mock_csvdf = MagicMock()
    mock_csvdf.select_dtypes.return_value.columns = [0]
    mock_astype = MagicMock()
    mock_astype.astype.return_value = MagicMock()
    test = [mock_astype]
    mock_csvdf.__getitem__.side_effect = test
    general_schema_conversion(
        target_table_name="test_table",
        silver_catalog="test",
        csvdf=mock_csvdf
    )


def test_check_column_type(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import check_column_type

    mock_csvdf = MagicMock()
    mock_astype = MagicMock()
    mock_astype.astype.return_value = MagicMock()
    test = [mock_astype]
    mock_csvdf.__getitem__.side_effect = test
    check_column_type(
        column="test_table",
        only_nonstring_schema={"test_column": "column_value"},
        only_string_schema=mock_csvdf,
        csvdf=MagicMock()
    )


def test_iterate_csvdf_cols(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import iterate_csvdf_cols

    iterate_csvdf_cols(
        only_string_schema=[MagicMock()],
        csvdf=MagicMock(),
        only_nonstring_schema={"test_column": "column_value"}
    )


@mock_aws
def test_process_files(_mock_imports, fake_glue_table_attrs):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.amc.main import process_files

    client = boto3.client("glue", region_name=os.environ["AWS_DEFAULT_REGION"])
    client.create_database(DatabaseInput={"Name": "glue_dbname"})
    client.create_table(DatabaseName="glue_dbname", TableInput={"Name": "glue_target_table", **fake_glue_table_attrs})

    s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
    s3.create_bucket(Bucket="test_bucket")
    s3 = boto3.resource("s3")
    s3_object = s3.Object(
       "test_bucket", "test_key"
    )
    s3_object.put(Body=json.dumps({
        "test": "tester",
    }))
    s3_object.metadata.update({"partitionedpath": "somepath","filebasename": "somefilbase","workflowname": "someworkflowname", "filetimestamp" : "sometimestamp"})
    s3_object.copy_from(CopySource={'Bucket':'test_bucket', 'Key':'test_key'}, Metadata=s3_object.metadata, MetadataDirective='REPLACE')

    key_policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'Test Key Policy',
            'Effect': 'Allow',
            'Principal': '*',
            'Action': "kms:*",
            'Resource': "*"
        }]
    }

    kms = boto3.client("kms", region_name=os.environ["AWS_DEFAULT_REGION"])
    kms_res = kms.create_key(Policy=json.dumps(key_policy))
    with patch("pandas.read_csv", MagicMock(columns=[], dtypes=[])):
        process_files(
            source_locations=["s3://test_bucket/test_key"],
            output_location="s3://test_bucket/",
            kms_key=kms_res["KeyMetadata"]["KeyId"],
            silver_catalog="glue_dbname"
        )

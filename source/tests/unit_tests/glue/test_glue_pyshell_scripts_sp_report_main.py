# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for glue/sdlf_heavy_transform/main.
# USAGE:
#   ./run-unit-tests.sh --test-file-name glue/test_glue_pyshell_scripts_sp_report_main.py

import sys
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from unittest.mock import patch
from collections import namedtuple

mocked_imports = [
    "awsglue",
    "awsglue.utils",
    "awsglue.job",
    "awsglue.context",
    'awsglue.gluetypes',
    "awsglue.dynamicframe",
    "pyspark.context",
    "pyspark.sql",
    "pyspark.sql.functions",
    'pyspark.sql.types',
]


@pytest.fixture(autouse=True)
def _mock_imports():
    mocked_awsglue = MagicMock()
    for mocked_import in mocked_imports:
        sys.modules[mocked_import] = mocked_awsglue
    mocked_awswrangler = MagicMock()
    sys.modules['awswrangler'] = mocked_awswrangler
    mocked_utilities = MagicMock()
    sys.modules['utilities'] = mocked_utilities


def test_parse_s3_object_key_valid_key(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.sp_report.main import parse_s3_object_key

    s3_object_key = 'pre-stage/adtech/<team>/<table_name>/<filename>.json'

    table_name, output_s3_path = parse_s3_object_key(s3_object_key)
    assert table_name == "<table_name>"
    assert output_s3_path == "post-stage/adtech/<team>/<table_name>/<filename>"


@patch('awsglue.context.GlueContext.create_dynamic_frame')
def test_create_dynamic_frame_from_options(mock_create_dynamic_frame, _mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.sp_report.main import load_source_data_from_s3

    mock_create_dynamic_frame.from_options.return_value = MagicMock()
    mock_glue_context = MagicMock()
    mock_glue_context.create_dynamic_frame = mock_create_dynamic_frame

    load_source_data_from_s3(mock_glue_context, "bucket", "s3_key")

    mock_create_dynamic_frame.from_options.assert_called_once()


@patch('awsglue.context.GlueContext.getSink')
def test_get_create_or_update_table(mock_get_sink, _mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.sp_report.main import create_or_update_table
    expected_sink = MagicMock()
    mock_get_sink.return_value = expected_sink

    # Mock the GlueContext object
    mock_glue_context = MagicMock()
    mock_glue_context.getSink = mock_get_sink

    create_or_update_table(mock_glue_context, None, "database", "table", "path")
    mock_get_sink.assert_called_once()


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a SparkSession for testing.
    """
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Testing Spark") \
        .getOrCreate()

    yield spark

    # Teardown
    spark.stop()


@patch('__main__.isinstance')
def test_is_choice_type_numeric_with_mock(mock_isinstance, _mock_imports, ):
    from pyspark.sql.types import StructType, IntegerType, StructField, DoubleType
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.sp_report.main import is_struct_type_numeric
    struct_fields = [
        StructField("string", DoubleType(), True),
        StructField("int", IntegerType(), True),
    ]
    struct_type = StructType(struct_fields)
    mock_isinstance.return_value = True

    result = is_struct_type_numeric(struct_type)
    assert result is True


def test_get_sp_report_columns(spark_session, _mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.sp_report.main import get_sp_report_columns
    # Create a sample DataFrame
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
    ])
    data = [("value1", "value2", "value3")]
    df = spark_session.createDataFrame(data, schema)

    # Test case 1: No columns to exclude
    columns_to_exclude = []
    expected_columns = ["col1", "col2", "col3"]
    actual_columns = get_sp_report_columns(df, columns_to_exclude)
    assert set(actual_columns) == set(expected_columns)

    # Test case 2: Exclude some columns
    columns_to_exclude = ["col2", "col3"]
    expected_columns = ["col1"]
    actual_columns = get_sp_report_columns(df, columns_to_exclude)
    assert set(actual_columns) == set(expected_columns)

    # Test case 3: Exclude all columns
    columns_to_exclude = ["col1", "col2", "col3"]
    expected_columns = []
    actual_columns = get_sp_report_columns(df, columns_to_exclude)
    assert set(actual_columns) == set(expected_columns)

    # Test case 4: Exclude non-existent columns
    columns_to_exclude = ["col4", "col5"]
    expected_columns = ["col1", "col2", "col3"]
    actual_columns = get_sp_report_columns(df, columns_to_exclude)
    assert set(actual_columns) == set(expected_columns)


def test_get_resolve_choice_specs(_mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.sp_report.main import get_resolve_choice_specs
    # Assuming you have the ReportChoiceFields namedtuple defined in your code
    ReportChoiceFields = namedtuple("ReportChoiceFields",
                                    ["fields_with_numeric_choice", "fields_with_non_numeric_choice"])
    # Prepare test data
    report_to_choice_fields = {
        "report1": ReportChoiceFields(fields_with_numeric_choice={"field1", "field2"},
                                      fields_with_non_numeric_choice={"field3", "field4"}),
        "report2": ReportChoiceFields(fields_with_numeric_choice={"field5"},
                                      fields_with_non_numeric_choice={"field6", "field7"})
    }

    # Call the function
    result = get_resolve_choice_specs(report_to_choice_fields)

    # Expected output
    expected_result = [
        ("report1[].field1", "cast:double"),
        ("report1[].field2", "cast:double"),
        ("report1[].field3", "cast:string"),
        ("report1[].field4", "cast:string"),
        ("report2[].field5", "cast:double"),
        ("report2[].field6", "cast:string"),
        ("report2[].field7", "cast:string")
    ]

    # Assert the result
    assert sorted(result) == sorted(expected_result)

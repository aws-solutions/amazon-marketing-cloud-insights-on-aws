# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for glue/sdlf_heavy_transform/main.
# USAGE:
#   ./run-unit-tests.sh --test-file-name glue/test_glue_pyshell_scripts_ads_report_main.py

import sys
import pytest
from unittest.mock import MagicMock, patch
from unittest.mock import Mock
from pyspark.sql.types import StructField, StructType, IntegerType

mocked_imports = [
    "awsglue",
    "awsglue.utils",
    "awsglue.job",
    "awsglue.context",
    "awsglue.gluetypes",
    "awsglue.dynamicframe",
    "pyspark.context",
    "pyspark.sql",
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
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main import extract_table_name_and_s3_path

    s3_object_key = 'pre-stage/adtech/<team>/<table_name>/<filename>.json'

    table_name, output_s3_path = extract_table_name_and_s3_path(s3_object_key)
    assert table_name == "<table_name>"
    assert output_s3_path == "post-stage/adtech/<team>/<table_name>/<filename>"


@patch('awsglue.context.GlueContext.create_dynamic_frame')
def test_create_dynamic_frame_from_options(mock_create_dynamic_frame, _mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main import load_source_data_from_s3

    mock_create_dynamic_frame.from_options.return_value = MagicMock()
    mock_glue_context = MagicMock()
    mock_glue_context.create_dynamic_frame = mock_create_dynamic_frame

    load_source_data_from_s3(mock_glue_context, "bucket", "s3_key")

    mock_create_dynamic_frame.from_options.assert_called_once()


@patch('awsglue.context.GlueContext.getSink')
def test_get_create_or_update_table(mock_get_sink, _mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main import create_or_update_table
    expected_sink = MagicMock()
    mock_get_sink.return_value = expected_sink

    # Mock the GlueContext object
    mock_glue_context = MagicMock()
    mock_glue_context.getSink = mock_get_sink

    create_or_update_table(mock_glue_context, None, "database", "table", "path")
    mock_get_sink.assert_called_once()


@patch('__main__.isinstance')
def test_is_choice_type_numeric_with_mock(mock_isinstance, _mock_imports, ):
    from pyspark.sql.types import StructType, IntegerType, StructField, DoubleType
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main import is_choice_type_numeric
    struct_fields = [
        StructField("string", DoubleType(), True),
        StructField("int", IntegerType(), True),
    ]
    struct_type = StructType(struct_fields)
    mock_isinstance.return_value = True

    result = is_choice_type_numeric(struct_type)
    assert result is True


@pytest.fixture
def mock_dynamic_frame():
    mock_df = Mock()
    mock_schema = StructType([
        StructField("numeric_field", IntegerType(), True),
    ])
    mock_df.schema.fields = mock_schema.fields

    # Mock the behavior of select
    mock_selected_df = Mock()
    mock_selected_df.schema.fields = [mock_schema.fields[0]]  # Only numeric_field
    mock_df.select.return_value = mock_selected_df

    mock_dynamic_frame = Mock()
    mock_dynamic_frame.toDF.return_value = mock_df
    return mock_dynamic_frame


@patch('data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main.is_choice_type_numeric', return_value=True)
def test_get_field_types_for_choices(mock_is_choice_type_numeric, mock_dynamic_frame, _mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main import categorize_choice_fields_by_type
    fields_with_choice = ["numeric_field"]
    numeric_fields, non_numeric_fields = categorize_choice_fields_by_type(
        mock_dynamic_frame, fields_with_choice
    )
    assert numeric_fields == ["numeric_field"]


@patch('data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main.categorize_choice_fields_by_type')
def test_get_resolve_choice_specs(mock_categorize_choice_fields_by_type, _mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main import get_resolve_choice_specs
    # Mock the return value of categorize_choice_fields_by_type
    mock_categorize_choice_fields_by_type.return_value = (['numeric_field'], ['string_field'])


    mock_dynamic_frame = None  # You can create a mock DynamicFrame if needed
    fields_with_choice = ['numeric_field', 'string_field']

    expected_result = [('numeric_field', 'cast:double'), ('string_field', 'cast:string')]
    actual_result = get_resolve_choice_specs(mock_dynamic_frame, fields_with_choice)

    assert actual_result == expected_result

@patch('data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main.categorize_choice_fields_by_type')
def test_get_resolve_choice_specs_empty_lists(mock_categorize_choice_fields_by_type, _mock_imports):
    from data_lake.glue.lambdas.sdlf_heavy_transform.adtech.ads_report.main import get_resolve_choice_specs
    # Mock the return value of categorize_choice_fields_by_type
    mock_categorize_choice_fields_by_type.return_value = ([], [])

    mock_dynamic_frame = None  # You can create a mock DynamicFrame if needed
    fields_with_choice = ['numeric_field', 'string_field']

    expected_result = []
    actual_result = get_resolve_choice_specs(mock_dynamic_frame, fields_with_choice)

    assert actual_result == expected_result

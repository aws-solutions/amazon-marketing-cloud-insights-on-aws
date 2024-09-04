# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for glue/sdlf_heavy_transform/shared/utilities.py.
# USAGE:
#   ./run-unit-tests.sh --test-file-name glue/test_glue_shared_utilities.py
import pytest
from unittest.mock import MagicMock, patch
import logging
from botocore.exceptions import ClientError

from data_lake.glue.lambdas.sdlf_heavy_transform.shared.utilities import GlueUtilities


SOLUTION_ARGS = {
    'SOLUTION_ID': 'test',
    'SOLUTION_VERSION': 'test',
    'RESOURCE_PREFIX': 'test',
    'METRICS_NAMESPACE': 'test'
}

@pytest.fixture
def mock_s3_client():
    s3_client = MagicMock()
    s3_client.head_object.return_value = {
        'ContentLength': 100,
        'Metadata': {
            'timestamp': 'test',
        }
    }
    s3_client.list_objects_v2.return_value = {'Contents': [{'Key': 'output.parquet'}]}
    return s3_client

@pytest.fixture
def mock_cloudwatch_client():
    cloudwatch_client = MagicMock()
    return cloudwatch_client

@pytest.fixture
def glue_utilities(mock_s3_client, mock_cloudwatch_client):
    glue_util = GlueUtilities(SOLUTION_ARGS)
    glue_util.s3_client = mock_s3_client
    glue_util.cloudwatch_client = mock_cloudwatch_client
    return glue_util

def test_create_logger(glue_utilities):
    logger = glue_utilities.create_logger()
    assert isinstance(logger, logging.Logger)
    assert len(logger.handlers) == 1

def test_put_metrics_count_value_custom_success(glue_utilities):
    glue_utilities.put_metrics_count_value_custom('test_metric', 123)
    glue_utilities.cloudwatch_client.put_metric_data.assert_called_once_with(
        Namespace=SOLUTION_ARGS['METRICS_NAMESPACE'],
        MetricData=[{
            'MetricName': 'test_metric',
            'Dimensions': [{'Name': 'stack-name', 'Value': SOLUTION_ARGS['RESOURCE_PREFIX']}],
            'Value': 123,
            'Unit': 'Count'
        }]
    )

def test_put_metrics_count_value_custom_failure(glue_utilities):
    glue_utilities.cloudwatch_client.put_metric_data.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "Internal Error"}}, 'PutMetricData'
    )
    with patch.object(glue_utilities.logger, 'error') as mock_error:
        glue_utilities.put_metrics_count_value_custom('test_metric', 123)
        mock_error.assert_called_once_with(
            'Error recording custom value 123 to metric test_metric: An error occurred (InternalError) when calling the PutMetricData operation: Internal Error'
        )

def test_record_glue_metrics_success(glue_utilities, mock_s3_client):
    mock_s3_client.head_object.return_value = {'ContentLength': 200}
    mock_s3_client.list_objects_v2.return_value = {'Contents': [{'Key': 'output.parquet'}]}

    glue_utilities.record_glue_metrics(
        'source_bucket', 'destination_bucket',
        source_keys=['source_key1'],
        destination_paths=['destination_path1']
    )

    glue_utilities.cloudwatch_client.put_metric_data.assert_any_call(
        Namespace=SOLUTION_ARGS['METRICS_NAMESPACE'],
        MetricData=[{
            'MetricName': 'SdlfHeavyTransformJob-bytes_read',
            'Dimensions': [{'Name': 'stack-name', 'Value': SOLUTION_ARGS['RESOURCE_PREFIX']}],
            'Value': 200,
            'Unit': 'Count'
        }]
    )
    glue_utilities.cloudwatch_client.put_metric_data.assert_any_call(
        Namespace=SOLUTION_ARGS['METRICS_NAMESPACE'],
        MetricData=[{
            'MetricName': 'SdlfHeavyTransformJob-bytes_written',
            'Dimensions': [{'Name': 'stack-name', 'Value': SOLUTION_ARGS['RESOURCE_PREFIX']}],
            'Value': 200,
            'Unit': 'Count'
        }]
    )

def test_record_glue_metrics_no_keys(glue_utilities):
    with patch.object(glue_utilities.logger, 'warning') as mock_warning:
        glue_utilities.record_glue_metrics('source_bucket', 'destination_bucket')
        mock_warning.assert_any_call('No source keys provided for Glue job, skipping bytes_read metric')
        mock_warning.assert_any_call('No destination paths provided for Glue job, skipping bytes_written metric')

def test_record_glue_metrics_failure(glue_utilities, mock_s3_client):
    mock_s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "Internal Error"}}, 'HeadObject'
    )
    mock_s3_client.list_objects_v2.return_value = {'Contents': [{'Key': 'output.parquet'}]}

    with patch.object(glue_utilities.logger, 'error') as mock_error:
        glue_utilities.record_glue_metrics(
            'source_bucket', 'destination_bucket',
            source_keys=['source_key1'],
            destination_paths=['destination_path1']
        )
        mock_error.assert_any_call(
            'Error retrieving bytes_read Glue metric for source_key source_key1: An error occurred (InternalError) when calling the HeadObject operation: Internal Error'
        )
        
def test_get_s3_object_metadata(glue_utilities):
    metadata = glue_utilities.get_s3_object_metadata('bucket', 'key')
    assert metadata == {'timestamp': 'test'}

def test_return_timestamp(glue_utilities):
    timestamp = glue_utilities.return_timestamp('bucket', 'key')
    assert timestamp == 'test'
    
def test_map_fixed_value_column(glue_utilities):
    input_record = {}
    column = "test_column"
    value = "test_value"
    output_record = glue_utilities.map_fixed_value_column(record=input_record, col_val=value, col_name=column)
    assert output_record == {column: value}
    
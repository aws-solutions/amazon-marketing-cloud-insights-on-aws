# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for amc_light_transform.py
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/layers/transforms/test_amc_light_transform.py

import os
from io import BytesIO
import sys
import json
import unittest
from unittest.mock import patch, Mock, MagicMock

from aws_solutions.core.helpers import get_service_client, get_service_resource, _helpers_service_clients, _helpers_service_resources

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")


MOCK_STAGE_BUCKET = "mock-stage-bucket"
MOCK_KMS_KEY = "mock-kms-key"
MOCK_TEAM = "adtech"
MOCK_DATASET = "ads_report"
MOCK_TABLE_NAME = "test-table"
MOCK_DATABASE_NAME = "test-database"


def mock_s3_resource():
    s3_resource = get_service_resource('s3')
    s3_resource.Object = MagicMock()
    s3_object = MagicMock()
    s3_object.configure_mock(
        put=MagicMock(return_value=None)
    )
    s3_resource.Object.return_value = s3_object

    return s3_resource

def mock_ssm_client():
    ssm_client = get_service_client('ssm')
    ssm_client.get_parameter = Mock()
    
    ssm_client.get_parameter.side_effect = [
        {'Parameter': {'Value': MOCK_DATABASE_NAME}},
    ]
    
    return ssm_client

def mock_dynamodb_resource():
    dynamodb_resource = get_service_resource('dynamodb')
    dynamodb_resource.Table = Mock()
    
    return dynamodb_resource

def mock_get_service_resource(service_name, *args, **kwargs):
    if service_name == 'glue': 
        return mock_glue_client()
    elif service_name == 'ssm':
        return mock_ssm_client()
    elif service_name == 'dynamodb':
        return mock_dynamodb_resource()


class TestCustomTransform(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_resource', side_effect=mock_get_service_resource)
    def setUp(self, mock_get_service_resource):   
        self.mock_s3_resource = mock_s3_resource()
        self.mock_dynamodb_resource = mock_dynamodb_resource()
        self.mock_ssm_client = mock_ssm_client()
        
    @patch.dict('sys.modules', {'awswrangler': MagicMock()})
    @patch('awswrangler.catalog.sanitize_table_name', return_value=MOCK_TABLE_NAME)  
    def test_transform_object(self, mock_sanitize_table_name):
        awswrangler = sys.modules['awswrangler']
        awswrangler.mock_sanitize_table_name = awswrangler.catalog.sanitize_table_name
        with patch('datalake_library.configuration.resource_configs.KMSConfiguration.get_kms_arn') as mock_get_kms_arn, \
             patch('datalake_library.configuration.resource_configs.S3Configuration.stage_bucket') as mock_stage_bucket, \
             patch('datalake_library.interfaces.s3_interface.S3Interface.get_last_modified') as mock_get_last_modified:
            mock_get_kms_arn.return_value = MOCK_KMS_KEY
            mock_stage_bucket.return_value = MOCK_STAGE_BUCKET
            mock_get_last_modified.return_value = "2023-08-04 12:34:56+00:00"
            from data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.stage_a_transforms.amc_light_transform import CustomTransform
            
            # set up our test function input
            resource_prefix = "test-prefix"
            bucket = "XXXXXXXXXXX"
            workflow_name = "analytics-12345678-1234-1234-1234-123456789abc"
            key = f"workflow={workflow_name}/schedule=adhoc/2024-01-01T01:01:00.000Z-{workflow_name}.csv"
            team = MOCK_TEAM
            dataset = MOCK_DATASET
            
            # run the function code
            test_response = CustomTransform().transform_object(resource_prefix, bucket, key, team, dataset)
        
        # assert that UI analytics queries are ignored and not processed
        assert test_response == []
        
    @patch.dict('sys.modules', {'awswrangler': MagicMock()})
    @patch('awswrangler.catalog.sanitize_table_name', return_value=MOCK_TABLE_NAME)  
    def test_get_table_prefix(self, mock_sanitize_table_name):
        awswrangler = sys.modules['awswrangler']
        awswrangler.mock_sanitize_table_name = awswrangler.catalog.sanitize_table_name
        with patch('datalake_library.configuration.resource_configs.KMSConfiguration.get_kms_arn') as mock_get_kms_arn, \
             patch('datalake_library.configuration.resource_configs.S3Configuration.stage_bucket') as mock_stage_bucket:
            mock_get_kms_arn.return_value = MOCK_KMS_KEY
            mock_stage_bucket.return_value = MOCK_STAGE_BUCKET
            from data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.stage_a_transforms.amc_light_transform import get_table_prefix
            
            # set up our test function input
            workflow = "test-workflow"
            oob_reports = []
            prefix = "amc"
            
            test_response = get_table_prefix(workflow, oob_reports, prefix)
        
        assert test_response == "amc"

    @patch.dict('sys.modules', {'awswrangler': MagicMock()})
    @patch('awswrangler.catalog.sanitize_table_name', return_value=MOCK_TABLE_NAME)  
    def test_get_oob_reports(self, mock_sanitize_table_name):
        awswrangler = sys.modules['awswrangler']
        awswrangler.mock_sanitize_table_name = awswrangler.catalog.sanitize_table_name
        with patch('datalake_library.configuration.resource_configs.KMSConfiguration.get_kms_arn') as mock_get_kms_arn, \
             patch('datalake_library.configuration.resource_configs.S3Configuration.stage_bucket') as mock_stage_bucket:
            mock_get_kms_arn.return_value = MOCK_KMS_KEY
            mock_stage_bucket.return_value = MOCK_STAGE_BUCKET
            from data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.stage_a_transforms.amc_light_transform import get_oob_reports
            
            # set up our test function input
            response = {
                "Items": ["test"]
            }
            
            test_response = get_oob_reports(response)
            
        assert test_response == []
        
    @patch.dict('sys.modules', {'awswrangler': MagicMock()})
    @patch('awswrangler.catalog.sanitize_table_name', return_value=MOCK_TABLE_NAME)  
    def test_get_output_path(self, mock_sanitize_table_name):
        awswrangler = sys.modules['awswrangler']
        awswrangler.mock_sanitize_table_name = awswrangler.catalog.sanitize_table_name
        with patch('datalake_library.configuration.resource_configs.KMSConfiguration.get_kms_arn') as mock_get_kms_arn, \
             patch('datalake_library.configuration.resource_configs.S3Configuration.stage_bucket') as mock_stage_bucket:
            mock_get_kms_arn.return_value = MOCK_KMS_KEY
            mock_stage_bucket.return_value = MOCK_STAGE_BUCKET
            from data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.stage_a_transforms.amc_light_transform import get_output_path
            
            # set up our test function input
            file_version = "" 
            table_prefix = "test-prefix"
            workflow_name = "test-workflow"
            schedule_frequency = "test-frequency"
            customer_hash_key = "test-hashkey"
            file_year = "test-year"
            file_month = "test-month"
            file_last_modified = "test-lastmodified"
            file_timestamp = "test-timestamp"
            file_basename = "test-basename"
            file_extension = "test-extension"
            
            output_path = get_output_path(
                file_version,
                table_prefix,
                workflow_name,
                schedule_frequency,
                customer_hash_key,
                file_year,
                file_month,
                file_last_modified,
                file_timestamp,
                file_basename,
                file_extension
            )
            
        assert output_path == "{}_{}_{}/customer_hash={}/export_year={}/export_month={}/file_last_modified={}/{}-{}.{}".format(table_prefix,workflow_name,schedule_frequency,customer_hash_key,file_year,file_month,file_last_modified,file_timestamp,file_basename,file_extension)
        
if __name__ == '__main__':
    unittest.main()

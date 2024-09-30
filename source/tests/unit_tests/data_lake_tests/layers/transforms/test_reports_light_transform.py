# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for reports_light_transform.py
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/layers/transforms/test_reports_light_transform.py

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


def mock_s3_resource():
    s3_resource = get_service_resource('s3')
    s3_resource.Object = MagicMock()
    s3_object = MagicMock()
    s3_object.configure_mock(
        put=MagicMock(return_value=None)
    )
    s3_resource.Object.return_value = s3_object

    return s3_resource

def mock_get_service_resource(service_name, *args, **kwargs):
    return mock_s3_resource()


class TestCustomTransform(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_resource', side_effect=mock_get_service_resource)
    def setUp(self, mock_get_service_resource):   
        self.mock_s3_resource = mock_s3_resource()
        
    @patch.dict('sys.modules', {'awswrangler': MagicMock()})
    @patch('awswrangler.catalog.sanitize_table_name', return_value=MOCK_TABLE_NAME)  
    def test_transform_object(self, mock_sanitize_table_name):
        awswrangler = sys.modules['awswrangler']
        awswrangler.mock_sanitize_table_name = awswrangler.catalog.sanitize_table_name
        with patch('datalake_library.configuration.resource_configs.KMSConfiguration.get_kms_arn') as mock_get_kms_arn, \
             patch('datalake_library.configuration.resource_configs.S3Configuration.stage_bucket') as mock_stage_bucket, \
             patch('data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.stage_a_transforms.reports_light_transform.CustomTransform.read_gzip', return_value=json.dumps({"test": "test"})):
            mock_get_kms_arn.return_value = MOCK_KMS_KEY
            mock_stage_bucket.return_value = MOCK_STAGE_BUCKET
            from data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.stage_a_transforms.reports_light_transform import CustomTransform
            
            # set up our test function input
            resource_prefix = "test-prefix"
            bucket = "XXXXXXXXXXX"
            table_name = MOCK_TABLE_NAME
            file_name = "report-123"
            #{team}/{dataset}/{table_name}/{file_name}.{file_extension}
            key = f"{MOCK_TEAM}/{MOCK_DATASET}/{table_name}/{file_name}.json.gz"
            team = MOCK_TEAM
            dataset = MOCK_DATASET
            
            # run the function code
            test_response = CustomTransform().transform_object(resource_prefix, bucket, key, team, dataset)
        
        # assert we write out an uncompressed file to the correct destination path
        assert test_response == [f"pre-stage/{MOCK_TEAM}/{MOCK_DATASET}/{table_name}/{file_name}.json"]
        
if __name__ == '__main__':
    unittest.main()

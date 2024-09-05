# ###############################################################################
# PURPOSE:
#   * Unit test for default_heavy_transform.py
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/layers/transforms/test_default_heavy_transform.py

import os
import sys
import json
import unittest
from unittest.mock import patch, Mock, MagicMock

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")


MOCK_DATABASE_NAME = 'test-database-name'
MOCK_GLUE_JOB_NAME = 'test-glue-job'

def mock_glue_client():
    glue_client  = get_service_client('glue')
    glue_client.start_job_run = Mock()
    glue_client.get_job_run = Mock()
    
    glue_client.start_job_run.return_value = {
        'JobRunId': 'example-job-run-id'
    }

    return glue_client

def mock_ssm_client():
    ssm_client = get_service_client('ssm')
    ssm_client.get_parameter = Mock()
    
    ssm_client.get_parameter.side_effect = [
        {'Parameter': {'Value': MOCK_DATABASE_NAME}},  # first call - silver catalog
        {'Parameter': None },   # second call - gold catalog (we don't expect there to be a value here)
        {'Parameter': {'Value': MOCK_GLUE_JOB_NAME} }   # third call - glue job name
    ]
    
    return ssm_client

def mock_get_service_client(service_name, *args, **kwargs):
    if service_name == 'glue': 
        return mock_glue_client()
    elif service_name == 'ssm':
        return mock_ssm_client()


class TestCustomTransform(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_get_service_client) # mock function calls here
    def setUp(self, mock_get_service_client):   
        self.mock_glue_client = mock_glue_client()
        self.mock_ssm_client = mock_ssm_client()
        
    @patch.dict('sys.modules', {'awswrangler': MagicMock()}) # mock library imports here
    def test_transform_object(self):
        awswrangler = sys.modules['awswrangler']
        awswrangler.mock_sanitize_table_name = awswrangler.catalog.sanitize_table_name
        with patch('datalake_library.configuration.resource_configs.KMSConfiguration.get_kms_arn') as mock_get_kms_arn: # mock class initializations inside testing code here
            mock_get_kms_arn.return_value = "mock-arn"
            from data_lake.lambda_layers.data_lake_library.python.datalake_library.transforms.stage_b_transforms.default_heavy_transform import CustomTransform
            
            # set up our test function input
            resource_prefix = "test-prefix"
            bucket = "XXXXXXXXXXX"
            keys = ["pre-stage/adtech/ads_report/11111111111111_sppurchasedproduct/report-11111111111111.json"]
            team = "adtech"
            dataset = "ads_report"
            
            # run the function code
            CustomTransform().transform_object(resource_prefix, bucket, keys, team, dataset)
        
        # capture some values passed to start_execution and assert them below
        _, kwargs = self.mock_glue_client.start_job_run.call_args
        
        # assert that the correct job arguments were passed for this dataset type
        assert kwargs['Arguments']['--SOURCE_S3_OBJECT_KEYS'] == keys[0]
        assert kwargs['Arguments']['--DATABASE_NAME'] == MOCK_DATABASE_NAME
        assert kwargs['Arguments']['--STAGE_BUCKET'] == bucket
        assert kwargs['Arguments']['--JOB_NAME'] == MOCK_GLUE_JOB_NAME
        
if __name__ == '__main__':
    unittest.main()

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Selling Partner Reporting/Lambdas/DownloadReport handler.
# USAGE:
#   ./run-unit-tests.sh --test-file-name reporting_microservices_tests/selling_partner_reporting_tests/lambdas/test_DownloadReport.py
###############################################################################

import os
import sys
import json
import unittest
import pytest
from unittest.mock import patch, Mock, MagicMock

from aws_solutions.core.helpers import get_service_client, _helpers_service_clients

sys.path.insert(0, "./infrastructure/aws_lambda_layers/microservice_layer/python/")
sys.path.insert(0, "./infrastructure/aws_lambda_layers/metrics_layer/python/")

@pytest.fixture(autouse=True)
def _mock_imports(monkeypatch):
    mocked_cloudwatch_metrics = MagicMock()
    sys.modules['cloudwatch_metrics'] = mocked_cloudwatch_metrics

def mock_s3_client():
    s3_client = get_service_client('s3')
    s3_client.put_object = Mock()
    
    return s3_client

def mock_clients():
    s3_client = mock_s3_client()
    _helpers_service_clients['s3'] = s3_client
    
    return s3_client


class TestDownloadReport(unittest.TestCase):
    @patch('aws_solutions.core.helpers.get_service_client', side_effect=mock_s3_client)
    def setUp(self, mock_s3_client):   
        os.environ['RESOURCE_PREFIX'] = 'RESOURCE_PREFIX'
        os.environ['SP_REPORT_BUCKET'] = 'SP_REPORT_BUCKET'
        os.environ['SP_REPORT_BUCKET_KMS_KEY_ID'] = 'SP_REPORT_BUCKET_KMS_KEY_ID'
        os.environ['TEAM'] = 'TEAM'
        os.environ['DATASET'] = 'DATASET'
        os.environ['STACK_NAME'] = 'STACK_NAME'
        os.environ['METRICS_NAMESPACE'] = 'METRICS_NAMESPACE'
        self.mock_s3_client = mock_clients()
        
    def test_handler_compressed(self):
        with patch('urllib3.PoolManager.request') as mock_request:
            mock_request.return_value = MagicMock(data=b"mocked data")
            from reporting_microservices.selling_partner_reporting.reporting_service.lambdas.DownloadReport.handler import handler
        
            # we pass in compressionAlgorithm to test when the file is return compressed
            test_event = {
                'tablePrefix': 'MyTable',
                'reportId': 123456,
                'url': 'presigned-url',
                'compressionAlgorithm': 'GZIP',
                'processingStatus': "DONE"
            }
        
            handler(test_event, None)
        
        _, kwargs = self.mock_s3_client.put_object.call_args
        
        self.assertEqual(kwargs['Body'], b"mocked data")
        self.assertEqual(kwargs['Bucket'], 'SP_REPORT_BUCKET')
        self.assertEqual(kwargs['Key'], 'TEAM/DATASET/MyTable/report-123456.json.gz')
        
    def test_handler_uncompressed(self):
        with patch('urllib3.PoolManager.request') as mock_request:
            mock_request.return_value = MagicMock(data=b"mocked data")
            from reporting_microservices.selling_partner_reporting.reporting_service.lambdas.DownloadReport.handler import handler
        
            # we omit compressionAlgorithm to test when the file is return uncompressed
            test_event = {
                'tablePrefix': 'MyTable',
                'reportId': 123456,
                'url': 'presigned-url',
                'processingStatus': "DONE"
            }
        
            handler(test_event, None)
        
        _, kwargs = self.mock_s3_client.put_object.call_args
        
        self.assertEqual(kwargs['Body'], b"mocked data")
        self.assertEqual(kwargs['Bucket'], 'SP_REPORT_BUCKET')
        self.assertEqual(kwargs['Key'], 'TEAM/DATASET/MyTable/report-123456.json')

    def test_other_funcs(self):
        with patch('urllib3.PoolManager.request') as mock_request:
            mock_request.return_value = MagicMock(data=b"mocked data")
            from reporting_microservices.selling_partner_reporting.reporting_service.lambdas.DownloadReport.handler import get_file_extension, set_error_message
        
            test_event = {
                'tablePrefix': 'MyTable',
                'reportId': 123456,
                'url': 'presigned-url',
                'processingStatus': "DONE",
                'compressionAlgorithm': 'NO-GZIP',
            }

            with pytest.raises(ValueError):
                get_file_extension(test_event)
            set_error_message(test_event, "error message")
        
        
        
if __name__ == '__main__':
    unittest.main()
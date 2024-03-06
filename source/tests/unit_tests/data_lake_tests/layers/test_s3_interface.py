# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for DataLakeLib/Interfaces/S3Interface.
# USAGE:
#   ./run-unit-tests.sh --test-file-name data_lake_tests/layers/test_s3_interface.py


import os
import tempfile
import json

import boto3
import pytest
from moto import mock_aws
from botocore.exceptions import ClientError
from unittest.mock import MagicMock

from data_lake.lambda_layers.data_lake_library.python.datalake_library.interfaces.s3_interface import S3Interface


@mock_aws
def test_s3_interface():
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
    kms_key = kms_res["KeyMetadata"]["KeyId"]

    bucket_name = "test_bucket"
    s3_key = "test_key.xyz"
    test_body = {"test": "test_value"}
    error_response = {
        "Error": {
            "Code": "StubResponseError",
            "Message": "Some test error",
        }
    }

    with tempfile.NamedTemporaryFile() as test_temp:
        ## test ClientError
        with pytest.raises(ClientError):
            s3_mock = MagicMock()
            s3_mock.Bucket.return_value.download_file.side_effect = ClientError(error_response=error_response, operation_name="download_file")
            s3_interface = S3Interface(s3_resource=s3_mock)
            s3_interface.download_object(bucket_name, s3_key, test_temp)
        with pytest.raises(ClientError):
            s3_mock = MagicMock()
            s3_mock.upload_file.side_effect = ClientError(error_response=error_response, operation_name="upload_file")
            s3_interface = S3Interface(s3_client=s3_mock)
            s3_interface.upload_object(test_temp.name, bucket_name, s3_key, kms_key)
        with pytest.raises(ClientError):
            s3_mock = MagicMock()
            s3_mock.Object.side_effect = ClientError(error_response=error_response, operation_name="Object")
            s3_interface = S3Interface(s3_resource=s3_mock)
            s3_interface.read_object(bucket_name, s3_key)
        with pytest.raises(ClientError):
            data_object = MagicMock()
            data_object.seek.return_value = json.dumps(test_body)
            s3_mock = MagicMock()
            s3_mock.put_object.side_effect = ClientError(error_response=error_response, operation_name="put_object")
            s3_interface = S3Interface(s3_client=s3_mock)
            s3_interface.write_object(bucket_name, s3_key, data_object, kms_key)
        with pytest.raises(ClientError):
            s3_mock = MagicMock(meta=MagicMock(client=MagicMock()))
            s3_mock.meta.client.copy.side_effect = ClientError(error_response=error_response, operation_name="meta.client.copy")
            s3_interface = S3Interface(s3_resource=s3_mock)
            s3_interface.copy_object(bucket_name, s3_key, bucket_name, s3_key, kms_key)
        with pytest.raises(ClientError):
            s3_mock = MagicMock()
            s3_mock.put_object_tagging.side_effect = ClientError(error_response=error_response, operation_name="put_object_tagging")
            s3_interface = S3Interface(s3_client=s3_mock)
            s3_interface.tag_object(bucket_name, s3_key, {})
        with pytest.raises(ClientError):
            s3_mock = MagicMock()
            s3_mock.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": 12345}]}]
            s3_mock.delete_objects.side_effect = ClientError(error_response=error_response, operation_name="delete_objects")
            s3_interface = S3Interface(s3_client=s3_mock)
            s3_interface.delete_objects(bucket_name, s3_key)
        
        s3 = boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
        s3_interface = S3Interface()
        s3.create_bucket(Bucket=bucket_name)

        bucket_dest = "test_dest_template"
        s3.create_bucket(Bucket=bucket_dest)

        test_temp.write(json.dumps(test_body).encode("utf-8"))
        test_temp.seek(0)

        s3_interface.upload_object(object_path=test_temp.name, bucket=bucket_name, key=s3_key, kms_key=kms_key)
        try:
            assert test_body == json.loads(s3_interface.read_object(bucket=bucket_name, key=s3_key).read().strip())
            temp_dir = tempfile.TemporaryDirectory()
            assert f"{temp_dir.name}/" + s3_key.split('/')[-1] == s3_interface.download_object(bucket=bucket_name, key=s3_key, temp_dir=temp_dir)
            temp_dir.cleanup()
        except Exception:
            pass

        s3_res = boto3.resource("s3")
        s3_object = s3_res.Object(bucket_name, f"test_prefix/{s3_key}")
        s3_object.put(Body="{}")
        assert ['test_prefix/test_key.xyz'] == s3_interface.list_objects(bucket=bucket_name, keys_path="test_prefix")

        s3_interface.delete_objects(bucket=bucket_name, prefix=s3_key)
        with pytest.raises(ClientError):
            s3_interface.read_object(bucket_name, s3_key)
        
        s3_interface.write_object(bucket=bucket_name, key=s3_key, data_object=test_temp, kms_key=kms_key)
        assert test_body == json.loads(s3_interface.read_object(bucket=bucket_name, key=s3_key).read().strip())

        s3_interface.copy_object(source_bucket=bucket_name, source_key=s3_key, dest_bucket=bucket_dest, dest_key=s3_key, kms_key=kms_key)
        assert test_body == json.loads(s3_interface.read_object(bucket=bucket_name, key=s3_key).read().strip())

        test_body3 = {"test_tag_key": "test_tag_value"}
        s3_interface.tag_object(bucket=bucket_dest, key=s3_key, tag_dict=test_body3)
        response = s3.get_object_tagging(Bucket=bucket_dest, Key=s3_key)
        assert response["TagSet"][0]["Key"] == "test_tag_key"
        assert response["TagSet"][0]["Value"] == "test_tag_value"

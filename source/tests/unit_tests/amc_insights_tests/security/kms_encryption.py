# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This module is responsible for testing kms encryption security in the AMC Insights stack.
"""

import pytest


def check_bucket_encryption(logical_id, resource):
    bucket_properties = resource["Properties"]
    
    # check bucket has KMS encryption
    kms_fail = f"Bucket resource {logical_id} should be KMS encrypted"
    try:
        encryption = bucket_properties["BucketEncryption"]["ServerSideEncryptionConfiguration"]
        contains_encryption = False
        for encryption_type in encryption:
            try:
                details = encryption_type["ServerSideEncryptionByDefault"]["SSEAlgorithm"]
                if details == "aws:kms":
                    contains_encryption = True
                    break
                
            except KeyError:
                continue
        
        if not contains_encryption:
            pytest.fail(kms_fail)
    
    except KeyError:
        pytest.fail(kms_fail)
        
        
def check_table_encryption(logical_id, resource):
    table_properties = resource["Properties"]
    
    # check table has KMS encryption
    kms_fail = f"DynamoDB Table resource {logical_id} should be KMS encrypted"
    try:
        encryption_type = table_properties["SSESpecification"]["SSEEnabled"]
        
        if not encryption_type:
            pytest.fail(kms_fail)
        
        encryption_algorithm = table_properties["SSESpecification"]["KMSMasterKeyId"]
        
        if encryption_algorithm is None:
            pytest.fail(kms_fail)
    
    except KeyError:
        pytest.fail(kms_fail)
    

def test_data_security(template):
    # test that all s3 buckets and dynamodb tables have kms encryption   
    buckets = template.find_resources("AWS::S3::Bucket")
    for bucket_logical_id, bucket_resource in buckets.items():
        check_bucket_encryption(logical_id=bucket_logical_id, resource=bucket_resource)
        
    tables = template.find_resources("AWS::DynamoDB::Table")
    for table_logical_id, table_resource in tables.items():
        check_table_encryption(logical_id=table_logical_id, resource=table_resource)
        
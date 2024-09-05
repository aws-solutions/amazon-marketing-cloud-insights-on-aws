# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This module is responsible for testing s3 bucket security in the AMC Insights stack.
"""


import pytest




def check_bucket(logical_id, resource):
    bucket_properties = resource["Properties"]
        
    # check bucket has blocked public access
    public_access_fail = f"Bucket resource {logical_id} should have PublicAccessBlockConfiguration"
    try:
        assert bucket_properties["PublicAccessBlockConfiguration"]["BlockPublicAcls"] == True
        assert bucket_properties["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] == True
        assert bucket_properties["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] == True
        assert bucket_properties["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] == True
    
    except KeyError:
        pytest.fail(public_access_fail)
        
def check_bucket_policy(logical_id, resource):
    bucket_policy_properties = resource["Properties"]
    
    # check deny non secure transport requests
    public_access_fail = f"Bucket Policy resource {logical_id} should deny non secure transport requests"
    statements = bucket_policy_properties["PolicyDocument"]["Statement"]
    contains_policy = False
    for statement in statements:
        try:
            if (
                    statement["Action"] == "s3:*"
                and statement["Condition"]["Bool"]["aws:SecureTransport"] == "false"
                and statement["Effect"] == "Deny"
                and statement["Principal"]["AWS"] == "*"
            ):
                contains_policy = True
                break
        
        except KeyError:
            continue
        
    if not contains_policy:
        pytest.fail(public_access_fail)
            
def test_bucket_security(template):
    # test that all s3 buckets have proper security configurations
    all_bucket_ids = set()
        
    buckets = template.find_resources("AWS::S3::Bucket")
    for bucket_logical_id, bucket_resource in buckets.items():
        check_bucket(logical_id=bucket_logical_id, resource=bucket_resource)
        # add the logical id to our list, to later check that it has a bucket policy with correct permissions
        all_bucket_ids.add(bucket_logical_id)
            
    bucket_policies = template.find_resources("AWS::S3::BucketPolicy")
    for bucket_policy_logical_id, bucket_policy_resource in bucket_policies.items():
        check_bucket_policy(logical_id=bucket_policy_logical_id, resource=bucket_policy_resource)
        # remove logical id for bucket after confirming it has an attached bucket policy
        all_bucket_ids.remove(bucket_policy_resource["Properties"]["Bucket"]["Ref"])
        
    if all_bucket_ids:
        pytest.fail(f"Bucket resources: {all_bucket_ids} should have policy attached with non secure transport requests denied")
        
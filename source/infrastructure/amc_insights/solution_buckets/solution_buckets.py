# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from constructs import Construct
import aws_cdk.aws_kms as kms
import aws_cdk as cdk
from aws_cdk.aws_s3 import BlockPublicAccess, BucketAccessControl
import aws_cdk.aws_s3 as s3

from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression


class SolutionBuckets(Construct):
    def __init__(
            self,
            scope,
            id
    ) -> None:
        super().__init__(scope, id)

        self.logging_bucket_key = kms.Key(
            self,
            id="logging-bucket-key",
            description="Logging Bucket Key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        self.logging_bucket = s3.Bucket(
            self,
            id="logging",
            encryption_key=self.logging_bucket_key,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            block_public_access=BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            versioned=True,
            enforce_ssl=True,
            object_lock_enabled=True,
        )

        self.artifacts_bucket_key = kms.Key(
            self,
            id="artifacts-bucket-key",
            description="Artifacts Bucket Key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        self.artifacts_bucket = s3.Bucket(
            self,
            id="artifacts",
            encryption_key=self.artifacts_bucket_key,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            block_public_access=BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            versioned=True,
            enforce_ssl=True,
        )

        self._add_cfn_nag_suppression()

    def _add_cfn_nag_suppression(self):
        add_cfn_nag_suppressions(
            self.logging_bucket.node.default_child,
            [
                CfnNagSuppression(rule_id="W35", reason="S3 Bucket should have access logging configured")
            ]
        )

        add_cfn_nag_suppressions(
            self.artifacts_bucket.node.default_child,
            [
                CfnNagSuppression(rule_id="W51", reason="S3 bucket should likely have a bucket policy"),
                CfnNagSuppression(rule_id="W35", reason="S3 Bucket should have access logging configured")
            ]
        )

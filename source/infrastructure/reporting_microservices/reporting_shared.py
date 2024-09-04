# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from constructs import Construct
import aws_cdk as cdk
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_kms as kms
from aws_cdk import CfnOutput
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression

from amc_insights.condition_aspect import ConditionAspect

class ReportingShared(Construct):
    def __init__(
            self,
            scope,
            id,
            creating_resources_condition: cdk.CfnCondition,
    ) -> None:
        super().__init__(scope, id)

        # Apply condition to create Microservice resources
        cdk.Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))
        
        self._resource_prefix = cdk.Aws.STACK_NAME

        self.bucket_key = kms.Key(
            self,
            id="BucketKey",
            description="Reporting Microservices Bucket Key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        self.bucket = s3.Bucket(
            self,
            id="Bucket",
            encryption_key=self.bucket_key,
            access_control=s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            event_bridge_enabled=True,
            versioned=True,
            enforce_ssl=True,
        )
        add_cfn_nag_suppressions(
            self.bucket.node.default_child,
            [
                CfnNagSuppression(rule_id="W35", reason="Bucket uses CloudTrail logging")
            ]
        )

        CfnOutput(
            self,
            id="ReportBucket",
            description="Use this link to access the Amazon Ads and Selling Partner reports bucket",
            value=f"https://s3.console.aws.amazon.com/s3/buckets/{self.bucket.bucket_name}",
            condition=creating_resources_condition
        )

        # Lambda role policy that allows accessing bucket
        self.bucket_access_policy = iam.Policy(
            self, "BucketAccessPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:PutObject",
                    ],
                    resources=[f"arn:aws:s3:::{self.bucket.bucket_name}",
                               f"arn:aws:s3:::{self.bucket.bucket_name}/*"]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "kms:Decrypt",
                        "kms:Encrypt",
                        "kms:GenerateDataKey",
                        "kms:ReEncryptTo",
                        "kms:ReEncryptFrom",
                        "kms:ListAliases",
                        "kms:ListKeys",
                    ],
                    resources=[
                        f"arn:aws:kms:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:key/{self.bucket.encryption_key.key_id}"
                    ],
                    conditions={
                        "StringEquals": {
                            "kms:CallerAccount": cdk.Aws.ACCOUNT_ID,
                            "kms:ViaService": f"s3.{cdk.Aws.REGION}.amazonaws.com"
                        },
                        "StringLike": {
                            "kms:EncryptionContext:aws:s3:arn": [
                                self.bucket.bucket_arn,
                                f"{self.bucket.bucket_arn}/*",
                            ]
                        }
                    }
                )
            ]
        )
        
        # Lambda role policy that allows creating report schedules in EventBridge
        self.cloudwatch_events_policy = iam.Policy(
            self,
            "cloudwatchEvents-Inline-Policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "events:PutRule",
                        "events:PutTargets",
                        "events:RemoveTargets",
                        "events:DeleteRule",
                        "events:TagResource",
                        "events:ListTargetsByRule"
                    ],
                    resources=[f"arn:aws:events:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:rule/{self._resource_prefix}-*"]
                )
            ]
        )
        
        # Lambda role policy that allows sending metrics to Cloudwatch
        self.cloudwatch_metrics_policy = iam.Policy(
            self,
            "PutCloudWatchMetricsPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            "cloudwatch:namespace": self.node.try_get_context("METRICS_NAMESPACE")
                        }
                    }
                )
            ]
        )
        add_cfn_nag_suppressions(
            self.cloudwatch_metrics_policy.node.default_child,
            [CfnNagSuppression(rule_id="W12", reason="IAM policy should not allow * resource")]
        )

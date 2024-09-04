# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import uuid

from constructs import Construct

from aws_cdk import (
    Duration,
    Aws,
    CustomResource,
    aws_lambda as lambda_,
    aws_iam as iam,
)
from aws_cdk.aws_iam import PolicyStatement
from platform_management.custom_resource import PLATFORM_MANAGEMENT_CUSTOM_RESOURCE_PATH
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm


class PlatformManagerUploader(Construct):

    def __init__(
            self,
            scope: Construct,
            id: str,
            resource_prefix: str,
            solution_buckets,
            notebook_samples_prefix: str
    ) -> None:
        super().__init__(scope, id)

        self._solution_buckets = solution_buckets
        self._notebook_samples_prefix = notebook_samples_prefix
        self._resource_prefix = resource_prefix

        self._create_iam_policy_for_custom_resource_lambda()
        self._create_platform_manager_lambda()
        self._create_platform_manager_custom_resource()

    def _create_iam_policy_for_custom_resource_lambda(self):
        artifacts_bucket_prefix_statement = PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:List*",
                "s3:Get*",
                "s3:Put*",
                "s3:Delete*",
            ],
            resources=[
                f"arn:aws:s3:::{self._solution_buckets.artifacts_bucket.bucket_name}/{self._notebook_samples_prefix}/*",
            ],
            conditions={
                "StringEquals": {
                    "aws:ResourceAccount": [
                        f"{Aws.ACCOUNT_ID}"
                    ]
                }
            }
        )

        self._sync_platform_manager_lambda_iam_policy = iam.Policy(
            self, "PlatformManagerLambdaIamPolicy",
            statements=[
                artifacts_bucket_prefix_statement
            ]
        )

    def _create_platform_manager_lambda(self):
        """
        This function is responsible for placing the platform manager to the S3 artifacts bucket.
        """

        self._platform_manager_lambda = SolutionsPythonFunction(
            self,
            "SyncPlatformManager",
            PLATFORM_MANAGEMENT_CUSTOM_RESOURCE_PATH / "lambdas" / "sync_platform_manager.py",
            "event_handler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            description="Lambda function for custom resource for placing the platform manager to the S3 artifacts bucket",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION")
            },
            layers=[
                SolutionsLayer.get_or_create(self),
                PowertoolsLayer.get_or_create(self)
            ]
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="platform-manager-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-platform-manager-lambda-alarm",
            lambda_function=self._platform_manager_lambda
        )

        self._sync_platform_manager_lambda_iam_policy.attach_to_role(self._platform_manager_lambda.role)
        self._solution_buckets.artifacts_bucket_key.grant_encrypt_decrypt(self._platform_manager_lambda.role)

        self._platform_manager_lambda.node.add_dependency(self._solution_buckets.artifacts_bucket_key)

    def _create_platform_manager_custom_resource(self):
        """
        This function creates the custom resource for placing the platform manager to the S3 artifacts bucket
        """
        self._platform_manager_custom_resource = CustomResource(
            self,
            "PlatformManagerCustomResource",
            service_token=self._platform_manager_lambda.function_arn,
            properties={
                "artifacts_bucket_name": self._solution_buckets.artifacts_bucket.bucket_name,
                "artifacts_key_prefix": f"{self._notebook_samples_prefix}/",
                "custom_resource_uuid": str(uuid.uuid4()) # random uuid to trigger redeploy on stack update
            },
        )
        self._platform_manager_custom_resource.node.add_dependency(self._sync_platform_manager_lambda_iam_policy)

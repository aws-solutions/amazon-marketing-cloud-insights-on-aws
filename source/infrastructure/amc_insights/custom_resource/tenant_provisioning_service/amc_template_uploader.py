# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import uuid 

from aws_cdk.aws_lambda import Runtime, Architecture
from constructs import Construct

import aws_cdk.aws_iam as iam

from aws_cdk import Duration, CustomResource, Aws

from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from amc_insights.custom_resource import AMC_INSIGHTS_CUSTOM_RESOURCE_PATH

AWS_RESOURCE_ACCOUNT_KEY = "aws:ResourceAccount"


class AMCTemplateUploader(Construct):
    def __init__(
            self,
            scope,
            id,
            solution_buckets,
            microservice_name,
            team,
            resource_prefix,
    ) -> None:
        super().__init__(scope, id)

        self._solution_buckets = solution_buckets
        self._microservice_name = microservice_name
        self._team = team
        self._resource_prefix = resource_prefix

        self._create_iam_policy_for_custom_resource_lambda()
        self._create_amc_initialize_template_lambda()
        self._create_amc_initialize_template_custom_resource()

    def _create_iam_policy_for_custom_resource_lambda(self):
        artifacts_bucket_prefix_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:List*",
                "s3:Get*",
                "s3:Put*",
                "s3:Delete*",
            ],
            resources=[
                f"arn:aws:s3:::{self._solution_buckets.artifacts_bucket.bucket_name}/{self._microservice_name}/scripts/{self._team}/*",
            ],
            conditions={
                "StringEquals": {
                    AWS_RESOURCE_ACCOUNT_KEY: [
                        f"{Aws.ACCOUNT_ID}"
                    ]
                }
            }
        )

        self._sync_cfn_template_lambda_iam_policy = iam.Policy(
            self, "AMCInitializeTemplateLambdaIamPolicy",
            statements=[artifacts_bucket_prefix_statement]
        )

    def _create_amc_initialize_template_lambda(self):
        """
        This function is responsible for placing the AMC initialize template to the S3 artifacts bucket.
        """
        self._amc_initialize_template_lambda = SolutionsPythonFunction(
            self,
            "SyncAMCInitializeTemplate",
            AMC_INSIGHTS_CUSTOM_RESOURCE_PATH / "tenant_provisioning_service" / "lambdas" /"sync_amc_initialize_template.py",
            "event_handler",
            runtime=Runtime.PYTHON_3_11,
            description="Lambda function for custom resource for placing the AMC initialize template to the S3 artifacts bucket",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self._resource_prefix,
            },
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self)
            ]
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="amc-initialize-template-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-amc-initialize-template-lambda-alarm",
            lambda_function=self._amc_initialize_template_lambda
        )

        self._sync_cfn_template_lambda_iam_policy.attach_to_role(self._amc_initialize_template_lambda.role)
        self._solution_buckets.artifacts_bucket_key.grant_encrypt_decrypt(self._amc_initialize_template_lambda.role)

        self._amc_initialize_template_lambda.node.add_dependency(self._solution_buckets.artifacts_bucket)

    def _create_amc_initialize_template_custom_resource(self):
        """
        This function creates the custom resource for placing the AMC initialize template to the S3 artifacts bucket
        """
        self._amc_initialize_template_custom_resource = CustomResource(
            self,
            "AMCInitializeTemplateCustomResource",
            service_token=self._amc_initialize_template_lambda.function_arn,
            properties={
                "artifacts_bucket_name": self._solution_buckets.artifacts_bucket.bucket_name,
                "artifacts_key_prefix": f"{self._microservice_name}/scripts/{self._team}/",
                "custom_resource_uuid": str(uuid.uuid4()) # random uuid to trigger redeploy on stack update
            },
        )
        self._amc_initialize_template_custom_resource.node.add_dependency(self._sync_cfn_template_lambda_iam_policy)

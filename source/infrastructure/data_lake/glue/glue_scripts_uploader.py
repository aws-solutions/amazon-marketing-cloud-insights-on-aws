# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import uuid
from aws_cdk.aws_iam import PolicyStatement
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_
from constructs import Construct
from aws_cdk import Duration, CustomResource
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from data_lake.glue import GLUE_CUSTOM_RESOURCE_PATH
from aws_cdk import Aws


class GlueScriptsUploader(Construct):
    """
    Custom resource to upload glue scripts
    """

    def __init__(self,
                 scope: Construct,
                 id: str,
                 solution_buckets,
                 dataset,
                 glue_prefix,
                 glue_script_path,
                 glue_script_local_file_path,

                 ) -> None:
        super().__init__(scope, id)

        self._solution_buckets = solution_buckets
        self._glue_prefix = glue_prefix
        self._resource_prefix = Aws.STACK_NAME
        self._dataset = dataset
        self._glue_script_path = glue_script_path
        self._glue_script_local_file_path = glue_script_local_file_path

        self._create_iam_policy_for_custom_resource_lambda()
        self._create_sdlf_heavy_transform_glue_script_lambda()
        self._create_sdlf_heavy_transform_glue_script_custom_resource()

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
                f"arn:aws:s3:::{self._solution_buckets.artifacts_bucket.bucket_name}/{self._glue_prefix}/*",
            ],
            conditions={
                "StringEquals": {
                    "aws:ResourceAccount": [
                        f"{Aws.ACCOUNT_ID}"
                    ]
                }
            }
        )
        policy_statements: list[iam.PolicyStatement] = [artifacts_bucket_prefix_statement]

        self._sync_sdlf_heavy_transform_glue_script_lambda_iam_policy = iam.Policy(
            self, "SDLFHeavyTransformGlueScriptLambdaPolicy",
            statements=policy_statements
        )

    def _create_sdlf_heavy_transform_glue_script_lambda(self):
        """
        This function is responsible for placing the glue script of sdlf heavy transform to the S3 artifacts bucket.
        """
        self._sdlf_heavy_transform_glue_script_lambda = SolutionsPythonFunction(
            self,
            "CreateSDLFHeavyTransformGlueScript",
            GLUE_CUSTOM_RESOURCE_PATH / "glue" / "lambdas" /"sync_sdlf_heavy_transform_glue_script.py",
            "event_handler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            function_name=f"{self._resource_prefix}-upload-{self._dataset}-glue-script",
            description="Place the glue script of sdlf heavy transform to the S3 artifacts bucket",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
            },
            layers=[
                SolutionsLayer.get_or_create(self),
                PowertoolsLayer.get_or_create(self),
            ]
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="sdlf-heavy-transform-glue-script-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-upload-{self._dataset}-glue-script-lambda-alarm",
            lambda_function=self._sdlf_heavy_transform_glue_script_lambda
        )

        self._sync_sdlf_heavy_transform_glue_script_lambda_iam_policy.attach_to_role(
            self._sdlf_heavy_transform_glue_script_lambda.role)

        self._solution_buckets.artifacts_bucket_key.grant_encrypt_decrypt(
            self._sdlf_heavy_transform_glue_script_lambda.role)

    def _create_sdlf_heavy_transform_glue_script_custom_resource(self):
        """
        This function creates the custom resource for placing the glue script of sdlf heavy transform to the S3 artifacts bucket
        """
        self._sdlf_heavy_transform_glue_script_custom_resource = CustomResource(
            self,
            "SDLFHeavyTransformGlueScriptCustomResource",
            service_token=self._sdlf_heavy_transform_glue_script_lambda.function_arn,
            properties={
                "artifacts_bucket_name": self._solution_buckets.artifacts_bucket.bucket_name,
                "artifacts_object_key": self._glue_script_path,
                "glue_script_file": self._glue_script_local_file_path,
                "amc_dataset_version": "3.0.0",
                "custom_resource_uuid": str(uuid.uuid4())  # random uuid to trigger redeploy on stack update
            },
        )
        self._sdlf_heavy_transform_glue_script_custom_resource.node.add_dependency(
            self._sync_sdlf_heavy_transform_glue_script_lambda_iam_policy)

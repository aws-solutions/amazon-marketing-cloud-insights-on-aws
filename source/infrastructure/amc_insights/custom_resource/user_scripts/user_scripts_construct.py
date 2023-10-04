# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk.aws_lambda import Runtime, Architecture
from constructs import Construct
import aws_cdk.aws_iam as iam
from aws_cdk import Duration, CustomResource, Aws, CfnOutput

from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from scripts import USER_SCRIPTS_PATH
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer


class UserScriptsCustomResource(Construct):
    def __init__(
            self,
            scope,
            id,
            solution_buckets
    ) -> None:
        super().__init__(scope, id)

        self._solution_buckets = solution_buckets
        self._resource_prefix = Aws.STACK_NAME

        self._create_iam_policy_for_custom_resource_lambda()
        self._create_user_scripts_lambda()
        self._create_user_scripts_custom_resource()
        self._create_cfn_output()

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
                f"arn:aws:s3:::{self._solution_buckets.artifacts_bucket.bucket_name}/user-scripts/*",
            ]
        )
        policy_statements: list[iam.PolicyStatement] = [artifacts_bucket_prefix_statement]

        self._sync_cfn_template_lambda_iam_policy = iam.Policy(
            self, "SyncUserScriptsLambdaIamPolicy",
            statements=policy_statements
        )

    def _create_user_scripts_lambda(self):
        """
        This function is responsible for placing the user scripts in the S3 artifacts bucket.
        """
        self._user_scripts_lambda = SolutionsPythonFunction(
            self,
            "CreateUserScripts",
            USER_SCRIPTS_PATH / "sync_user_scripts.py",
            "event_handler",
            runtime=Runtime.PYTHON_3_9,
            description="Lambda function for custom resource for placing the user scripts in the S3 artifacts bucket",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
            },
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self)
            ]
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="user-scripts-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-user-scripts-lambda-alarm",
            lambda_function=self._user_scripts_lambda
        )

        self._sync_cfn_template_lambda_iam_policy.attach_to_role(self._user_scripts_lambda.role)

        self._solution_buckets.artifacts_bucket_key.grant_encrypt_decrypt(
            self._user_scripts_lambda.role)

        self._user_scripts_lambda.node.add_dependency(self._solution_buckets.artifacts_bucket)

    def _create_user_scripts_custom_resource(self):
        """
        This function creates the custom resource for placing the user scripts in the S3 artifacts bucket
        """
        self._user_scripts_custom_resource = CustomResource(
            self,
            "UserScriptsCustomResource",
            service_token=self._user_scripts_lambda.function_arn,
            properties={
                "artifacts_bucket_name": self._solution_buckets.artifacts_bucket.bucket_name,
                "artifacts_key_prefix": "user-scripts/",
            },
        )
        self._user_scripts_custom_resource.node.add_dependency(self._sync_cfn_template_lambda_iam_policy)

    def _create_cfn_output(self):
        user_scripts_output_string = f'''
            aws s3 cp s3://{self._solution_buckets.artifacts_bucket.bucket_name}/user-scripts ./amc_insights_user_scripts --recursive
        '''
        self._user_script_output = CfnOutput(
            self,
            "UserScriptOutput",
            description="Use this command to download the solution user scripts locally",
            value=user_scripts_output_string
        )

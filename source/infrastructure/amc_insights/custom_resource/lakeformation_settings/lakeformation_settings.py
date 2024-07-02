# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk.aws_lambda import Runtime, Architecture
from constructs import Construct
import aws_cdk.aws_iam as iam
from aws_cdk import Duration, CustomResource, Aws, Aspects
from typing import List
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from amc_insights.custom_resource import AMC_INSIGHTS_CUSTOM_RESOURCE_PATH
from amc_insights.condition_aspect import ConditionAspect
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from data_lake.datasets.sdlf_dataset import SDLFDatasetConstruct


class LakeformationSettings(Construct):
    def __init__(
            self,
            scope,
            id,
            dataset_resources,
            pmn_resources,
            datalake_condition,
            microservice_condition
    ) -> None:
        super().__init__(scope, id)

        self._resource_prefix = Aws.STACK_NAME
        self._pmn_resources = pmn_resources

        self._glue_role_arn_list = [dataset.glue_role.role_arn for dataset in dataset_resources]

        self._create_lakformation_settings_lambda()

        self._create_lakeformation_settings_custom_resource(
            role_list=self._glue_role_arn_list,
            condition=datalake_condition,
            name="dataset_cr"
        )
        self._create_lakeformation_settings_custom_resource(
            role_list=[
                self._pmn_resources.sagemaker_role.role_arn
            ],
            condition=microservice_condition,
            name="pmn_cr"
        )

    ######################################
    #          Custom Resource           #
    ######################################

    def _create_lakformation_settings_lambda(self):
        self._create_iam_policy_for_custom_resource_lambda()
        self._create_lakeformation_settings_lambda()

    def _create_iam_policy_for_custom_resource_lambda(self):
        artifacts_bucket_prefix_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "lakeformation:PutDataLakeSettings",
                "lakeformation:GetDataLakeSettings",
                "lakeformation:ListPermissions",
                "lakeformation:ListLFTags",
                "lakeformation:BatchGrantPermissions"
            ],
            resources=[
                f"arn:aws:lakeformation:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog:{Aws.ACCOUNT_ID}"
            ]
        )
        policy_statements: list[iam.PolicyStatement] = [artifacts_bucket_prefix_statement]

        self._lakeformation_settings_lambda_iam_policy = iam.Policy(
            self, "LakeFormationSettingsLambdaIamPolicy",
            statements=policy_statements
        )

    def _create_lakeformation_settings_lambda(self):
        """
        This function is responsible for removing deployed roles from Lake Formation administrator list
        """
        self._lakeformation_settings_lambda = SolutionsPythonFunction(
            self,
            "LakeformationSettingsLambda",
            AMC_INSIGHTS_CUSTOM_RESOURCE_PATH / "lakeformation_settings" / "lambdas" / "remove_data_lake_admin.py",
            "event_handler",
            runtime=Runtime.PYTHON_3_9,
            description="Lambda function for custom resource for creating and placing the user iam resources in the S3 artifacts bucket",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION")
            },
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self)
            ]
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="lakeformation-settings-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-lakeformation-settings-lambda-alarm",
            lambda_function=self._lakeformation_settings_lambda
        )

        self._lakeformation_settings_lambda_iam_policy.attach_to_role(self._lakeformation_settings_lambda.role)

    def _create_lakeformation_settings_custom_resource(self, role_list, condition, name):
        """
        This function creates the custom resource for removing deployed roles from Lake Formation administrator list
        """
        self._lakeformation_settings_custom_resource = CustomResource(
            self,
            f"LakeformationSettingsLambdaCustomResource-{name}",
            service_token=self._lakeformation_settings_lambda.function_arn,
            properties={
                'ADMIN_ROLE_LIST': role_list
            }
        )
        self._lakeformation_settings_custom_resource.node.add_dependency(self._lakeformation_settings_lambda)

        Aspects.of(self._lakeformation_settings_custom_resource).add(
            ConditionAspect(self, f"ConditionAspect-{name}", condition))

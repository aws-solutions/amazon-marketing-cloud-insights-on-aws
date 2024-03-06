# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from constructs import Construct
from aws_cdk import (
    CustomResource,
    Duration,
    Aws,
    aws_lambda as lambda_,
    aws_iam as iam
)
from aws_cdk.aws_iam import Effect, PolicyStatement

from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from amc_insights.custom_resource import AMC_INSIGHTS_CUSTOM_RESOURCE_PATH
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer


class OperationalMetrics(Construct):
    service_name = "operational-metrics"

    def __init__(
            self,
            scope: Construct,
            id: str,
    ) -> None:
        super().__init__(scope, id)

        self._resource_prefix = Aws.STACK_NAME
        self._create_iam_policy_for_custom_resource_lambda()
        self._create_operational_metrics_lambda()
        self._create_operational_metrics_custom_resource()

    def _create_iam_policy_for_custom_resource_lambda(self):
        secrets_manager_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "secretsmanager:DescribeSecret",
                "secretsmanager:CreateSecret",
                "secretsmanager:DeleteSecret",
                "secretsmanager:UpdateSecret",
                "secretsmanager:PutSecretValue",
                "secretsmanager:GetSecretValue"
            ],
            resources=[
                f"arn:aws:secretsmanager:{Aws.REGION}:{Aws.ACCOUNT_ID}:secret:{Aws.STACK_NAME}-anonymized-metrics-uuid*"],
        )

        self.operational_metrics_lambda_iam_policy = iam.Policy(
            self, "OperationalMetricsLambdaIamPolicy",
            statements=[
                secrets_manager_statement
            ]
        )

    def _create_operational_metrics_lambda(self):
        """
        This function is responsible for creating the anonymized operational metrics uuid in Secrets Manager.
        """
        self._operational_metrics_lambda = SolutionsPythonFunction(
            self,
            "CreateOperationalMetrics",
            AMC_INSIGHTS_CUSTOM_RESOURCE_PATH / "anonymized_operational_metrics" / "lambdas" / "stack_uuid.py",
            "event_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            description="Lambda function for custom resource for the creating anonymized operational metrics uuid in Secrets Manager",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "STACK_NAME": Aws.STACK_NAME
            },
            layers=[
                SolutionsLayer.get_or_create(self),
                PowertoolsLayer.get_or_create(self)
            ]
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="operational-metrics-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-operational-metrics-lambda-alarm",
            lambda_function=self._operational_metrics_lambda
        )

        self.operational_metrics_lambda_iam_policy.attach_to_role(self._operational_metrics_lambda.role)

    def _create_operational_metrics_custom_resource(self):
        """
        This function creates the customer resource for creating the anonymized operational metrics uuid in Secrets Manager.
        """
        self._operational_metrics_custom_resource = CustomResource(
            self,
            "OperationalMetricsCustomResource",
            service_token=self._operational_metrics_lambda.function_arn,
        )

        self._operational_metrics_custom_resource.node.add_dependency(self.operational_metrics_lambda_iam_policy)

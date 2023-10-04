# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from constructs import Construct
from aws_cdk import (
    Duration,
    Aws,
    aws_lambda as lambda_,
    aws_iam as iam
)
from aws_cdk.aws_iam import Effect, PolicyStatement
from aws_cdk.aws_events import CfnRule
from aws_cdk.aws_lambda import CfnPermission

from amc_insights.custom_resource import AMC_INSIGHTS_CUSTOM_RESOURCE_PATH
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression


class CloudwatchMetrics(Construct):
    service_name = "cloudwatch-metrics"

    def __init__(
            self,
            scope: Construct,
            id: str,
    ) -> None:
        super().__init__(scope, id)

        self._resource_prefix = Aws.STACK_NAME
        self._create_iam_policy()
        self._create_cloudwatch_metrics_function()
        self._create_event_bridge_rule()

    def _create_iam_policy(self):
        secrets_manager_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "secretsmanager:GetSecretValue"
            ],
            resources=[
                f"arn:aws:secretsmanager:{Aws.REGION}:{Aws.ACCOUNT_ID}:secret:{Aws.STACK_NAME}-anonymous-metrics-uuid*"],
        )
        cloudwatch_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "cloudwatch:GetMetricStatistics"
            ],
            resources=[
                "*"  # NOSONAR
            ]
        )

        self.cloudwatch_metrics_lambda_iam_policy = iam.Policy(
            self, "CloudwatchMetricsLambdaIamPolicy",
            statements=[
                secrets_manager_statement,
                cloudwatch_statement
            ]
        )
        add_cfn_nag_suppressions(
            self.cloudwatch_metrics_lambda_iam_policy.node.default_child,
            [
                CfnNagSuppression(rule_id="W12",
                                  reason="IAM policy should not allow * resource")
            ]
        )

    def _create_cloudwatch_metrics_function(self):
        """
        This function is responsible for aggregating and reporting CloudWatch metrics.
        """
        self._cloudwatch_metrics_function = SolutionsPythonFunction(
            self,
            "CloudwatchMetricsFunction",
            AMC_INSIGHTS_CUSTOM_RESOURCE_PATH / "cloudwatch_metrics" / "lambdas" / "report.py",
            "event_handler",
            runtime=lambda_.Runtime.PYTHON_3_10,
            description="Lambda function for reporting cloudwatch metrics",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "STACK_NAME": Aws.STACK_NAME,
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE")
            },
            layers=[
                SolutionsLayer.get_or_create(self)
            ]
        )

        self.cloudwatch_metrics_lambda_iam_policy.attach_to_role(self._cloudwatch_metrics_function.role)

    def _create_event_bridge_rule(self):
        send_cloudwatch_metrics_rule = CfnRule(
            self,
            "CloudwatchMetricsRule",
            name=f"{self._resource_prefix}-send-cloudwatch-metrics",
            description="Send cloudwatch metrics daily at 5am UTC",
            schedule_expression="cron(0 5 * * ? *)",
            state="ENABLED",
            targets=[CfnRule.TargetProperty(
                arn=self._cloudwatch_metrics_function.function_arn,
                id="send-cloudwatch-metrics",
            )])

        CfnPermission(
            self,
            "CloudwatchMetricsPermissions",
            action="lambda:InvokeFunction",
            function_name=self._cloudwatch_metrics_function.function_arn,
            principal="events.amazonaws.com",
            source_arn=send_cloudwatch_metrics_rule.attr_arn
        )

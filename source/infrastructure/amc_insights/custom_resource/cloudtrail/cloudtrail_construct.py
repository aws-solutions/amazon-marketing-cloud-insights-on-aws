# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk import CustomResource, Duration, Aws
import aws_cdk.aws_cloudtrail as cloud_trail
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_
from constructs import Construct

from amc_insights.custom_resource import AMC_INSIGHTS_CUSTOM_RESOURCE_PATH
from amc_insights.solution_buckets import SolutionBuckets
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm


class CloudTrailConstruct(Construct):
    def __init__(
            self,
            scope,
            id,
            solution_buckets_resources: SolutionBuckets,
            data_lake_enabled
    ) -> None:
        """
        This construct creates a CloudTrail resource and conditionally sets Data Events to the CloudTrail.
        """
        super().__init__(scope, id)

        self.solution_buckets_resources = solution_buckets_resources
        self._data_lake_enabled = data_lake_enabled.value_as_string

        self._resource_prefix = Aws.STACK_NAME

        self.trail = cloud_trail.Trail(
            self,
            "S3AndLambda",
            bucket=self.solution_buckets_resources.logging_bucket,
            is_multi_region_trail=True,
            include_global_service_events=True,
            management_events=cloud_trail.ReadWriteType.ALL,
        )

        self._create_iam_policy_for_custom_resource_lambdas()
        self._custom_resource_trail_data_events()

    def _create_iam_policy_for_custom_resource_lambdas(self):
        self._set_data_events_lambda_iam_policy = iam.Policy(
            self, "SetDataEventInTrail",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "cloudtrail:GetEventSelectors",
                        "cloudtrail:PutEventSelectors",
                        "cloudtrail:GetTrail",
                        "cloudtrail:ListTrails",
                        "cloudtrail:UpdateTrail",
                    ],
                    resources=[
                        self.trail.trail_arn
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "cloudformation:DescribeStacks",
                        "cloudformation:DescribeStackResource"
                    ],
                    resources=[
                        f"arn:aws:cloudformation:*:{Aws.ACCOUNT_ID}:stack/{self._resource_prefix}*",
                    ]
                )
            ]
        )

    def _custom_resource_trail_data_events(self):
        """
        Create custom resource to set Data Events in CloudTrail
        """
        set_data_events_lambda = SolutionsPythonFunction(
            self,
            "SetDataEvents",
            AMC_INSIGHTS_CUSTOM_RESOURCE_PATH / "cloudtrail" / "lambdas" /"trail_data_events.py",
            "event_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            description="Lambda function for custom resource to set data events in CloudTrail when Data Lake is deployed",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "CLOUD_TRAIL_ARN": self.trail.trail_arn,
                "ARTIFACTS_BUCKET_ARN": self.solution_buckets_resources.artifacts_bucket.bucket_arn,
                "RAW_BUCKET_LOGICAL_ID": 'foundationsrawbucket6964B12D',
                "STAGE_BUCKET_LOGICAL_ID": 'foundationsstagebucket7D53680B',
                "ATHENA_BUCKET_LOGICAL_ID": 'foundationsathenabucket9F4DB591',
                "LAMBDA_FUNCTION_ARNS_START_WITH": f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:{self._resource_prefix}*",
                "STACK_NAME": self._resource_prefix,
                "DATA_LAKE_ENABLED": str(self._data_lake_enabled)
            },
            layers=[
                SolutionsLayer.get_or_create(self),
                PowertoolsLayer.get_or_create(self)
            ]
        )
        self._set_data_events_lambda_iam_policy.attach_to_role(set_data_events_lambda.role)

        SolutionsLambdaFunctionAlarm(
            self,
            id="set-data-events-with-datalake-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-set-data-events-lambda-alarm",
            lambda_function=set_data_events_lambda
        )

        CustomResource(
            self,
            "SetDataEventsCustomResource",
            service_token=set_data_events_lambda.function_arn,
        )

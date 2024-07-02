# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
from constructs import Construct
import aws_cdk.aws_events as events
from aws_cdk import Duration
import aws_cdk.aws_events_targets as targets
from aws_cdk.aws_iam import Effect, PolicyStatement, ServicePrincipal, Policy
from aws_cdk.aws_lambda import Code, LayerVersion, Function, Runtime
import aws_cdk.aws_lambda as lambda_
from aws_cdk import Aws, Aspects
from amc_insights.condition_aspect import ConditionAspect
from aws_cdk.aws_ssm import StringParameter

from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from data_lake.foundations.foundations_construct import FoundationsConstruct
from ..stages import (
    SDLFLightTransform,
    SDLFLightTransformConfig,
    SDLFHeavyTransform,
    SDLFHeavyTransformConfig
)


class SDLFPipelineConstruct(Construct):
    def __init__(
            self,
            scope: Construct,
            id: str,
            environment_id: str,
            team: str,
            foundations_resources: FoundationsConstruct,
            pipeline,
            creating_resources_condition,
    ) -> None:
        super().__init__(scope, id)

        # Apply condition to resources pipeline construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._resource_prefix = Aws.STACK_NAME
        self._environment_id: str = environment_id
        self._team = team
        self.pipeline = pipeline
        self._foundations_resources = foundations_resources

        # Simple single-dataset pipeline with static config
        self._create_sdlf_pipeline(
            team=self._team,
            pipeline=self.pipeline,
            foundations_resources=self._foundations_resources
        )

    def _create_sdlf_pipeline(self, team, pipeline, foundations_resources) -> None:
        # Routing function
        self._create_lamda_layer()
        self.routing_function = self._create_routing_lambda()

        # S3 Event Capture (Raw Bucket)
        self._create_s3_event_capture(
            bucket_name=self._foundations_resources.raw_bucket.bucket_name,
            lambda_event_target=self.routing_function
        )

        # Stage A
        self.stage_a_transform = SDLFLightTransform(
            self,
            id="sdlf-stage-a",
            resource_prefix=self._resource_prefix,
            description=f"{self._resource_prefix} data lake light transform",
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{team}-{pipeline}-stage-a",
                "type": "octagon_pipeline",
                "description": f"{self._resource_prefix} data lake light transform",
                "id": "sdlf-stage-a"
            },
            environment_id=self._environment_id,
            config=SDLFLightTransformConfig(
                team=team,
                pipeline=pipeline
            ),
            foundations_resources=foundations_resources,
        )

        StringParameter(
            self,
            "stage-a-role-name",
            parameter_name=f"/{self._resource_prefix}/Lambda/StageARoleName",
            simple_name=True,
            string_value=self.stage_a_transform._process_lambda.role.role_name,
        )

        # Stage B
        self.stage_b_transform = SDLFHeavyTransform(
            self,
            id="sdlf-stage-b",
            resource_prefix=self._resource_prefix,
            description=f"{self._resource_prefix} data lake heavy transform",
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{team}-{pipeline}-stage-b",
                "type": "octagon_pipeline",
                "description": f"{self._resource_prefix} data lake heavy transform",
                "id": "sdlf-stage-b"
            },
            config=SDLFHeavyTransformConfig(
                team=team,
                pipeline=pipeline
            ),
            foundations_resources=foundations_resources,
        )

    def _create_lamda_layer(self):
        self.metrics_layer = LayerVersion(
            self,
            "metrics-layer",
            code=Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[3]}",
                    "infrastructure/aws_lambda_layers/metrics_layer/"
                )
            ),
            layer_version_name=f"{self._resource_prefix}-metrics-layer",
            compatible_runtimes=[Runtime.PYTHON_3_9],
        )

    def _create_routing_lambda(self) -> Function:
        # Lambda
        routing_function: Function = lambda_.Function(
            self,
            id="data-lake-routing",
            function_name=f"{self._resource_prefix}-data-lake-routing",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent.parent}", "pipelines/lambdas/routing")),
            handler="handler.lambda_handler",
            description="Routes data to the right team and pipeline for processing",
            timeout=Duration.seconds(60),
            memory_size=256,
            runtime=Runtime.PYTHON_3_9,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "ENV": self._environment_id,
                "RESOURCE_PREFIX": self._resource_prefix,
                "SDLF_CUSTOMER_CONFIG": self._foundations_resources.customer_config_table.table_name,
                "OCTAGON_DATASET_TABLE_NAME": self._foundations_resources.datasets.table_name,
                "OCTAGON_METADATA_TABLE_NAME": self._foundations_resources.object_metadata.table_name,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            layers=[
                SolutionsLayer.get_or_create(self),
                self._foundations_resources.powertools_layer,
                self.metrics_layer
            ]
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="data-lake-routing-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-data-lake-routing-lambda-alarm",
            lambda_function=routing_function
        )

        StringParameter(
            self,
            "routing-queue-arn",
            parameter_name=f"/{self._resource_prefix}/Lambda/RoutingQueueArn",
            simple_name=True,
            string_value=routing_function.function_arn,
        )

        dynamodb_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "dynamodb:Query",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem"
            ],
            resources=[
                self._foundations_resources.customer_config_table.table_arn,
                f"{self._foundations_resources.customer_config_table.table_arn}/*",
                self._foundations_resources.object_metadata.table_arn,
                f"{self._foundations_resources.object_metadata.table_arn}/*",
                self._foundations_resources.datasets.table_arn,
                f"{self._foundations_resources.datasets.table_arn}/*"
            ],
        )

        kms_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "kms:Decrypt",
                "kms:DescribeKey",
                "kms:Encrypt",
                "kms:GenerateDataKey",
                "kms:ReEncryptTo",
                "kms:ReEncryptFrom",
                "kms:ListAliases",
                "kms:ListKeys",
            ],
            resources=["*"],
            conditions={
                "ForAnyValue:StringLike": {
                    "kms:ResourceAliases": "alias/*"
                }
            }
        )

        sqs_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "sqs:SendMessage",
                "sqs:DeleteMessage",
                "sqs:ReceiveMessage",
                "sqs:GetQueueAttributes",
                "sqs:ListQueues",
                "sqs:GetQueueUrl",
                "sqs:ListDeadLetterSourceQueues",
                "sqs:ListQueueTags"
            ],
            resources=[f"arn:aws:sqs:{Aws.REGION}:{Aws.ACCOUNT_ID}:{self._resource_prefix}-*"],
        )

        ssm_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "ssm:GetParameter",
                "ssm:GetParameters",
                "ssm:GetParametersByPath",
            ],
            resources=[f"arn:aws:ssm:{Aws.REGION}:{Aws.ACCOUNT_ID}:parameter/{self._resource_prefix}/*"],
        )

        # This policy grants permission to record metrics in CloudWatch.
        # This is needed for anonymized metrics.
        cloudwatch_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=["cloudwatch:PutMetricData"],
            resources=[
                "*"
            ],
            conditions={"StringEquals": {
                "cloudwatch:namespace": self.node.try_get_context("METRICS_NAMESPACE")}}
        )

        routing_function_policy = Policy(
            self, "routing-function-role-policy",
            statements=[dynamodb_policy_statement, kms_policy_statement, sqs_policy_statement, ssm_policy_statement, cloudwatch_policy_statement]
        )
        add_cfn_nag_suppressions(
            routing_function_policy.node.default_child,
            [CfnNagSuppression(rule_id="W12", reason="IAM policy should not allow * resource")]
        )

        routing_function_policy.attach_to_role(routing_function.role)

        routing_function.add_permission(
            id="invoke-lambda",
            principal=ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction"
        )

        routing_function.node.add_dependency(self._foundations_resources.customer_config_table)
        routing_function.node.add_dependency(self._foundations_resources.object_metadata)
        routing_function.node.add_dependency(self._foundations_resources.datasets)

        return routing_function

    def _create_s3_event_capture(self, bucket_name, lambda_event_target):
        event_names = [
            "CopyObject",
            "CompleteMultipartUpload",
            "PutObject",
            "DeleteObject"
        ]

        events.Rule(
            self,
            id="raw-s3-bucket-event-capture",
            rule_name=f"{self._resource_prefix}-raw-bucket-s3-event-capture",
            description="Capture data landing in the raw s3 bucket",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail={
                    "eventSource": [
                        "s3.amazonaws.com"
                    ],
                    "eventName": event_names,
                    "requestParameters": {
                        "bucketName": [
                            bucket_name
                        ],
                    }
                },
            ),
            targets=[targets.LambdaFunction(lambda_event_target)]
        )

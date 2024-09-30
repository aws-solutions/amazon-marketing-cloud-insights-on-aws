# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import json
from aws_cdk.aws_lambda import Code, LayerVersion, Runtime
from aws_cdk import aws_stepfunctions as sfn, Aspects
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal, Policy
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk.aws_sqs import DeadLetterQueue, QueueEncryption
from aws_cdk import RemovalPolicy, Duration, Aws, Fn
from aws_cdk.aws_ssm import StringParameter
from constructs import Construct
import aws_cdk.aws_kms as kms
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_cloudwatch as cloudwatch
import aws_cdk.aws_logs as logs
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from data_lake.register.register_construct import RegisterConstruct
from data_lake.foundations.foundations_construct import FoundationsConstruct
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression, CfnNagSuppressAll
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer


@dataclass
class SDLFLightTransformConfig:
    team: str
    pipeline: str


class SDLFLightTransform(Construct):
    def __init__(
            self,
            scope,
            resource_prefix: str,
            id: str,
            config: SDLFLightTransformConfig,
            props: Dict[str, Any],
            environment_id: str,
            foundations_resources: FoundationsConstruct,
            **kwargs: Any,
    ) -> None:
        super().__init__(scope, id)

        self._environment_id: str = environment_id
        self.resource_prefix = resource_prefix
        self._props: Dict[str, Any] = props
        self._foundations_resources = foundations_resources
        self.team = config.team
        self.pipeline = config.pipeline

        RegisterConstruct(self, self._props["id"], props=self._props,
                          register_lambda=self._foundations_resources.register_function)

        self._create_queue(self.team, self.pipeline)

        self._create_lambdas(self.team, self.pipeline, scope)

        # Create event source mapping from SQS (routing-a) to Lambda (routing-a)
        self._create_routing_a_event_source_mapping()

        self._create_state_machine()

        self._suppress_cfn_nag_warnings()

    def _create_queue(self, team, pipeline) -> None:
        # SQS and DLQ
        # sqs kms key resource
        sqs_key = kms.Key(
            self,
            "sqs-key-a",
            description="SQS Key Stage A",
            alias=f"alias/{self.resource_prefix}-{team}-{pipeline}-sqs-stage-a-key",
            enable_key_rotation=True,
            pending_window=Duration.days(30),
            removal_policy=RemovalPolicy.DESTROY,
        )

        self._routing_dlq = DeadLetterQueue(
            max_receive_count=1,
            queue=sqs.Queue(self,
                            id='amc-dlq-a',
                            queue_name=f'{self.resource_prefix}-{team}-{pipeline}-dlq-a.fifo',
                            fifo=True,
                            visibility_timeout=Duration.seconds(60),
                            encryption=QueueEncryption.KMS,
                            encryption_master_key=sqs_key,
                            removal_policy=RemovalPolicy.DESTROY))

        cloudwatch.Alarm(
            self,
            id='alarm-dlq-a',
            alarm_description='CloudWatch Alarm for Routing DLQ A',
            metric=self._routing_dlq.queue.metric('ApproximateNumberOfMessagesVisible', period=Duration.seconds(60)),
            evaluation_periods=1,
            datapoints_to_alarm=1,
            threshold=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
        )

        StringParameter(
            self,
            'amc-dlq-a.fifo-ssm',
            parameter_name=f"/{self.resource_prefix}/SQS/{team}/{pipeline}StageADLQ",
            simple_name=True,
            string_value=f'{self.resource_prefix}-{team}-{pipeline}-dlq-a.fifo',
        )

        self._routing_queue = sqs.Queue(
            self,
            id='queue-a',
            queue_name=f'{self.resource_prefix}-{team}-{pipeline}-queue-a.fifo',
            fifo=True,
            content_based_deduplication=True,
            visibility_timeout=Duration.seconds(60),
            encryption=QueueEncryption.KMS,
            encryption_master_key=sqs_key,
            dead_letter_queue=self._routing_dlq,
            removal_policy=RemovalPolicy.DESTROY)

        StringParameter(
            self,
            'amc-queue-a.fifo-ssm',
            parameter_name=f"/{self.resource_prefix}/SQS/{team}/{pipeline}StageAQueue",
            simple_name=True,
            string_value=f'{self.resource_prefix}-{team}-{pipeline}-queue-a.fifo',
        )

    def _create_routing_a_event_source_mapping(self):
        self._routing_lambda.add_event_source(
            SqsEventSource(
                self._routing_queue,
                batch_size=10,
            )
        )

    def _create_lambdas(self, team, pipeline, scope) -> None:
        lambda_handler = "handler.lambda_handler"

        self._routing_lambda = lambda_.Function(
            self,
            "routing-a",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-routing-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent}", "lambdas/routing")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "RESOURCE_PREFIX": self.resource_prefix,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            description="Triggers Data Lake StageA step function",
            timeout=Duration.minutes(1),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            runtime=Runtime.PYTHON_3_11,
        )

        # Grant permission to record metrics in cloudwatch.
        # This is needed for anonymized metrics.
        self._routing_lambda.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["cloudwatch:PutMetricData"],
                resources=[
                    "*"
                ],
                conditions={"StringEquals": {
                    "cloudwatch:namespace": self.node.try_get_context("METRICS_NAMESPACE")}}
            )
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="routing-a-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-routing-a-lambda-alarm",
            lambda_function=self._routing_lambda
        )

        self._redrive_lambda = lambda_.Function(
            self,
            "redrive-a",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-redrive-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent}", "lambdas/redrive")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageA",
                "RESOURCE_PREFIX": self.resource_prefix
            },
            description="Redrive Data Lake StageA Step Function",
            timeout=Duration.minutes(1),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            runtime=Runtime.PYTHON_3_11,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="redrive-a-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-redrive-a-lambda-alarm",
            lambda_function=self._redrive_lambda
        )

        self._postupdate_lambda = lambda_.Function(
            self,
            "postupdate-a",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-postupdate-a",
            code=Code.from_asset(
                os.path.join(f"{Path(__file__).parent}", "lambdas/postupdate_metadata")),
            handler=lambda_handler,
            environment={
                "stage_bucket": self._foundations_resources.stage_bucket.bucket_name,
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self.resource_prefix
            },
            description="Post update comprehensive catalogue metadata in Data Lake StageA",
            timeout=Duration.minutes(1),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            runtime=Runtime.PYTHON_3_11,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="postupdate-a-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-postupdate-a-lambda-alarm",
            lambda_function=self._postupdate_lambda
        )

        self._preupdate_lambda = lambda_.Function(
            self,
            "preupdate-a",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-preupdate-a",
            code=Code.from_asset(
                os.path.join(f"{Path(__file__).parent}", "lambdas/preupdate_metadata")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self.resource_prefix
            },
            description="Pre update comprehensive catalogue metadata in Data Lake StageA",
            timeout=Duration.minutes(1),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            runtime=Runtime.PYTHON_3_11,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="preupdate-a-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-preupdate-a-lambda-alarm",
            lambda_function=self._preupdate_lambda
        )

        self._error_lambda = lambda_.Function(
            self,
            "error-a",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-error-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent}", "lambdas/error")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self.resource_prefix
            },
            description="Send errors to DLQ in Data Lake StageA",
            timeout=Duration.minutes(1),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            runtime=Runtime.PYTHON_3_11,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="error-a-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-error-a-lambda-alarm",
            lambda_function=self._error_lambda
        )

        self._process_lambda = lambda_.Function(
            self,
            "process-a",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-process-a",
            code=Code.from_asset(
                os.path.join(f"{Path(__file__).parent}", "lambdas/process_object")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self.resource_prefix
            },
            description="Executes lights transform in Data Lake StageA",
            timeout=Duration.minutes(15),
            memory_size=1536,
            runtime=Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="process-a-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-process-a-lambda-alarm",
            lambda_function=self._process_lambda
        )

        self._foundations_resources.raw_bucket_key.grant_decrypt(self._process_lambda)
        self._foundations_resources.raw_bucket.grant_read(self._process_lambda)
        self._foundations_resources.stage_bucket_key.grant_encrypt(self._process_lambda)
        self._foundations_resources.stage_bucket.grant_write(self._process_lambda)

        self.lambda_functions = [self._routing_lambda, self._postupdate_lambda, self._preupdate_lambda,
                            self._process_lambda, self._error_lambda, self._redrive_lambda]

        self._add_layers(self.lambda_functions)
        self._create_and_attach_policy_to_lambda_roles(team, pipeline, self.lambda_functions)
        self._add_dependencies(self.lambda_functions)

    def _add_layers(self, lambda_functions):
        self._process_lambda.add_layers(self._foundations_resources.wrangler_layer)

        metrics_layer = LayerVersion(
            self,
            "metrics-layer",
            code=Code.from_asset(path=os.path.join(f"{Path(__file__).parents[3]}", "aws_lambda_layers/metrics_layer/")),
            layer_version_name=f"{self.resource_prefix}-metrics-layer",
            compatible_runtimes=[Runtime.PYTHON_3_11],
        )

        data_lake_layer_version = LayerVersion.from_layer_version_arn(
            self,
            "layer-1",
            layer_version_arn=self._foundations_resources.data_lake_library_layer.layer_version_arn,
        )

        for lambda_function in lambda_functions:
            lambda_function.add_layers(data_lake_layer_version)
            lambda_function.add_layers(SolutionsLayer.get_or_create(self))
            lambda_function.add_layers(PowertoolsLayer.get_or_create(self))
            lambda_function.add_layers(metrics_layer)

    def _add_dependencies(self, lambda_functions):
        self._process_lambda.node.add_dependency(self._foundations_resources.wrangler_layer)

        for lambda_function in lambda_functions:
            lambda_function.node.add_dependency(self._foundations_resources.powertools_layer)

    def _create_and_attach_policy_to_lambda_roles(self, team, pipeline, lambda_functions):
        process_lambda_s3_policy = Policy(
            self,
            "lambda-s3-policy",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:Get*",
                        "s3:List*",
                        "s3-object-lambda:Get*",
                        "s3-object-lambda:List*"
                    ],
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            "aws:ResourceAccount": [
                                f"{Aws.ACCOUNT_ID}"
                            ]
                        }
                    }
                )
            ]
        )

        sm_a_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "states:StartExecution",
            ],
            resources=[
                f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:{self.resource_prefix}-{team}-{pipeline}-sm-a"],
        )

        kms_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
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
                f"arn:aws:kms:*:{Aws.ACCOUNT_ID}:key/*"
            ],
            conditions={
                "ForAnyValue:StringLike": {
                    "kms:ResourceAliases": [f"alias/{self.resource_prefix}-*", "alias/tps-*"]
                }
            }
        )

        s3_pre_stage_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "s3:GetObject"
            ],
            resources=[
                f"arn:aws:s3:::{self._foundations_resources.stage_bucket.bucket_name}/pre-stage/{team}/*"
            ],
            conditions={
                "StringEquals": {
                    "aws:ResourceAccount": [
                        f"{Aws.ACCOUNT_ID}"
                    ]
                }
            }
        )

        dynamodb_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "dynamodb:Query",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
            ],
            resources=[
                f"arn:aws:dynamodb:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/octagon-*",
                f"arn:aws:dynamodb:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{self.resource_prefix}-*"
            ],
        )

        ssm_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "ssm:GetParameter",
                "ssm:GetParameters",
                "ssm:GetParametersByPath",
            ],
            resources=[f"arn:aws:ssm:{Aws.REGION}:{Aws.ACCOUNT_ID}:parameter/{self.resource_prefix}/*"],
        )

        sqs_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "sqs:SendMessage",
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:ListQueues",
                "sqs:GetQueueUrl",
                "sqs:ListDeadLetterSourceQueues",
                "sqs:ListQueueTags"
            ],
            resources=[f"arn:aws:sqs:{Aws.REGION}:{Aws.ACCOUNT_ID}:{self.resource_prefix}-{team}-*"],
        )

        lambda_policy = Policy(
            self,
            "sdlf-light-transform-lambdas-policy",
            statements=[
                sm_a_policy_statement, kms_policy_statement,
                s3_pre_stage_policy_statement,
                dynamodb_policy_statement, ssm_policy_statement, sqs_policy_statement
            ]
        )

        cloudwatch_metrics_policy = Policy(
            self,
            "PutCloudWatchMetricsPolicy",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
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

        process_lambda_s3_policy.attach_to_role(self._process_lambda.role)
        for lambda_function in lambda_functions:
            lambda_policy.attach_to_role(lambda_function.role)
            cloudwatch_metrics_policy.attach_to_role(lambda_function.role)

    def _create_state_machine(self) -> None:
        definition = {
            "Comment": "Simple pseudo flow",
            "StartAt": "Try",
            "States": {
                "Try": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "Pre-update Comprehensive Catalogue",  # NOSONAR
                            "States": {
                                "Pre-update Comprehensive Catalogue": {
                                    "Type": "Task",
                                    "Resource": self._preupdate_lambda.function_arn,
                                    "Comment": "Pre-update Comprehensive Catalogue",
                                    "Next": "Execute Light Transformation"  # NOSONAR
                                },
                                "Execute Light Transformation": {
                                    "Type": "Task",
                                    "Resource": self._process_lambda.function_arn,
                                    "Comment": "Execute Light Transformation",
                                    "ResultPath": "$.body.processedKeys",
                                    "Next": "Post-update comprehensive Catalogue"  # NOSONAR
                                },
                                "Post-update comprehensive Catalogue": {
                                    "Type": "Task",
                                    "Resource": self._postupdate_lambda.function_arn,
                                    "Comment": "Post-update comprehensive Catalogue",
                                    "ResultPath": "$.statusCode",
                                    "End": True
                                }
                            }
                        }
                    ],
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "ResultPath": None,
                            "Next": "Error"
                        }
                    ],
                    "Next": "Done"
                },
                "Done": {
                    "Type": "Succeed"
                },
                "Error": {
                    "Type": "Task",
                    "Resource": self._error_lambda.function_arn,
                    "Comment": "Send Original Payload to DLQ",
                    "Next": "Failed"
                },
                "Failed": {
                    "Type": "Fail"
                }
            }
        }

        _log_group_name = f"/aws/vendedlogs/states/{Aws.STACK_NAME}-sdlf-light-{Fn.select(2, Fn.split('/', Aws.STACK_ID))}"
        _sfn_log_group = logs.LogGroup(
            self, 
            'sdlf-light-sm-a-log-group', 
            log_group_name=_log_group_name,
            retention=logs.RetentionDays.INFINITE
        )
        add_cfn_nag_suppressions(
            _sfn_log_group.node.default_child, 
            suppressions=[CfnNagSuppression("W86", "Log retention period set to INFINITE")]
        )

        sfn_role: Role = Role(
            self,
            "sdlf-light-sfn-job-role",
            assumed_by=ServicePrincipal("states.amazonaws.com"),
        )

        sfn_job_policy = ManagedPolicy(
            self,
            "sdlf-light-sfn-job-policy",
            roles=[sfn_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "lambda:InvokeFunction"
                        ],
                        resources=[
                            f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:{self.resource_prefix}-{self.team}-{self.pipeline}-*"
                        ],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "logs:CreateLogDelivery",
                            "logs:CreateLogStream",
                            "logs:GetLogDelivery",
                            "logs:UpdateLogDelivery",
                            "logs:DeleteLogDelivery",
                            "logs:ListLogDeliveries",
                            "logs:PutLogEvents",
                            "logs:PutResourcePolicy",
                            "logs:DescribeResourcePolicies",
                            "logs:DescribeLogGroups"
                        ],
                        resources=["*"],  # NOSONAR
                    )
                ]
            ),
        )
        add_cfn_nag_suppressions(
            sfn_job_policy.node.default_child,
            [
                CfnNagSuppression(rule_id="W13", reason="IAM managed policy should not allow * resource")
            ]
        )

        self.sm_a = sfn.CfnStateMachine(
            self,
            'sdlf-light-sm-a',
            role_arn=sfn_role.role_arn,
            definition_string=json.dumps(definition, indent=4),
            state_machine_name=f"{self.resource_prefix}-{self.team}-{self.pipeline}-sm-a",
            logging_configuration=sfn.CfnStateMachine.LoggingConfigurationProperty(
                destinations=[sfn.CfnStateMachine.LogDestinationProperty(
                    cloud_watch_logs_log_group=sfn.CfnStateMachine.CloudWatchLogsLogGroupProperty(
                        log_group_arn=_sfn_log_group.log_group_arn
                    )
                )],
                include_execution_data=False,
                level="ALL",
            )
        )

        StringParameter(
            self,
            "sdlf-light-sm-a-arn",
            parameter_name=f"/{self.resource_prefix}/SM/{self.team}/{self.pipeline}StageASM",
            simple_name=True,
            string_value=self.sm_a.attr_arn,
        )

    def _suppress_cfn_nag_warnings(self):
        Aspects.of(self).add(
            CfnNagSuppressAll(
                [
                    CfnNagSuppression(rule_id="W76", reason="SPCM for IAM policy document is higher than 25"),
                    CfnNagSuppression(rule_id="W12", reason="IAM policy should not allow * resource")
                ],
                "AWS::IAM::Policy"
            )
        )

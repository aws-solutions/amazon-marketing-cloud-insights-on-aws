# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import json
from aws_cdk.aws_lambda import Code, LayerVersion, Runtime
from aws_cdk import aws_stepfunctions as sfn, Aspects
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk import Aws, Duration, Fn
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
import aws_cdk.aws_logs as logs
from aws_cdk.aws_ssm import StringParameter
from constructs import Construct
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.cfn_nag import CfnNagSuppression, CfnNagSuppressAll, add_cfn_nag_suppressions
from data_lake.register.register_construct import RegisterConstruct
from data_lake.foundations.foundations_construct import FoundationsConstruct


@dataclass
class SDLFHeavyTransformConfig:
    team: str
    pipeline: str


class SDLFHeavyTransform(Construct):
    def __init__(
            self,
            scope,
            resource_prefix: str,
            id: str,
            config: SDLFHeavyTransformConfig,
            props: Dict[str, Any],
            foundations_resources: FoundationsConstruct,
            **kwargs: Any,
    ) -> None:
        super().__init__(scope, id)

        self._config: SDLFHeavyTransformConfig = config
        self._props: Dict[str, Any] = props
        self.resource_prefix = resource_prefix
        self._foundations_resources = foundations_resources

        RegisterConstruct(self, self._props["id"], props=self._props,
                          register_lambda=self._foundations_resources.register_function)

        self.team = self._config.team
        self.pipeline = self._config.pipeline

        self._create_lambdas(self.team, self.pipeline)

        self._create_state_machine(name=f"{self.resource_prefix}-{self.team}-{self.pipeline}-sm-b")

        self._suppress_cfn_nag_warnings()

    def _create_lambdas(self, team, pipeline) -> None:
        lambda_handler = "handler.lambda_handler"

        self._routing_lambda = lambda_.Function(
            self,
            "routing-b",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-routing-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent}", "lambdas/routing")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "RESOURCE_PREFIX": self.resource_prefix,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            description="Triggers Step Function stageB",
            timeout=Duration.minutes(1),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            runtime=Runtime.PYTHON_3_9,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="routing-b-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-routing-b-lambda-alarm",
            lambda_function=self._routing_lambda
        )

        self._redrive_lambda = lambda_.Function(
            self,
            "redrive-b",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-redrive-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent}", "lambdas/redrive")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageB",
                "RESOURCE_PREFIX": self.resource_prefix,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            description="Redrive Step Function stageB",
            timeout=Duration.minutes(1),
            memory_size=256,
            runtime=Runtime.PYTHON_3_9,
            architecture=lambda_.Architecture.ARM_64,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="redrive-b-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-redrive-b-lambda-alarm",
            lambda_function=self._redrive_lambda
        )

        self._postupdate_lambda = lambda_.Function(
            self,
            "postupdate-b",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-postupdate-b",
            code=Code.from_asset(
                os.path.join(f"{Path(__file__).parent}", "lambdas/postupdate_metadata")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "RESOURCE_PREFIX": self.resource_prefix,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            description="post update metadata",
            timeout=Duration.minutes(1),
            memory_size=256,
            runtime=Runtime.PYTHON_3_9,
            architecture=lambda_.Architecture.ARM_64,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="postupdate-b-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-postupdate-b-lambda-alarm",
            lambda_function=self._postupdate_lambda
        )

        self._check_job_lambda = lambda_.Function(
            self,
            "checkjob-b",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-checkjob-b",
            code=Code.from_asset(
                os.path.join(f"{Path(__file__).parent}", "lambdas/check_job")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "RESOURCE_PREFIX": self.resource_prefix,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            description="check if glue job still running",
            timeout=Duration.minutes(1),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            runtime=Runtime.PYTHON_3_9,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="checkjob-b-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-checkjob-b-lambda-alarm",
            lambda_function=self._check_job_lambda
        )

        self._error_lambda = lambda_.Function(
            self,
            "error-b",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-error-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent}", "lambdas/error")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "RESOURCE_PREFIX": self.resource_prefix,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            description="send errors to DLQ",
            timeout=Duration.minutes(1),
            memory_size=256,
            runtime=Runtime.PYTHON_3_9,
            architecture=lambda_.Architecture.ARM_64,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="error-b-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-error-b-lambda-alarm",
            lambda_function=self._error_lambda
        )

        self._process_lambda = lambda_.Function(
            self,
            "process-b",
            function_name=f"{self.resource_prefix}-{team}-{pipeline}-process-b",
            code=Code.from_asset(
                os.path.join(f"{Path(__file__).parent}", "lambdas/process_object")),
            handler=lambda_handler,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "RESOURCE_PREFIX": self.resource_prefix,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            description="execute heavy transform",
            timeout=Duration.minutes(15),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            runtime=Runtime.PYTHON_3_9,
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="process-b-lambda-alarm",
            alarm_name=f"{self.resource_prefix}-{team}-{pipeline}-process-b-lambda-alarm",
            lambda_function=self._process_lambda
        )

        self._foundations_resources.stage_bucket_key.grant_decrypt(self._process_lambda)
        self._foundations_resources.stage_bucket.grant_read(self._process_lambda)
        self._foundations_resources.stage_bucket_key.grant_encrypt(self._process_lambda)
        self._foundations_resources.stage_bucket.grant_write(self._process_lambda)

        self._process_lambda.node.add_dependency(self._foundations_resources.wrangler_layer)
        self._check_job_lambda.node.add_dependency(self._foundations_resources.wrangler_layer)

        lambda_functions = [self._routing_lambda, self._postupdate_lambda, self._check_job_lambda,
                            self._process_lambda, self._error_lambda, self._redrive_lambda]

        self._add_layers(lambda_functions)

        self._attach_policy_to_lambda_roles(team, pipeline, lambda_functions)

    def _add_layers(self, lambda_functions):
        self._process_lambda.add_layers(self._foundations_resources.wrangler_layer)
        self._check_job_lambda.add_layers(self._foundations_resources.wrangler_layer)

        metrics_layer = LayerVersion(
            self,
            "metrics-layer",
            code=Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[3]}",
                    "aws_lambda_layers/metrics_layer/"
                )
            ),
            layer_version_name=f"{self.resource_prefix}-metrics-layer",
            compatible_runtimes=[Runtime.PYTHON_3_9],
        )

        data_lake_layer_version = LayerVersion.from_layer_version_arn(
            self,
            "layer-2",
            layer_version_arn=self._foundations_resources.data_lake_library_layer.layer_version_arn,
        )

        for _lambda_object in lambda_functions:
            _lambda_object.add_layers(data_lake_layer_version)
            _lambda_object.add_layers(SolutionsLayer.get_or_create(self))
            _lambda_object.add_layers(PowertoolsLayer.get_or_create(self))
            _lambda_object.add_layers(metrics_layer)

    def _attach_policy_to_lambda_roles(self, team, pipeline, lambda_functions):
        sm_b_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "states:StartExecution"
            ],
            resources=[
                f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:{self.resource_prefix}-{team}-{pipeline}-sm-b"],
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
                f"arn:aws:kms:{Aws.REGION}:{Aws.ACCOUNT_ID}:key/*"
            ],
            conditions={
                "ForAnyValue:StringLike": {
                    "kms:ResourceAliases": [f"alias/{self.resource_prefix}-*", "alias/tps-*"]
                }
            }
        )
        glue_crawler_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "glue:StartCrawler"
            ],
            resources=[
                f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:crawler/{self.resource_prefix}-{team}-*"],
        )

        glue_job_run_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "glue:StartJobRun",
                "glue:GetJobRun"
            ],
            resources=[f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:job/{self.resource_prefix}-{team}-*"],
        )

        s3_stage_bucket_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "s3:ListBucket"
            ],
            resources=[self._foundations_resources.stage_bucket.bucket_arn],
            conditions={
                "StringEquals": {
                    "aws:ResourceAccount": [
                        f"{Aws.ACCOUNT_ID}"
                    ]
                }
            }
        )

        s3_pre_and_post_stage_bucket_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "s3:GetObject"
            ],
            resources=[
                f"arn:aws:s3:::{self._foundations_resources.stage_bucket.bucket_name}/pre-stage/{team}/*",
                f"arn:aws:s3:::{self._foundations_resources.stage_bucket.bucket_name}/post-stage/{team}/*",
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

        # This policy grants permission to record metrics in CloudWatch.
        # This is needed for anonymous metrics.
        cloudwatch_policy_statement = PolicyStatement(
                effect=Effect.ALLOW,
                actions=["cloudwatch:PutMetricData"],
                resources=[
                    "*"
                ],
                conditions={"StringEquals": {
                    "cloudwatch:namespace": self.node.try_get_context("METRICS_NAMESPACE")}}
            )

        lambda_policy = iam.Policy(
            self,
            "sdlf-heavy-transform-lambdas-policy",
            statements=[
                sm_b_policy_statement, kms_policy_statement, glue_crawler_policy_statement,
                glue_job_run_policy_statement, s3_stage_bucket_policy_statement,
                s3_pre_and_post_stage_bucket_policy_statement, dynamodb_policy_statement,
                ssm_policy_statement, sqs_policy_statement, cloudwatch_policy_statement
            ]
        )

        cloudwatch_metrics_policy = iam.Policy(
            self,
            "PutCloudWatchMetricsPolicy1",
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
                ),
            ]
        )

        for _lambda_object in lambda_functions:
            lambda_policy.attach_to_role(_lambda_object.role)
            cloudwatch_metrics_policy.attach_to_role(_lambda_object.role)

    def _create_state_machine(self, name) -> None:
        definition = {
            "Comment": "Simple pseudo flow",
            "StartAt": "Try",
            "States": {
                "Try": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "Process Data",  # NOSONAR
                            "States": {
                                "Process Data": {
                                    "Type": "Task",
                                    "Resource": self._process_lambda.function_arn,
                                    "Comment": "Process Data",
                                    "ResultPath": "$.body.job",
                                    "Next": "Wait"
                                },
                                "Wait": {
                                    "Type": "Wait",
                                    "Seconds": 15,
                                    "Next": "Get Job status"
                                },
                                "Get Job status": {
                                    "Type": "Task",
                                    "Resource": self._check_job_lambda.function_arn,
                                    "ResultPath": "$.body.job",
                                    "Next": "Did Job finish?"
                                },
                                "Did Job finish?": {
                                    "Type": "Choice",
                                    "Choices": [{
                                        "Variable": "$.body.job.jobDetails.jobStatus",
                                        "StringEquals": "SUCCEEDED",
                                        "Next": "Post-update Comprehensive Catalogue"  # NOSONAR
                                    }, {
                                        "Variable": "$.body.job.jobDetails.jobStatus",
                                        "StringEquals": "FAILED",
                                        "Next": "Job Failed"  # NOSONAR
                                    }],
                                    "Default": "Wait"
                                },
                                "Job Failed": {
                                    "Type": "Fail",
                                    "Error": "Job Failed",
                                    "Cause": "Job failed, please check the logs"
                                },
                                "Post-update Comprehensive Catalogue": {
                                    "Type": "Task",
                                    "Resource": self._postupdate_lambda.function_arn,
                                    "Comment": "Post-update Comprehensive Catalogue",
                                    "ResultPath": "$.statusCode",
                                    "End": True
                                },
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

        _log_group_name = f"/aws/vendedlogs/states/{Aws.STACK_NAME}-sdlf-heavy-{Fn.select(2, Fn.split('/', Aws.STACK_ID))}"
        _sfn_log_group = logs.LogGroup(
            self, 
            'sdlf-heavy-sm-b-log-group', 
            log_group_name=_log_group_name,
            retention=logs.RetentionDays.INFINITE
            )

        sfn_role: Role = Role(
            self,
            "sdlf-heavy-sfn-job-role",
            assumed_by=ServicePrincipal("states.amazonaws.com"),
        )

        sfn_job_policy = ManagedPolicy(
            self,
            "sdlf-heavy-sfn-job-policy",
            roles=[sfn_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "lambda:InvokeFunction"
                        ],
                        resources=[
                            f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:{self.resource_prefix}-{self.team}-*"
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
                CfnNagSuppression(rule_id="W13", reason="IAM managed policy should not allow * resource"),
            ]
        )

        sm_b = sfn.CfnStateMachine(
            self,
            'sdlf-heavy-sm-b',
            role_arn=sfn_role.role_arn,
            definition_string=json.dumps(definition, indent=4),
            state_machine_name=f"{name}",
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
            "sdlf-heavy-sm-b-arn",
            parameter_name=f"/{self.resource_prefix}/SM/{self.team}/{self.pipeline}StageBSM",
            simple_name=True,
            string_value=sm_b.attr_arn,
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

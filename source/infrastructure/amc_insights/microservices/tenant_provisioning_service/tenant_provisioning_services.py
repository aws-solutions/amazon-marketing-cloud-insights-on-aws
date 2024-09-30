# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
import json
from aws_cdk.aws_kms import Key
from aws_cdk.aws_lambda import Code, LayerVersion, Runtime, Function, Architecture
from constructs import Construct
import aws_cdk as cdk
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal, Policy
import aws_cdk.aws_dynamodb as DDB
import aws_cdk.aws_logs as logs
from aws_cdk import Aws, CfnCondition, Aspects, Fn, CfnOutput

from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from amc_insights.condition_aspect import ConditionAspect
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression, CfnNagSuppressAll, add_cfn_guard_suppressions
from amc_insights.custom_resource.tenant_provisioning_service.amc_template_uploader import AMCTemplateUploader

AWS_RESOURCE_ACCOUNT_KEY = "aws:ResourceAccount"
LOGGING_SUPRESSION = [CfnNagSuppression(rule_id="W12", reason="IAM policy should not allow * resource")]


class TenantProvisioningService(Construct):
    def __init__(
            self,
            scope,
            id,
            team,
            dataset,
            workflow_manager_resources,
            creating_resources_condition: CfnCondition,
            solution_buckets,
            data_lake_enabled
    ) -> None:
        super().__init__(scope, id)

        # Apply condition to resources in TPS construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self.microservice_name = "tps"
        self._region = Aws.REGION
        self._team = team
        self._dataset = dataset
        self._resource_prefix = Aws.STACK_NAME
        self._workflow_manager_resources = workflow_manager_resources
        self._solution_buckets = solution_buckets
        self._data_lake_enabled = data_lake_enabled.value_as_string
        self._creating_resources_condition = creating_resources_condition
        self.lambda_function_list = []

        AMCTemplateUploader(
            self, "SyncAMCTemplate",
            self._solution_buckets,
            self.microservice_name,
            self._team,
            self._resource_prefix,
        )

        self.create_kms_key()

        self._customer_config_ddb = self._create_ddb_table(ddb_name="tps-CustomerConfig")

        self._create_lamda_layer()

        self.create_amc_onboarding_sm()

        self.create_invoke_tps_initialize_sm_lambda()

        self.add_cloudwatch_metric_policy_to_lambdas(self.lambda_function_list)

        self._suppress_cfn_nag_warnings()

    ######################################
    #              KMS                   #
    ######################################
    def create_kms_key(self):
        self.tps_kms_key = Key(
            self,
            id="tps-master-key",
            description="TPS Service Master Key",
            alias=f"alias/{self._resource_prefix}-tps-{self._team}-master-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

    ######################################
    #        Dynamo DB                   #
    ######################################

    def _create_ddb_table(self, ddb_name):
        ddb_props = {
            "partition_key": DDB.Attribute(name="customerId", type=DDB.AttributeType.STRING),
            "sort_key": DDB.Attribute(name="customerName", type=DDB.AttributeType.STRING)
        }

        table: DDB.Table = DDB.Table(
            self,
            f"{ddb_name}",
            encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.tps_kms_key,
            stream=DDB.StreamViewType.NEW_AND_OLD_IMAGES,
            billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
            **ddb_props,
        )

        CfnOutput(
            self,
            "TPSCustomerConfigTable",
            description="Use this link to access the TPS Customer Config table",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/dynamodbv2/home?region={Aws.REGION}#table?name={table.table_name}",
            condition=self._creating_resources_condition
        )

        return table

    ######################################
    #           Lambda Layer             #
    ######################################

    def _create_lamda_layer(self):
        self.powertools_layer = PowertoolsLayer.get_or_create(self)
        self.metrics_layer = LayerVersion(
            self,
            "metrics-layer",
            code=Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[3]}",
                    "aws_lambda_layers/metrics_layer/"
                )
            ),
            layer_version_name=f"{self._resource_prefix}-metrics-layer",
            compatible_runtimes=[Runtime.PYTHON_3_11],
        )

    ######################################
    #           Lambdas                  #
    ######################################

    def create_invoke_tps_initialize_sm_lambda(self):
        self.lambda_invoke_tps_initialize_sm = Function(
            self,
            "InvokeTPSInitializeSM",
            function_name=f"{self._resource_prefix}-tps-InvokeTPSInitializeSM",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}",
                                              "tenant_provisioning_service/lambdas/InvokeTPSInitializeSM")),
            handler="handler.handler",  # NOSONAR
            description="Triggers the AMC Instance Setup state machine",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            runtime=Runtime.PYTHON_3_11,
            architecture=Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STATE_MACHINE_ARN": self._sm.attr_arn,
                "DATASET_NAME": self._dataset,
                "TEAM_NAME": self._team,
                "APPLICATION_ACCOUNT": Aws.ACCOUNT_ID,
                "DEFAULT_SNS_TOPIC": self._workflow_manager_resources.sns_topic.topic_arn,
                "RESOURCE_PREFIX": Aws.STACK_NAME,
                "STACK_NAME": Aws.STACK_NAME,
                "APPLICATION_REGION": Aws.REGION,
            },
            layers=[self.powertools_layer, SolutionsLayer.get_or_create(self), self.metrics_layer]
        )

        self.lambda_function_list.append(self.lambda_invoke_tps_initialize_sm)

        self.lambda_invoke_tps_initialize_sm.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["states:StartExecution"],
                resources=[
                    f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:{self._resource_prefix}-tps-*"
                ],
            )
        )
        self.lambda_invoke_tps_initialize_sm.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "sqs:List*",
                    "sqs:ReceiveMessage",
                    "sqs:SendMessage*",
                    "sqs:DeleteMessage*",
                    "sqs:GetQueue*"
                ],
                resources=[
                    f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._resource_prefix}-tps-*"
                ],
            )
        )

    def create_amc_onboarding_sm(self):
        add_amc_instance_role = Role(
            self,
            "Add AMC Instance Role",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
        )
        add_amc_instance_allow_policy = Policy(
            self,
            "add-amc-instance-allow-policy",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:CreateBucket",  # NOSONAR
                        "s3:ListBucket",  # NOSONAR
                        "s3:PutBucketPolicy",  # NOSONAR
                        "s3:PutBucketAcl",
                        "s3:PutBucketPublicAccessBlock",
                        "s3:PutAccountPublicAccessBlock",
                        "s3:GetAccountPublicAccessBlock",
                        "s3:GetBucketPublicAccessBlock",
                        "s3:PutBucketNotification",
                        "s3:PutBucketTagging",
                        "s3:GetBucketAcl",
                        "s3:GetBucketNotification",
                        "s3:GetEncryptionConfiguration",
                        "s3:PutEncryptionConfiguration",
                        "s3:GetBucketPolicy",
                        "s3:GetBucketPolicyStatus",
                        "s3:DeleteBucketPolicy",
                        "s3:DeleteBucket",
                        "s3:GetBucketOwnershipControls",
                        "s3:PutBucketOwnershipControls",
                        "s3:GetObject",  # NOSONAR
                        "s3:PutObject",  # NOSONAR
                        "s3:PutBucketVersioning",
                    ],
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            AWS_RESOURCE_ACCOUNT_KEY: [
                                f"{Aws.ACCOUNT_ID}"
                            ]
                        }
                    }
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject"
                    ],
                    resources=[f"arn:aws:s3:::*/{self._resource_prefix}/*"],
                    conditions={
                        "StringEquals": {
                            AWS_RESOURCE_ACCOUNT_KEY: [
                                f"{Aws.ACCOUNT_ID}"
                            ]
                        }
                    }
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:CreateKey"
                    ],
                    resources=["*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:TagResource",  # NOSONAR
                        "kms:CreateAlias",  # NOSONAR
                        "kms:UpdateAlias",  # NOSONAR
                        "kms:DescribeKey",
                        "kms:PutKeyPolicy",  # NOSONAR
                        "kms:ScheduleKeyDeletion",  # NOSONAR
                        "kms:DeleteAlias"
                    ],
                    resources=[
                        f"arn:aws:kms:*:{cdk.Aws.ACCOUNT_ID}:key/*",
                        f"arn:aws:kms:*:{cdk.Aws.ACCOUNT_ID}:alias/*"
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:Decrypt",
                        "kms:GenerateDataKey",
                        "kms:GetKeyPolicy"
                    ],
                    resources=[
                        self._solution_buckets.artifacts_bucket_key.key_arn,
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "iam:PassRole"
                    ],
                    resources=[
                        f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:role/service-role/{self._resource_prefix}*",
                        f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:role/{self._resource_prefix}*"
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "cloudformation:GetTemplate",
                        "cloudformation:GetTemplateSummary",
                        "cloudformation:ListStacks",
                        "cloudformation:ValidateTemplate",
                        "cloudformation:CreateChangeSet",
                        "cloudformation:CreateStack",
                        "cloudformation:DeleteChangeSet",
                        "cloudformation:DeleteStack",
                        "cloudformation:DescribeChangeSet",
                        "cloudformation:DescribeStacks",
                        "cloudformation:ExecuteChangeSet",
                        "cloudformation:SetStackPolicy",
                        "cloudformation:UpdateStack",
                        "cloudformation:DescribeStackResource"
                    ],
                    resources=[
                        f"arn:aws:cloudformation:*:{cdk.Aws.ACCOUNT_ID}:stack/{self._resource_prefix}*",
                        f"arn:aws:cloudformation:*:{cdk.Aws.ACCOUNT_ID}:stack/tps*",
                        f"arn:aws:cloudformation:{cdk.Aws.REGION}:aws:transform/*"
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "events:Put*",
                        "events:Create*",
                        "events:List*",
                        "events:Describe*",
                        "events:EnableRule",
                        "events:ActivateEventSource",
                        "events:DeactivateEventSource",
                        "events:DeleteRule",
                        "events:RemoveTargets",
                        "events:RemovePermission"
                    ],
                    resources=[
                        f"arn:aws:events:*:{Aws.ACCOUNT_ID}:event-bus/default",
                        f"arn:aws:events:*:{Aws.ACCOUNT_ID}:rule/*"
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "iam:GetPolicyVersion",
                        "iam:CreateRole",
                        "iam:PutRolePolicy",
                        "iam:CreatePolicyVersion",
                        "iam:GetRole",
                        "iam:GetPolicy",
                        "iam:CreatePolicy",
                        "iam:UpdateRole",
                        "iam:GetRolePolicy",
                        "iam:DeleteRolePolicy",
                        "iam:DeleteRole"
                    ],
                    resources=[
                        f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:role/{self._resource_prefix}*",
                        f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:policy/*"
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "sns:GetTopicAttributes",
                        "sns:CreateTopic",
                        "sns:SetTopicAttributes",
                        "sns:TagResource"
                    ],
                    resources=[
                        f"arn:aws:sns:{Aws.REGION}:{Aws.ACCOUNT_ID}:{Aws.STACK_NAME}*"
                    ],
                ),
            ]
        )
        # deny role ability to change its own permissions
        add_amc_instance_deny_policy = Policy(
            self,
            "add-amc-instance-deny-policy",
            statements=[
                PolicyStatement(
                    effect=Effect.DENY,
                    actions=[
                        "iam:*"
                    ],
                    resources=[
                        f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:role/{add_amc_instance_role.role_name}*",
                        f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:policy/{add_amc_instance_allow_policy.policy_name}"
                    ],
                )
            ]
        )
        lambda_basic_role_policy = Policy(
            self,
            "lambda-basic-role-policy",
            statements=[
                PolicyStatement(
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources=[
                        f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/*"
                    ],
                    effect=Effect.ALLOW,
                ),
            ]
        )
        add_amc_instance_role.attach_inline_policy(lambda_basic_role_policy)
        add_amc_instance_role.attach_inline_policy(add_amc_instance_allow_policy)
        add_amc_instance_role.attach_inline_policy(add_amc_instance_deny_policy)
        add_cfn_nag_suppressions(
            add_amc_instance_role.node.default_child,
            [CfnNagSuppression(rule_id="W11", reason="IAM role should not allow * resource on its permissions policy")]
        )

        self.add_amc_instance = Function(
            self,
            "AddAmcInstance",
            function_name=f"{self._resource_prefix}-tps-AddAmcInstance",
            code=Code.from_asset(
                os.path.join(f"{Path(__file__).parents[1]}", "tenant_provisioning_service/lambdas/AddAMCInstance")),
            handler="handler.handler",
            description="Creates/Updates TPS customer stacks",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime=Runtime.PYTHON_3_11,
            architecture=Architecture.ARM_64,
            role=add_amc_instance_role,
            environment={
                "TEMPLATE_URL": f"https://{self._solution_buckets.artifacts_bucket.bucket_name}.s3.amazonaws.com/{self.microservice_name}/scripts/{self._team}/scripts",
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "ADD_AMC_INSTANCE_LAMBDA_ROLE_ARN": add_amc_instance_role.role_arn,
                "RESOURCE_PREFIX": Aws.STACK_NAME,
                "DATA_LAKE_ENABLED": str(self._data_lake_enabled),
                "SNS_KMS_KEY_ID": self._workflow_manager_resources.kms_key.key_id,
                "APPLICATION_ACCOUNT": Aws.ACCOUNT_ID,
                "APPLICATION_REGION": Aws.REGION,
                "LOGGING_BUCKET_NAME": self._solution_buckets.logging_bucket.bucket_name,
                "ARTIFACTS_BUCKET_NAME": self._solution_buckets.artifacts_bucket.bucket_name,
                "ARTIFACTS_BUCKET_KEY_ID": self._solution_buckets.artifacts_bucket_key.key_id,
                "ROUTING_QUEUE_LOGICAL_ID": 'datalakepipelinedatalakeroutingBEE8F1BC',
                "STAGE_A_ROLE_LOGICAL_ID": 'datalakepipelinesdlfstageaprocessaServiceRole15419483',
            },
            layers=[self.powertools_layer, SolutionsLayer.get_or_create(self), self.metrics_layer]
        )

        self.add_amc_instance_check = Function(
            self,
            "AddAmcInstanceStatusCheck",
            function_name=f"{self._resource_prefix}-tps-AddAmcInstanceStatusCheck",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}",
                                              "tenant_provisioning_service/lambdas/AddAMCInstanceCheck")),
            handler="handler.handler",
            description="Checks if TPS customer stacks have finished (success/failure)",
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            runtime=Runtime.PYTHON_3_11,
            architecture=Architecture.ARM_64,
            role=add_amc_instance_role,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": Aws.STACK_NAME,
            },
            layers=[self.powertools_layer, SolutionsLayer.get_or_create(self), self.metrics_layer]
        )

        self.amc_instance_post_deploy_metadata = Function(
            self,
            "postDeployMetadataInstanceConfig",
            function_name=f"{self._resource_prefix}-tps-postDeployMetadataInstanceConfig",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}",
                                              "tenant_provisioning_service/lambdas/AMCInstancePostDeployMetadata")),
            handler="handler.handler",
            description="Adds TPS customer information to SDLF and WFM DynamoDB tables",
            timeout=cdk.Duration.minutes(10),
            memory_size=512,
            runtime=Runtime.PYTHON_3_11,
            architecture=Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "Region": cdk.Aws.REGION,
                "DATA_LAKE_ENABLED": str(self._data_lake_enabled),
                "WFM_CUSTOMER_CONFIG_TABLE": self._workflow_manager_resources.dynamodb_customer_config_table.table_name,
                "RESOURCE_PREFIX": self._resource_prefix,
                "AWS_ACCOUNT_ID": Aws.ACCOUNT_ID,
                "TPS_CUSTOMER_CONFIG_TABLE": self._customer_config_ddb.table_name,
                "APPLICATION_REGION": Aws.REGION,
                "SDLF_CUSTOMER_CONFIG_LOGICAL_ID": "foundationssdlfCustomerConfig45371CE6",
                "LOGGING_BUCKET_NAME": self._solution_buckets.logging_bucket.bucket_name,
            },
            layers=[self.powertools_layer, SolutionsLayer.get_or_create(self), self.metrics_layer]
        )

        self.lambda_function_list.extend(
            [self.add_amc_instance, self.add_amc_instance_check, self.amc_instance_post_deploy_metadata])

        self._customer_config_ddb.grant_read_write_data(self.amc_instance_post_deploy_metadata)
        self._workflow_manager_resources.dynamodb_customer_config_table.grant_read_write_data(
            self.amc_instance_post_deploy_metadata)

        amc_instance_post_deploy_metadata_policy = Policy(
            self, "postDeployMetadataInstanceConfigPolicy",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:DescribeKey",
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:CreateGrant*"
                    ],
                    resources=[
                        self.tps_kms_key.key_arn,
                        self._workflow_manager_resources.kms_key.key_arn,
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "cloudformation:DescribeStackResource",
                        "cloudformation:DescribeStacks"
                    ],
                    resources=[
                        f"arn:aws:cloudformation:*:{cdk.Aws.ACCOUNT_ID}:stack/{self._resource_prefix}*",
                    ]
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:GetBucketLogging",
                        "s3:PutBucketLogging",
                        "s3:PutBucketPolicy",
                        "s3:PutBucketNotification",
                        "s3:GetBucketNotification"
                    ],
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            AWS_RESOURCE_ACCOUNT_KEY: [
                                f"{Aws.ACCOUNT_ID}"
                            ]
                        }
                    }
                )
            ]
        )
        amc_instance_post_deploy_metadata_policy.attach_to_role(self.amc_instance_post_deploy_metadata.role)

        add_cfn_nag_suppressions(
            amc_instance_post_deploy_metadata_policy.node.default_child,
            LOGGING_SUPRESSION
        )
        add_cfn_guard_suppressions(
            resource=amc_instance_post_deploy_metadata_policy.node.default_child,
            suppressions=["IAM_POLICY_NON_COMPLIANT_ARN"]
        )
        # Suppression Reason: S3 Bucket arns do not follow arn:partition:service:region:account-id format

        definition = {
            "Comment": "Simple pseudo flow",
            "StartAt": "Try",
            "States": {
                "Try": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "Process AMC Instance Request",  # NOSONAR
                            "States": {
                                "Process AMC Instance Request": {
                                    "Type": "Task",
                                    "Resource": self.add_amc_instance.function_arn,
                                    "Comment": "Process AMC Instance Request",
                                    "ResultPath": "$.body.stackId",
                                    "Next": "Wait"
                                },
                                "Wait": {
                                    "Type": "Wait",
                                    "Seconds": 45,
                                    "Next": "Get Stack status"
                                },
                                "Get Stack status": {
                                    "Type": "Task",
                                    "Resource": self.add_amc_instance_check.function_arn,
                                    "ResultPath": "$.body.stackStatus",  # NOSONAR
                                    "Next": "Did Job finish?"
                                },
                                "Did Job finish?": {
                                    "Type": "Choice",
                                    "Choices": [{
                                        "Variable": "$.body.stackStatus",
                                        "StringEquals": "CREATE_COMPLETE",
                                        "Next": "Post-deploy update config tables"  # NOSONAR
                                    }, {
                                        "Variable": "$.body.stackStatus",
                                        "StringEquals": "UPDATE_COMPLETE",
                                        "Next": "Post-deploy update config tables"
                                    }, {
                                        "Variable": "$.body.stackStatus",
                                        "StringEquals": "FAILED",
                                        "Next": "Stack Failed"  # NOSONAR
                                    }],
                                    "Default": "Wait"
                                },
                                "Stack Failed": {
                                    "Type": "Fail",
                                    "Error": "Stack Failed",
                                    "Cause": "Stack failed, please check the logs"
                                },
                                "Post-deploy update config tables": {
                                    "Type": "Task",
                                    "Resource": self.amc_instance_post_deploy_metadata.function_arn,
                                    "Comment": "Post-deploy update config tables",
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
                            "Next": "Failed"
                        }
                    ],
                    "Next": "Done"
                },
                "Done": {
                    "Type": "Succeed"
                },
                "Failed": {
                    "Type": "Fail"
                }
            }
        }

        _log_group_name = f"/aws/vendedlogs/states/{Aws.STACK_NAME}-tps-initialize-amc-{Fn.select(2, Fn.split('/', Aws.STACK_ID))}"
        _sfn_log_group = logs.LogGroup(
            self,
            'tps-initialize-amc-log-group',
            log_group_name=_log_group_name,
            retention=logs.RetentionDays.INFINITE
        )

        name = "tps-initialize-amc"
        sfn_role: Role = Role(
            self,
            f"{name}-sfn-job-role",
            assumed_by=ServicePrincipal("states.amazonaws.com"),
        )

        sfn_job_policy = ManagedPolicy(
            self,
            f"{name}-sfn-job-policy",
            roles=[sfn_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "lambda:InvokeFunction"
                        ],
                        resources=[
                            f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{self._resource_prefix}-*"
                        ],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "states:DescribeExecution",
                            "states:StopExecution"
                        ],
                        resources=[
                            f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:{self._resource_prefix}-tps-*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "events:DescribeRule",
                            "events:PutTargets",
                            "events:PutRule"
                        ],
                        resources=[
                            f"arn:aws:events:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule",
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
            )
        )

        add_cfn_nag_suppressions(
            sfn_job_policy.node.default_child,
            [CfnNagSuppression(rule_id="W13", reason="IAM managed policy should not allow * resource")]
        )

        self._sm = sfn.CfnStateMachine(
            self,
            name,
            role_arn=sfn_role.role_arn,
            definition_string=json.dumps(definition, indent=4),
            state_machine_name=f"{self._resource_prefix}-{name}",
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

    def add_cloudwatch_metric_policy_to_lambdas(self, lambda_function_list):
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

        add_cfn_nag_suppressions(
            cloudwatch_metrics_policy.node.default_child,
            LOGGING_SUPRESSION
        )

        for lambda_function in lambda_function_list:
            cloudwatch_metrics_policy.attach_to_role(lambda_function.role)

    def _suppress_cfn_nag_warnings(self):
        Aspects.of(self).add(
            CfnNagSuppressAll(
                LOGGING_SUPRESSION,
                "AWS::IAM::Policy"
            )
        )

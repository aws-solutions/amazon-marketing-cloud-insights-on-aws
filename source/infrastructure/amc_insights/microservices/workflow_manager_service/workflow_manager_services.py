# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
from aws_cdk.aws_lambda import LayerVersion
from aws_cdk.aws_iam import Effect, PolicyStatement, ServicePrincipal, Policy, Role, AccountRootPrincipal, \
    PolicyDocument
import aws_cdk as cdk
from aws_cdk.aws_sns import Topic
from aws_cdk import aws_dynamodb as dynamodb
import aws_cdk.aws_kms as kms
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_logs as logs
import aws_cdk.aws_secretsmanager as secrets_manager
from aws_cdk.aws_sns_subscriptions import EmailSubscription
from constructs import Construct
from aws_cdk import Aws, Aspects, CfnCondition, Fn, CfnOutput, SecretValue, RemovalPolicy, Duration
from aws_cdk import aws_stepfunctions as stepfunctions
from aws_cdk import aws_stepfunctions_tasks as tasks
from amc_insights.condition_aspect import ConditionAspect
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.cfn_nag import CfnNagSuppression, CfnNagSuppressAll
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer

CONCURRENT_WORKFLOW_EXECUTION_LIMIT = 10


class WorkFlowManagerService(Construct):

    def __init__(
            self,
            scope: Construct,
            id: str,
            team: str,
            email_parameter,
            creating_resources_condition: CfnCondition,
    ) -> None:
        super().__init__(scope, id)

        # Apply condition to resources in WFM construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._suppress_cfn_nag_warnings()

        self._region = Aws.REGION
        self._team = team
        self._resource_prefix = Aws.STACK_NAME
        self._sns_email = email_parameter.value_as_string

        ##########################
        #    OAUTH RESOURCES     #
        ##########################
        amc_secrets_manager_key_policy_document = PolicyDocument(
            statements=[
                PolicyStatement(
                    sid="Allow administration of the key",
                    effect=Effect.ALLOW,
                    principals=[AccountRootPrincipal()],
                    actions=["kms:*"],
                    resources=["*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    principals=[AccountRootPrincipal()],
                    actions=["kms:Decrypt",
                             "kms:Encrypt",
                             "kms:ReEncrypt*",
                             "kms:GenerateDataKey*"
                             ],
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            "kms:CallerAccount": Aws.ACCOUNT_ID,
                            "kms:ViaService": f"secretsmanager.{Aws.REGION}.amazonaws.com",
                        },
                        "StringLike": {
                            "kms:EncryptionContext:SecretARN": [
                                f"arn:aws:secretsmanager:{Aws.REGION}:{Aws.ACCOUNT_ID}:secret:{self._resource_prefix}*"
                            ]
                        }
                    }
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    principals=[AccountRootPrincipal()],
                    actions=["kms:CreateGrant",
                             "kms:DescribeKey"
                             ],
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            "kms:CallerAccount": Aws.ACCOUNT_ID,
                            "kms:ViaService": f"secretsmanager.{Aws.REGION}.amazonaws.com"
                        },
                        "StringLike": {
                            "kms:EncryptionContext:SecretARN": [
                                f"arn:aws:secretsmanager:{Aws.REGION}:{Aws.ACCOUNT_ID}:secret:{self._resource_prefix}*"
                            ]
                        }
                    }
                ),
            ]
        )

        self.amc_secrets_manager_key = kms.Key(
            self,
            "SecretKey",
            removal_policy=RemovalPolicy.DESTROY,
            description="OAuth Creds Secrets Manager Key",
            enable_key_rotation=True,
            policy=amc_secrets_manager_key_policy_document,
            pending_window=Duration.days(30),
        )
        self.amc_secrets_manager = secrets_manager.Secret(
            self,
            "Secret",
            encryption_key=self.amc_secrets_manager_key,
            removal_policy=RemovalPolicy.DESTROY,
            secret_object_value={
                "client_id": SecretValue.plain_text(""),
                "client_secret": SecretValue.plain_text(""),
                "authorization_code": SecretValue.plain_text(""),
                "refresh_token": SecretValue.plain_text(""),
                "access_token": SecretValue.plain_text("")
            },
        )
        CfnOutput(
            self,
            "AMCSecrets",
            description="Use this link to access the Secrets Manager",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/secretsmanager/secret?name={self.amc_secrets_manager.secret_name}&region={Aws.REGION}",
            condition=creating_resources_condition,
        )

        ##################################
        #            KMS                 #
        ##################################

        self.kms_key = kms.Key(
            self,
            id="wfm-master-key",
            description="WFM Service Master Key",
            alias=f"alias/{self._resource_prefix}-wfm-{self._team}-master-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        ##################################
        #            SNS                 #
        ##################################

        self.kms_key.add_to_resource_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey",
                    "kms:GenerateDataKeyPair",
                    "kms:GenerateDataKeyPairWithoutPlaintext",
                    "kms:GenerateDataKeyWithoutPlaintext",
                    "kms:ReEncryptTo",
                    "kms:ReEncryptFrom",
                    "kms:ListAliases",
                    "kms:ListGrants",
                    "kms:ListKeys",
                    "kms:ListKeyPolicies"
                ],
                resources=["*"],
                principals=[ServicePrincipal("sns.amazonaws.com")],
                conditions={
                    "StringLike": {
                        "kms:EncryptionContext:aws:sns:arn": [
                            f"arn:aws:sns:{Aws.REGION}:{Aws.ACCOUNT_ID}:{self._resource_prefix}-wfm-SNSTopic"
                        ]
                    }
                }
            )
        )
        self.sns_topic = Topic(
            self,
            "wfm-SNSTopic",
            topic_name=f"{self._resource_prefix}-wfm-SNSTopic",
            master_key=self.kms_key
        )
        self.sns_topic.add_subscription(EmailSubscription(self._sns_email))

        ##################################
        #         DynamoDB               #
        ##################################

        self.dynamodb_customer_config_table = dynamodb.Table(
            self,
            'wfm-CustomerConfig',
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.kms_key,
            partition_key=dynamodb.Attribute(
                name='customerId',
                type=dynamodb.AttributeType.STRING
            ),
            point_in_time_recovery=True,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        self.dynamodb_customer_config_table_output = CfnOutput(
            self,
            "WFMCustomerConfigTable",
            description="Use this link to access the WFM Customer Config table",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/dynamodbv2/home?region={Aws.REGION}#table?name={self.dynamodb_customer_config_table.table_name}",
            condition=creating_resources_condition
        )

        self.dynamodb_workflows_table = dynamodb.Table(
            self,
            'wfm-Workflows',
            partition_key=dynamodb.Attribute(
                name="customerId",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="workflowId",
                type=dynamodb.AttributeType.STRING
            ),
            point_in_time_recovery=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            encryption_key=self.kms_key,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        self.dynamodb_workflows_table_output = CfnOutput(
            self,
            "WFMWorkflowsTable",
            description="Use this link to access the WFM Workflows table",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/dynamodbv2/home?region={Aws.REGION}#table?name={self.dynamodb_workflows_table.table_name}",
            condition=creating_resources_condition
        )

        self.dynamodb_execution_status_table = dynamodb.Table(
            self,
            'wfm-WorkflowExecutions',
            partition_key=dynamodb.Attribute(
                name="customerId",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="workflowExecutionId",
                type=dynamodb.AttributeType.STRING
            ),
            point_in_time_recovery=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            encryption_key=self.kms_key,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        self.dynamodb_execution_status_table_ouput = CfnOutput(
            self,
            "WFMExecutionStatusTable",
            description="Use this link to access the WFM Execution Status table",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/dynamodbv2/home?region={Aws.REGION}#table?name={self.dynamodb_execution_status_table.table_name}",
            condition=creating_resources_condition
        )

        ######################################
        #           Lambda Layers            #
        ######################################

        self.powertools_layer = self.powertools_layer = PowertoolsLayer.get_or_create(self)

        self.wfm_layer = LayerVersion(
            self,
            "wfm-layer",
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambda_layers/wfm_layer/"
                )
            ),
            layer_version_name=f"{self._resource_prefix}-wfm-layer",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
        )
        self.metrics_layer = LayerVersion(
            self,
            "metrics-layer",
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[3]}",
                    "aws_lambda_layers/metrics_layer/"
                )
            ),
            layer_version_name=f"{self._resource_prefix}-metrics-layer",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
        )

        ######################################
        #           Lambdas                  #
        ######################################

        self.lambda_invoke_workflow_sm = _lambda.Function(
            self,
            'wfm-InvokeWorkflowSM',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/InvokeWorkflowSM"
                )
            ),
            description="Invokes the Workflow state machine",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",  # NOSONAR
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self._resource_prefix,
                "STACK_NAME": Aws.STACK_NAME
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ],
        )
        self.kms_key.grant_encrypt_decrypt(self.lambda_invoke_workflow_sm.role)

        self.lambda_invoke_workflow_execution_sm = _lambda.Function(
            self,
            'wfm-InvokeWorkflowExecutionSM',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/InvokeWorkflowExecutionSM"
                )
            ),
            description="Invokes the Workflow Executions state machine",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "RESOURCE_PREFIX": self._resource_prefix,
                "DATASET_WORKFLOW_TABLE": self.dynamodb_workflows_table.table_name,
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.lambda_invoke_workflow_execution_sm.add_permission(
            "wfm-allow-eventbridge-invoke",
            principal=ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:events:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:rule/*"
        )
        self.dynamodb_workflows_table.grant_read_data(self.lambda_invoke_workflow_execution_sm.role)
        self.dynamodb_workflows_table.grant(
            self.lambda_invoke_workflow_execution_sm.role,
            "dynamodb:DescribeTable"  # NOSONAR
        )
        self.kms_key.grant_encrypt_decrypt(self.lambda_invoke_workflow_execution_sm.role)

        self.lambda_check_workflow_execution_status = _lambda.Function(
            self,
            'wfm-CheckWorkflowExeuctionStatus',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/CheckWorkflowExecutionStatus"
                )
            ),
            description="Checks the status of workflow executions",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "EXECUTION_STATUS_TABLE": self.dynamodb_execution_status_table.table_name,
                "RESOURCE_PREFIX": self._resource_prefix,
                "REGION": Aws.REGION,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.dynamodb_execution_status_table.grant_read_write_data(
            self.lambda_check_workflow_execution_status.role)
        self.dynamodb_execution_status_table.grant(
            self.lambda_check_workflow_execution_status.role,
            "dynamodb:DescribeTable"
        )
        self.kms_key.grant_encrypt_decrypt(
            self.lambda_check_workflow_execution_status.role)

        self.lambda_get_execution_summary = _lambda.Function(
            self,
            'wfm-GetWorkflowExecutionSummary',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/GetExecutionSummary"
                )
            ),
            description="Gets a summary of workflow executions",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self._resource_prefix,
                "REGION": Aws.REGION,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.kms_key.grant_encrypt_decrypt(self.lambda_get_execution_summary.role)

        self.lambda_create_workflow_execution = _lambda.Function(
            self,
            'wfm-CreateWorkflowExecution',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/CreateWorkflowExecution"
                )
            ),
            description="Creates a new workflow execution",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.seconds(600),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "EXECUTION_STATUS_TABLE": self.dynamodb_execution_status_table.table_name,
                "RESOURCE_PREFIX": self._resource_prefix,
                "REGION": Aws.REGION,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.dynamodb_execution_status_table.grant_read_write_data(self.lambda_create_workflow_execution.role)
        self.dynamodb_execution_status_table.grant(
            self.lambda_create_workflow_execution.role,
            "dynamodb:DescribeTable"
        )
        self.kms_key.grant_encrypt_decrypt(self.lambda_create_workflow_execution.role)

        self.lambda_cancel_workflow_execution = _lambda.Function(
            self,
            'wfm-CancelWorkflowExecutionLambda',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/CancelWorkflowExecution"
                )
            ),
            description="Cancels a running workflow execution",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "EXECUTION_STATUS_TABLE": self.dynamodb_execution_status_table.table_name,
                "RESOURCE_PREFIX": self._resource_prefix,
                "REGION": Aws.REGION,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.dynamodb_execution_status_table.grant_read_write_data(self.lambda_cancel_workflow_execution.role)
        self.dynamodb_execution_status_table.grant(
            self.lambda_cancel_workflow_execution.role,
            "dynamodb:DescribeTable"
        )
        self.kms_key.grant_encrypt_decrypt(self.lambda_cancel_workflow_execution.role)

        self.lambda_create_workflow = _lambda.Function(
            self,
            'wfm-CreateWorkflow',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/CreateWorkflow"
                )
            ),
            description="Creates a new workflow",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "WORKFLOWS_TABLE_NAME": self.dynamodb_workflows_table.table_name,
                "REGION": Aws.REGION,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
                "RESOURCE_PREFIX": self._resource_prefix
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.dynamodb_workflows_table.grant_read_write_data(
            self.lambda_create_workflow.role)
        self.dynamodb_workflows_table.grant(
            self.lambda_create_workflow.role, "dynamodb:DescribeTable")
        self.kms_key.grant_encrypt_decrypt(self.lambda_create_workflow.role)

        self.lambda_update_workflow = _lambda.Function(
            self,
            'wfm-UpdateWorkflow',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/UpdateWorkflow"
                )
            ),
            description="Updates an existing workflow",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self._resource_prefix,
                "WORKFLOWS_TABLE_NAME": self.dynamodb_workflows_table.table_name,
                "REGION": Aws.REGION,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.dynamodb_workflows_table.grant_read_write_data(self.lambda_update_workflow.role)
        self.dynamodb_workflows_table.grant(
            self.lambda_update_workflow.role,
            "dynamodb:DescribeTable"
        )
        self.kms_key.grant_encrypt_decrypt(self.lambda_update_workflow.role)

        self.lambda_get_workflow = _lambda.Function(
            self,
            'wfm-GetWorkflow',
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/GetWorkflow"
                )
            ),
            description="Gets a workflow definition",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self._resource_prefix,
                "WORKFLOWS_TABLE_NAME": self.dynamodb_workflows_table.table_name,
                "REGION": Aws.REGION,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.dynamodb_workflows_table.grant_read_write_data(self.lambda_get_workflow.role)
        self.dynamodb_workflows_table.grant(
            self.lambda_get_workflow.role,
            "dynamodb:DescribeTable"
        )
        self.kms_key.grant_encrypt_decrypt(self.lambda_get_workflow.role)

        self.lambda_delete_workflow = _lambda.Function(
            self,
            'wfm-DeleteWorkflow',
            code=_lambda.Code.from_asset(
                os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/DeleteWorkflow"
                )
            ),
            description="Deletes a workflow",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "WORKFLOWS_TABLE_NAME": self.dynamodb_workflows_table.table_name,
                "RESOURCE_PREFIX": self._resource_prefix,
                "REGION": Aws.REGION,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )
        self.dynamodb_workflows_table.grant_read_write_data(self.lambda_delete_workflow.role)
        self.dynamodb_workflows_table.grant(
            self.lambda_delete_workflow.role,
            "dynamodb:DescribeTable"
        )
        self.kms_key.grant_encrypt_decrypt(self.lambda_delete_workflow.role)

        self.lambda_create_workflow_schedule = _lambda.Function(
            self,
            'wfm-CreateWorkflowSchedule',
            code=_lambda.Code.from_asset(
                os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/CreateWorkflowSchedule"
                )
            ),
            description="Creates a workflow schedule",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "INVOKE_WORKFLOW_EXECUTION_SM_LAMBDA_ARN": self.lambda_invoke_workflow_execution_sm.function_arn,
                "RULE_PREFIX": self._resource_prefix,
                "RESOURCE_PREFIX": self._resource_prefix
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )

        self.lambda_delete_workflow_schedule = _lambda.Function(
            self,
            'wfm-DeleteWorkflowSchedule',
            code=_lambda.Code.from_asset(
                os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/DeleteWorkflowSchedule"
                )
            ),
            description="Deletes a workflow schedule",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RULE_PREFIX": self._resource_prefix,
                "RESOURCE_PREFIX": self._resource_prefix
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )

        self.lambda_amc_auth = _lambda.Function(
            self,
            'wfm-AMCAuth',
            code=_lambda.Code.from_asset(
                os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "workflow_manager_service/lambdas/AMCAuth"
                )
            ),
            description="Runs AMC OAuth Flow",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            timeout=cdk.Duration.minutes(1),
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "RESOURCE_PREFIX": self._resource_prefix,
                "AMC_SECRETS_MANAGER": self.amc_secrets_manager.secret_name,
                "REGION": Aws.REGION
            },
            layers=[
                self.powertools_layer,
                self.wfm_layer,
                self.metrics_layer,
                SolutionsLayer.get_or_create(self)
            ]
        )

        # create cloudwatch events policy to allow the workflow scheduler lambdas to manage schedule rules
        self.cloudwatch_events_policy = Policy(
            self,
            "cloudwatchEvents-Inline-Policy",
            statements=[
                PolicyStatement(
                    actions=[
                        "events:PutRule",
                        "events:PutTargets",
                        "events:RemoveTargets",
                        "events:DeleteRule",
                        "events:TagResource",
                        "events:ListTargetsByRule"
                    ],
                    resources=[f"arn:aws:events:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:rule/{self._resource_prefix}-*"]
                )
            ]
        )
        self.lambda_delete_workflow_schedule.role.attach_inline_policy(self.cloudwatch_events_policy)
        self.lambda_create_workflow_schedule.role.attach_inline_policy(self.cloudwatch_events_policy)

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
                ),
            ]
        )

        secrets_manager_actions = [
            "secretsmanager:DescribeSecret",
            "secretsmanager:CreateSecret",
            "secretsmanager:UpdateSecret",
            "secretsmanager:PutSecretValue",
            "secretsmanager:GetSecretValue"
        ]
        secrets_manager_lambda_iam_policy = Policy(
            self, "SecretsManagerLambdaIamPolicy",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=secrets_manager_actions,
                    resources=[
                        f"arn:aws:secretsmanager:{Aws.REGION}:{Aws.ACCOUNT_ID}:secret:{self.amc_secrets_manager.secret_name}*"],
                )
            ]
        )

        self.lambda_function_list = [self.lambda_cancel_workflow_execution, self.lambda_check_workflow_execution_status,
                                     self.lambda_create_workflow, self.lambda_create_workflow_execution,
                                     self.lambda_create_workflow_schedule, self.lambda_delete_workflow,
                                     self.lambda_delete_workflow_schedule, self.lambda_get_execution_summary,
                                     self.lambda_get_workflow, self.lambda_invoke_workflow_execution_sm,
                                     self.lambda_invoke_workflow_sm, self.lambda_update_workflow,
                                     self.lambda_amc_auth]
        for function in self.lambda_function_list:
            cloudwatch_metrics_policy.attach_to_role(function.role)
            secrets_manager_lambda_iam_policy.attach_to_role(function.role)
            self.amc_secrets_manager.encryption_key.grant_encrypt_decrypt(function.role)

        ##################################
        #          Execution             #
        # Stepfunctions State Machine    #
        ##################################

        wait_initial_wait_step = stepfunctions.Wait(
            self,
            'Wait 5 seconds before starting',
            time=stepfunctions.WaitTime.seconds_path('$.initialWait')
        )
        choice_determine_execution_request_type = stepfunctions.Choice(
            self,
            'Determine Execution Request Type'
        )
        fail_execution_request_failed_to_process = stepfunctions.Fail(
            self,
            'Failed to process Execution request'
        )
        choice_evaluate_execution_final_status_response = stepfunctions.Choice(
            self,
            'Final Execution Evaluation'
        )

        choice_evaluate_execution_final_status_response.when(
            stepfunctions.Condition.or_(
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",  # NOSONAR
                    "SUCCEEDED"
                ),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "CANCELLED")
            ),
            stepfunctions.Succeed(
                self,
                'Execution Request Successfully Completed'
            )
        ).when(
            stepfunctions.Condition.or_(
                stepfunctions.Condition.string_equals(
                    "$.responseStatus", "FAILED"
                ),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "REJECTED"
                )
            ),
            stepfunctions.Fail(
                self,
                'Execution Request did not Complete'
            )
        ).otherwise(fail_execution_request_failed_to_process)

        publish_execution_sns = tasks.SnsPublish(
            self,
            'Publish Execution To SNS',
            message=stepfunctions.TaskInput.from_json_path_at("States.JsonToString($.snsMessage)"),
            topic=self.sns_topic,
            subject=stepfunctions.JsonPath.string_at("$.messageSubject"),  # NOSONAR
            result_path="$.SNSResponse",
            message_per_subscription_type=True,
            message_attributes={
                "customerId": tasks.MessageAttribute(
                    value=stepfunctions.TaskInput.from_json_path_at("$.customerId"),
                    data_type=tasks.MessageAttributeDataType.STRING
                ),
                "requestType": tasks.MessageAttribute(
                    value=stepfunctions.TaskInput.from_json_path_at("$.executionRequest.requestType"),  # NOSONAR
                    data_type=tasks.MessageAttributeDataType.STRING
                ),
                "responseStatus": tasks.MessageAttribute(
                    value=stepfunctions.TaskInput.from_json_path_at("$.responseStatus"),
                    data_type=tasks.MessageAttributeDataType.STRING
                )
            }
        )
        publish_execution_sns.next(choice_evaluate_execution_final_status_response)

        task_get_execution_status = tasks.LambdaInvoke(
            self,
            "Get Execution Status",
            output_path="$.Payload",  # NOSONAR
            lambda_function=self.lambda_check_workflow_execution_status
        )
        task_create_execution = tasks.LambdaInvoke(
            self,
            "Create Workflow Execution",
            output_path="$.Payload",
            lambda_function=self.lambda_create_workflow_execution
        )
        task_cancel_execution = tasks.LambdaInvoke(
            self,
            "Cancel Workflow Execution",
            output_path="$.Payload",
            lambda_function=self.lambda_cancel_workflow_execution
        )
        task_get_execution_summary = tasks.LambdaInvoke(
            self,
            "Get Workflow Execution Summary",
            output_path="$.Payload",
            lambda_function=self.lambda_get_execution_summary
        )
        choice_evaluate_summary = stepfunctions.Choice(
            self,
            'Evaluate Execution Summary',
            comment=f"check to see if there are less that {CONCURRENT_WORKFLOW_EXECUTION_LIMIT} running and pending executions before creating a new execution"
        )

        choice_evaluate_summary.when(
            stepfunctions.Condition.number_greater_than_equals(
                "$.executionsSummary.totalRunningorPending",
                CONCURRENT_WORKFLOW_EXECUTION_LIMIT
            ),
            stepfunctions.Wait(
                self,
                'Wait 10 Minutes for Other Executions to Finish',
                time=stepfunctions.WaitTime.duration(cdk.Duration.seconds(600))).next(task_get_execution_summary)
        ).otherwise(task_create_execution)
        task_get_execution_summary.next(choice_evaluate_summary)

        result_selector_items = {
            "customerId.$": "$.Item.customerId.S",
            "outputSNSTopicArn.$": "$.Item.outputSNSTopicArn.S",
            "amcInstanceId.$": "$.Item.amcInstanceId.S",
            "amcAmazonAdsAdvertiserId.$": "$.Item.amcAmazonAdsAdvertiserId.S",
            "amcAmazonAdsMarketplaceId.$": "$.Item.amcAmazonAdsMarketplaceId.S",
        }

        # State machine begins

        task_execution_get_customer_config = tasks.DynamoGetItem(
            self,
            'GetCustomerConfigRecord',
            key={
                'customerId': tasks.DynamoAttributeValue.from_string(
                    stepfunctions.JsonPath.string_at(
                        '$.customerId'
                    )
                )
            },
            result_selector=result_selector_items,
            table=self.dynamodb_customer_config_table,
            result_path="$.customerConfig")
        wait_initial_wait_step.next(task_execution_get_customer_config)
        task_execution_get_customer_config.next(choice_determine_execution_request_type)

        choice_determine_execution_request_type.when(
            stepfunctions.Condition.string_equals(
                "$.executionRequest.requestType",
                "getExecutionStatus"
            ),
            task_get_execution_status
        ).when(
            stepfunctions.Condition.string_equals(
                "$.executionRequest.requestType",
                "createExecution"
            ),
            task_get_execution_summary
        ).when(
            stepfunctions.Condition.string_equals(
                "$.executionRequest.requestType",
                "cancelExecution"
            ),
            task_cancel_execution
        ).otherwise(fail_execution_request_failed_to_process)
        choice_evaluate_execution_status_response = stepfunctions.Choice(
            self,
            "Execution Status Response"
        )
        task_create_execution.next(choice_evaluate_execution_status_response)
        task_cancel_execution.next(choice_evaluate_execution_status_response)

        wait_5_min_for_execution = stepfunctions.Wait(
            self,
            'Wait 5 minutes before re checking execution status',
            time=stepfunctions.WaitTime.duration(cdk.Duration.seconds(300)))
        wait_5_min_for_execution.next(task_get_execution_status)

        task_set_execution_email_body = tasks.EvaluateExpression(
            self,
            'Set Execution Notification Body',
            expression="`${$.messageSubject}\n\nCustomer ID: ${$.customerId}\nRequest Type: ${$.executionRequest.requestType}\nWorkflow Execution Name: ${$.workflowExecutionName}\nResponse Status: ${$.responseStatus}\nResponse Message: ${$.responseMessage}\n\n\nWorkflow Execution Request:\n${$.snsMessage.default}`",
            runtime=_lambda.Runtime.NODEJS_18_X,
            result_path="$.snsMessage.email"
        ).next(publish_execution_sns)
        pass_copy_execution_to_snsmessage = stepfunctions.Pass(
            self,
            'Copy Execution to SNS message content',
            result_path="$.snsMessage",
            parameters={"default.$": "States.JsonToString($)"}  # NOSONAR
        ).next(task_set_execution_email_body)
        task_set_execution_notification_subject = tasks.EvaluateExpression(
            self,
            'Set Execution Notification Subject',
            expression="`${$.customerId} ${$.executionRequest.requestType} for ${$.workflowExecutionName} ${$.responseStatus}`.slice(0,100)",
            runtime=_lambda.Runtime.NODEJS_18_X,
            result_path="$.messageSubject").next(pass_copy_execution_to_snsmessage)

        choice_evaluate_execution_status_response.when(
            stepfunctions.Condition.or_(
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "PENDING"
                ),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "RUNNING"
                ),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "PUBLISHING"
                )
            ),
            wait_5_min_for_execution).when(
            stepfunctions.Condition.or_(
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "SUCCEEDED"
                ),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "CANCELLED"
                ),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "FAILED"
                ),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus",
                    "REJECTED"
                )
            ),
            task_set_execution_notification_subject).when(
            stepfunctions.Condition.string_equals(
                "$.message",
                "Endpoint request timed out"
            ),
            stepfunctions.Wait(
                self,
                'Wait 10 Minutes to retry api call',
                time=stepfunctions.WaitTime.duration(cdk.Duration.seconds(600))
            ).next(choice_determine_execution_request_type)
        ).otherwise(fail_execution_request_failed_to_process)
        task_get_execution_status.next(choice_evaluate_execution_status_response)

        _executions_sm_log_group_name = f"/aws/vendedlogs/states/{Aws.STACK_NAME}-wfm-executions-{Fn.select(2, Fn.split('/', Aws.STACK_ID))}"
        _executions_sm_log_group = logs.LogGroup(
            self,
            'WFMExecutionsSMLogGroup',
            log_group_name=_executions_sm_log_group_name,
            retention=logs.RetentionDays.INFINITE
        )

        self.statemachine_workflow_executions_sm = stepfunctions.StateMachine(
            self,
            "WFMExecutionsSM",
            state_machine_name=f"{self._resource_prefix}-wfm-executions",
            definition=wait_initial_wait_step,
            logs=stepfunctions.LogOptions(level=stepfunctions.LogLevel.ALL, destination=_executions_sm_log_group)
        )
        self.kms_key.grant_encrypt_decrypt(self.statemachine_workflow_executions_sm.role)

        # Grant the lambda invoke function access to invoke the state machine
        self.statemachine_workflow_executions_sm.grant_start_execution(self.lambda_invoke_workflow_execution_sm.role)
        self.lambda_invoke_workflow_execution_sm.add_environment(
            "STEP_FUNCTION_STATE_MACHINE_ARN",
            self.statemachine_workflow_executions_sm.state_machine_arn
        )

        ##################################
        #         Workflow               #
        #  Stepfunctions State Machine   #
        ##################################

        choice_determine_workflow_request_type = stepfunctions.Choice(
            self,
            'Determine Workflow Request Type'
        )

        task_create_workflow = tasks.LambdaInvoke(
            self,
            "Create Workflow",
            output_path="$.Payload",
            lambda_function=self.lambda_create_workflow
        )
        task_update_workflow = tasks.LambdaInvoke(
            self,
            "Update Workflow",
            output_path="$.Payload",
            lambda_function=self.lambda_update_workflow
        )
        task_get_workflow = tasks.LambdaInvoke(
            self,
            "Get Workflow",
            output_path="$.Payload",
            lambda_function=self.lambda_get_workflow
        )
        task_delete_workflow = tasks.LambdaInvoke(
            self,
            "Delete Workflow",
            output_path="$.Payload",
            lambda_function=self.lambda_delete_workflow
        )
        choice_evaluate_workflow_status_response = stepfunctions.Choice(
            self,
            "Evaluate Workflow Status Response"
        )

        result_selector_items = {
            "customerId.$": "$.Item.customerId.S",
            "outputSNSTopicArn.$": "$.Item.outputSNSTopicArn.S",
            "amcInstanceId.$": "$.Item.amcInstanceId.S",
            "amcAmazonAdsAdvertiserId.$": "$.Item.amcAmazonAdsAdvertiserId.S",
            "amcAmazonAdsMarketplaceId.$": "$.Item.amcAmazonAdsMarketplaceId.S",
        }
        task_get_customer_config = tasks.DynamoGetItem(
            self,
            'GetCustomerConfig',
            key={'customerId': tasks.DynamoAttributeValue.from_string(
                stepfunctions.JsonPath.string_at('$.customerId')
            )
            },
            result_selector=result_selector_items,
            table=self.dynamodb_customer_config_table,
            result_path="$.customerConfig"
        )

        task_get_customer_config.next(choice_determine_workflow_request_type)
        task_create_workflow.next(choice_evaluate_workflow_status_response)
        task_update_workflow.next(choice_evaluate_workflow_status_response)
        task_get_workflow.next(choice_evaluate_workflow_status_response)
        task_delete_workflow.next(choice_evaluate_workflow_status_response)

        publish_workflow_sns = tasks.SnsPublish(
            self,
            'Publish Message To SNS',
            message=stepfunctions.TaskInput.from_json_path_at("States.JsonToString($.snsMessage)"),
            topic=self.sns_topic,
            subject=stepfunctions.JsonPath.string_at("$.messageSubject"),
            result_path="$.SNSResponse", message_per_subscription_type=True,
            message_attributes={"customerId": tasks.MessageAttribute(
                value=stepfunctions.TaskInput.from_json_path_at("$.customerId"),
                data_type=tasks.MessageAttributeDataType.STRING
            ),
                "requestType": tasks.MessageAttribute(
                    value=stepfunctions.TaskInput.from_json_path_at("$.workflowRequest.requestType"),  # NOSONAR
                    data_type=tasks.MessageAttributeDataType.STRING
                ),
                "responseStatus": tasks.MessageAttribute(
                    value=stepfunctions.TaskInput.from_json_path_at("$.responseStatus"),
                    data_type=tasks.MessageAttributeDataType.STRING
                )
            }
        )

        choice_final_status_check = stepfunctions.Choice(
            self,
            "Final Status Check"
        )

        publish_workflow_sns.next(choice_final_status_check)

        pass_copy_to_snsmessage = stepfunctions.Pass(
            self,
            'Set SNS message content',
            result_path="$.snsMessage",
            parameters={
                "default.$": "States.JsonToString($)",
                "email.$": "States.JsonToString($)"
            }
        )

        task_set_workflow_email_body = tasks.EvaluateExpression(
            self,
            'Set Workflow Notification Body',
            expression="`${$.messageSubject}\n\nCustomer ID: ${$.customerId}\nRequest Type: ${$.workflowRequest.requestType}\nWorkflow ID: ${$.workflowRequest.workflowId}\nResponse Status: ${$.responseStatus}\nResponse Message: ${$.responseMessage}\n\n\nWorkflow Request:\n${$.snsMessage.default}`",
            runtime=_lambda.Runtime.NODEJS_18_X,
            result_path="$.snsMessage.email").next(
            publish_workflow_sns
        )

        pass_copy_to_snsmessage.next(task_set_workflow_email_body)

        task_set_notification_subject = tasks.EvaluateExpression(
            self,
            'Set Message Notification Subject',
            expression="`${$.customerId} ${$.workflowRequest.requestType} for ${$.workflowRequest.workflowId} ${$.responseStatus}`.slice(0,100)",
            runtime=_lambda.Runtime.NODEJS_18_X,
            result_path="$.messageSubject").next(
            pass_copy_to_snsmessage
        )

        choice_evaluate_workflow_status_response.when(
            stepfunctions.Condition.or_(
                stepfunctions.Condition.not_(
                    stepfunctions.Condition.is_present("$.responseStatus")
                ),
                stepfunctions.Condition.not_(
                    stepfunctions.Condition.or_(
                        stepfunctions.Condition.string_equals(
                            "$.responseStatus",
                            "CREATED"
                        ),
                        stepfunctions.Condition.string_equals(
                            "$.responseStatus",
                            "UPDATED"
                        ),
                        stepfunctions.Condition.string_equals(
                            "$.responseStatus",
                            "RECEIVED"
                        ),
                        stepfunctions.Condition.string_equals(
                            "$.responseStatus",
                            "DELETED"
                        )
                    )
                )
            ),
            tasks.EvaluateExpression(
                self,
                'Set Execution as Failed',
                expression="`FAILED`",
                result_path="$.responseStatus"
            ).next(task_set_notification_subject)
        ).otherwise(task_set_notification_subject)

        choice_final_status_check.when(
            stepfunctions.Condition.or_(
                stepfunctions.Condition.string_equals(
                    "$.responseStatus", "CREATED"),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus", "UPDATED"),
                stepfunctions.Condition.string_equals(
                    "$.responseStatus", "RECEIVED"),
                stepfunctions.Condition.string_equals("$.responseStatus", "DELETED")),
            stepfunctions.Succeed(self, "Successfully processed Workflow Request")).otherwise(
            stepfunctions.Fail(self, 'Failed to process workflow request'))

        choice_determine_workflow_request_type.when(
            stepfunctions.Condition.string_equals(
                "$.workflowRequest.requestType", "createWorkflow"),
            task_create_workflow).when(
            stepfunctions.Condition.string_equals(
                "$.workflowRequest.requestType", "updateWorkflow"),
            task_update_workflow).when(
            stepfunctions.Condition.string_equals(
                "$.workflowRequest.requestType", "getWorkflow"),
            task_get_workflow) \
            .when(stepfunctions.Condition.string_equals("$.workflowRequest.requestType", "deleteWorkflow"),
                  task_delete_workflow) \
            .otherwise(stepfunctions.Fail(self, 'Invalid Workflow Request Type'))

        _workflows_sm_log_group_name = f"/aws/vendedlogs/states/{Aws.STACK_NAME}-wfm-workflows-{Fn.select(2, Fn.split('/', Aws.STACK_ID))}"
        _workflows_sm_log_group = logs.LogGroup(
            self,
            'WFMWorkflowsSMLogGroup',
            log_group_name=_workflows_sm_log_group_name,
            retention=logs.RetentionDays.INFINITE
        )

        self.statemachine_workflows_sm = stepfunctions.StateMachine(
            self,
            "WFMWorkflowsSM",
            state_machine_name=f"{self._resource_prefix}-wfm-workflows",
            definition=task_get_customer_config,
            logs=stepfunctions.LogOptions(level=stepfunctions.LogLevel.ALL, destination=_workflows_sm_log_group)
        )

        # Grant the lambda access to invoke the state machine
        self.statemachine_workflows_sm.grant_start_execution(self.lambda_invoke_workflow_sm.role)
        self.lambda_invoke_workflow_sm.add_environment(
            "STEP_FUNCTION_STATE_MACHINE_ARN",
            self.statemachine_workflows_sm.state_machine_arn
        )

        self.kms_key.grant_encrypt_decrypt(self.statemachine_workflows_sm.role)

        ##################################
        #             SNS                #
        #        TOPIC PUBLISHING        #
        ##################################

        # Add permissions for the wfm lambdas & state machines to publish to sns topics
        function_list = [
            self.lambda_cancel_workflow_execution,
            self.lambda_check_workflow_execution_status,
            self.lambda_create_workflow,
            self.lambda_create_workflow_execution,
            self.lambda_get_workflow,
            self.lambda_create_workflow_schedule,
            self.lambda_delete_workflow,
            self.lambda_delete_workflow_schedule,
            self.lambda_get_execution_summary,
            self.lambda_invoke_workflow_execution_sm,
            self.lambda_invoke_workflow_sm,
            self.lambda_update_workflow,
            self.statemachine_workflows_sm,
            self.statemachine_workflow_executions_sm
        ]
        sns_publish_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=['sns:Publish'],
            resources=[f"arn:aws:sns:{Aws.REGION}:{Aws.ACCOUNT_ID}:{Aws.STACK_ID}-wfm-SNSTopic-*"],
        )
        for function in function_list:
            self.sns_topic.grant_publish(function.role)
            function.add_to_role_policy(sns_publish_policy_statement)

    def _suppress_cfn_nag_warnings(self):
        Aspects.of(self).add(
            CfnNagSuppressAll(
                [
                    CfnNagSuppression(rule_id="W76", reason="SPCM for IAM policy document is higher than 25"),
                    CfnNagSuppression(rule_id="W12", reason="IAM policy should not allow * resource"),
                ],
                "AWS::IAM::Policy"
            )
        )

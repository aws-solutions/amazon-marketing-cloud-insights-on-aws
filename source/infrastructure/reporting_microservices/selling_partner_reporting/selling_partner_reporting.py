# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk import Aws, Aspects, CfnCondition, CfnOutput, SecretValue, RemovalPolicy, Duration
from aws_cdk import aws_lambda as lambda_
from aws_cdk.aws_iam import Effect, PolicyStatement, ServicePrincipal, Policy, Role, AccountRootPrincipal, \
    PolicyDocument
import aws_cdk.aws_kms as kms
import aws_cdk.aws_secretsmanager as secrets_manager
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from aws_cdk import aws_stepfunctions as sfn
import os
from pathlib import Path
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from constructs import Construct
from amc_insights.condition_aspect import ConditionAspect

LAMBDA_HANDLER = "handler.handler"


class SellingPartnerReporting(Construct):

    def __init__(
            self,
            scope: Construct,
            id: str,
            lambda_layers,
            team,
            dataset,
            reporting_shared_resources,
            creating_resources_condition: CfnCondition,
    ) -> None:
        super().__init__(scope, id)

        # Apply condition to resources in WFM construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._region = Aws.REGION
        self._resource_prefix = Aws.STACK_NAME
        self.lambda_layers = lambda_layers
        self.reporting_shared = reporting_shared_resources
        self._team = team
        self._dataset = dataset
        self.lambda_function_list = []

        self.create_secret()
        self.create_lambdas()
        self.create_state_machine()

        CfnOutput(
            self,
            "Secrets",
            description="Use this link to access Secrets Manager for the Selling Partner API",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/secretsmanager/secret?name={self.selling_partner_secrets_manager.secret_name}&region={Aws.REGION}",
            condition=creating_resources_condition,
        )

    def create_secret(self):
        ##########################
        #    OAUTH RESOURCES     #
        ##########################
        selling_partner_secrets_manager_key_policy_document = PolicyDocument(
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

        self.selling_partner_secrets_manager_key = kms.Key(
            self,
            "SecretKey",
            removal_policy=RemovalPolicy.DESTROY,
            description="OAuth Creds Secrets Manager Key",
            enable_key_rotation=True,
            policy=selling_partner_secrets_manager_key_policy_document,
            pending_window=Duration.days(30),
        )
        self.selling_partner_secrets_manager = secrets_manager.Secret(
            self,
            "Secret",
            encryption_key=self.selling_partner_secrets_manager_key,
            removal_policy=RemovalPolicy.DESTROY,
            secret_object_value={
                "client_id": SecretValue.unsafe_plain_text(""),
                "client_secret": SecretValue.unsafe_plain_text(""),
                "refresh_token": SecretValue.unsafe_plain_text(""),
                "access_token": SecretValue.unsafe_plain_text("")
            },
        )

    def create_lambdas(self):
        selling_partner_api_layer = lambda_.LayerVersion(
            self,
            "SellingPartnerReportingLayer",
            code=lambda_.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[0]}",
                    "reporting_service/lambda_layers/selling_partner_reporting_layer/"
                )
            ),
            layer_version_name=f"{self._resource_prefix}-selling-partner-reporting-layer",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
        )
        self.selling_partner_create_report = lambda_.Function(
            self,
            "SellingPartnerCreateReport",
            code=lambda_.Code.from_asset(
                os.path.join(f"{Path(__file__).parents[0]}", "reporting_service/lambdas/CreateReport")),
            handler=LAMBDA_HANDLER,
            description="Create report from Selling Partner API",
            timeout=Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME,
                "REGION": Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "SELLING_PARTNER_SECRETS_MANAGER": self.selling_partner_secrets_manager.secret_name,
            },
            layers=[selling_partner_api_layer, self.lambda_layers.microservice_layer,
                    SolutionsLayer.get_or_create(self), self.lambda_layers.metrics_layer]
        )
        secrets_manager_actions = [
            "secretsmanager:DescribeSecret",
            "secretsmanager:CreateSecret",
            "secretsmanager:UpdateSecret",
            "secretsmanager:PutSecretValue",
            "secretsmanager:GetSecretValue"
        ]
        self.secrets_manager_lambda_iam_policy = Policy(
            self, "SecretsManagerLambdaIamPolicy",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=secrets_manager_actions,
                    resources=[
                        f"arn:aws:secretsmanager:{Aws.REGION}:{Aws.ACCOUNT_ID}:secret:{self.selling_partner_secrets_manager.secret_name}*"
                    ],
                )
            ]
        )
        self.secrets_manager_lambda_iam_policy.attach_to_role(self.selling_partner_create_report.role)
        self.selling_partner_secrets_manager.encryption_key.grant_encrypt_decrypt(
            self.selling_partner_create_report.role)

        self.get_report_status = lambda_.Function(
            self,
            "GetReportStatus",
            code=lambda_.Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}",
                                                      "reporting_service/lambdas/GetReportStatus")),
            handler=LAMBDA_HANDLER,
            description="Seller Partner Check Report Status",
            timeout=Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME,
                "REGION": Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "SELLING_PARTNER_SECRETS_MANAGER": self.selling_partner_secrets_manager.secret_name,
            },
            layers=[selling_partner_api_layer, self.lambda_layers.microservice_layer,
                    SolutionsLayer.get_or_create(self), self.lambda_layers.metrics_layer]
        )
        self.secrets_manager_lambda_iam_policy.attach_to_role(self.get_report_status.role)
        self.selling_partner_secrets_manager.encryption_key.grant_encrypt_decrypt(self.get_report_status.role)

        self.get_report_document = lambda_.Function(
            self,
            "GetReportDocument",
            code=lambda_.Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}",
                                                      "reporting_service/lambdas/GetReportDocument")),
            handler=LAMBDA_HANDLER,
            description="Seller Partner Get Report Document",
            timeout=Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME,
                "REGION": Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "SELLING_PARTNER_SECRETS_MANAGER": self.selling_partner_secrets_manager.secret_name,
            },
            layers=[
                selling_partner_api_layer,
                self.lambda_layers.microservice_layer,
                SolutionsLayer.get_or_create(self),
                self.lambda_layers.metrics_layer
            ]
        )
        self.secrets_manager_lambda_iam_policy.attach_to_role(self.get_report_document.role)
        self.selling_partner_secrets_manager.encryption_key.grant_encrypt_decrypt(self.get_report_document.role)

        self.download_report = lambda_.Function(
            self,
            "DownloadReport",
            code=lambda_.Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}",
                                                      "reporting_service/lambdas/DownloadReport")),
            handler=LAMBDA_HANDLER,
            description="Download Seller Partners Report",
            timeout=Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME,
                "REGION": Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "SP_REPORT_BUCKET": self.reporting_shared.bucket.bucket_name,
                "SP_REPORT_BUCKET_KMS_KEY_ID": self.reporting_shared.bucket.encryption_key.key_id,
                "TEAM": self._team,
                "DATASET": self._dataset
            },
            layers=[
                selling_partner_api_layer,
                self.lambda_layers.microservice_layer,
                SolutionsLayer.get_or_create(self),
                self.lambda_layers.metrics_layer
            ]
        )
        self.reporting_shared.bucket_access_policy.attach_to_role(self.download_report.role)

        self.selling_partner_invoke_state_machine = lambda_.Function(
            self,
            "InvokeSpReportSM",
            code=lambda_.Code.from_asset(
                os.path.join(f"{Path(__file__).parents[0]}", "reporting_service/lambdas/InvokeSpReportSM")),
            handler=LAMBDA_HANDLER,
            description="Invoke state machine for Selling Partner Reporting",
            timeout=Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": Aws.STACK_NAME,
                "REGION": Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix
            },
            layers=[selling_partner_api_layer, self.lambda_layers.microservice_layer,
                    SolutionsLayer.get_or_create(self), self.lambda_layers.metrics_layer]
        )
        
        self.selling_partner_schedule_report = lambda_.Function(
            self,
            "ScheduleSpReport",
            code=lambda_.Code.from_asset(
                os.path.join(f"{Path(__file__).parents[0]}", "reporting_service/lambdas/ScheduleSpReport")),
            handler=LAMBDA_HANDLER,
            description="Schedule report for Selling Partner Reporting",
            timeout=Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "REGION": Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "DATASET": self._dataset,
                "INVOKE_SP_REPORT_SM_LAMBDA_ARN": self.selling_partner_invoke_state_machine.function_arn
            },
            layers=[selling_partner_api_layer, self.lambda_layers.microservice_layer,
                    SolutionsLayer.get_or_create(self), self.lambda_layers.metrics_layer]
        )
        self.reporting_shared.cloudwatch_events_policy.attach_to_role(self.selling_partner_schedule_report.role)

        self.lambda_function_list.extend(
            [
                self.selling_partner_create_report,
                self.get_report_status,
                self.get_report_document,
                self.download_report,
                self.selling_partner_invoke_state_machine,
                self.selling_partner_schedule_report
            ]
        )
        for function in self.lambda_function_list:
            self.reporting_shared.cloudwatch_metrics_policy.attach_to_role(function.role)

    def create_state_machine(self):
        # constants for sonarqube
        PAYLOAD = "$.Payload"
        STATUS_CODE = "$.statusCode"
        PROCESSING_STATUS = "$.processingStatus"
        DOCUMENT_ID = "$.reportDocumentId"
        ERROR_MESSAGE = "$.errorMessage"
        SUCCESS = "$.success"

        task_create_sellers_partner_report = sfn_tasks.LambdaInvoke(
            self,
            "Create Selling Partner Report",
            output_path=PAYLOAD,
            lambda_function=self.selling_partner_create_report
        )
        task_get_report_status = sfn_tasks.LambdaInvoke(
            self,
            "Get Report Status",
            output_path=PAYLOAD,
            lambda_function=self.get_report_status
        )

        task_get_report_document = sfn_tasks.LambdaInvoke(
            self,
            "Get Report Document",
            output_path=PAYLOAD,
            lambda_function=self.get_report_document
        )

        task_download_report = sfn_tasks.LambdaInvoke(
            self,
            "Download Report",
            output_path=PAYLOAD,
            lambda_function=self.download_report
        )
        
        # we try to catch and surface errors for easier debugging through the state machine console
        # if no error message has been assigned to the ERROR_MESSAGE path, we fail with a generic message
        caught_error = sfn.Fail(self, "Caught Error", error_path=ERROR_MESSAGE)
        generic_failed = sfn.Fail(self, "Execution Failed", cause="Check CloudWatch Logs for more information")
        execution_success = sfn.Succeed(self, "Execution Succeeded")

        wait_2_minutes_to_check_report = sfn.Wait(
            self,
            'Wait 2 minutes before re-checking generation status of report',
            time=sfn.WaitTime.duration(Duration.seconds(120)))
        
        choice_evaluate_failure = sfn.Choice(
            self,
            "Evaluate Failure"
        ).when(
            sfn.Condition.is_present(
                    ERROR_MESSAGE
            ), caught_error).otherwise(
                generic_failed
        )

        # response codes: https://developer-docs.amazon.com/sp-api/docs/reports-api-v2021-06-30-reference
        # processing status: https://developer-docs.amazon.com/sp-api/docs/reports-api-v2021-06-30-verify-that-the-report-processing-is-complete
        choice_evaluate_create_report_response = sfn.Choice(
            self,
            "Evaluate Create Report Response"
        ).when(
            sfn.Condition.number_equals(
                STATUS_CODE,
                202
            ),
            task_get_report_status).otherwise(
                choice_evaluate_failure)
            
        choice_evaluate_get_report_status_response = sfn.Choice(
            self,
            "Evaluate Get Report Status Response"
        ).when(
            sfn.Condition.or_(
                sfn.Condition.string_equals(
                    PROCESSING_STATUS,
                    "IN_QUEUE"
                ),
                sfn.Condition.string_equals(
                    PROCESSING_STATUS,
                    "IN_PROGRESS"
                )
            ),
            wait_2_minutes_to_check_report).when(
            sfn.Condition.or_(
                sfn.Condition.string_equals(
                    PROCESSING_STATUS,
                    "DONE"
                ),
                sfn.Condition.and_(
                    sfn.Condition.string_equals(
                        PROCESSING_STATUS,
                        "FATAL"
                    ),
                    sfn.Condition.is_present(
                        DOCUMENT_ID,
                    ),
                )
            ),
            task_get_report_document).otherwise(
                choice_evaluate_failure)
            
        choice_evaluate_get_report_document_response = sfn.Choice(
            self,
            "Evaluate Get Report Document Response"
        ).when(
            sfn.Condition.or_(
                sfn.Condition.and_(
                    sfn.Condition.string_equals(
                        PROCESSING_STATUS,
                        "DONE"
                    ),
                    sfn.Condition.is_present(
                        DOCUMENT_ID,
                    ),
                ),
                sfn.Condition.and_(
                    sfn.Condition.string_equals(
                        PROCESSING_STATUS,
                        "FATAL"
                    ),
                    sfn.Condition.is_present(
                        DOCUMENT_ID,
                    ),
                )
            ), task_download_report).otherwise(
                choice_evaluate_failure)
            
        choice_evaluate_download_report_response = sfn.Choice(
            self,
            "Evaluate Download Report Response"
        ).when(
            sfn.Condition.boolean_equals(
                    SUCCESS,
                    True
            ), execution_success).otherwise(
                choice_evaluate_failure)

        task_create_sellers_partner_report.next(choice_evaluate_create_report_response)
        task_get_report_status.next(choice_evaluate_get_report_status_response)
        wait_2_minutes_to_check_report.next(task_get_report_status)
        task_get_report_document.next(choice_evaluate_get_report_document_response)
        task_download_report.next(choice_evaluate_download_report_response)

        self.sp_report_stepfunctions = sfn.StateMachine(
            self,
            "SellerPartnersCreateReportSM",
            state_machine_name=f"{self._resource_prefix}-selling-partner-reports",
            definition_body=sfn.DefinitionBody.from_chainable(task_create_sellers_partner_report),
        )

        # Grant lambda access to invoke the state machine
        self.sp_report_stepfunctions.grant_start_execution(self.selling_partner_invoke_state_machine.role)
        self.selling_partner_invoke_state_machine.add_environment(
            "STEP_FUNCTION_STATE_MACHINE_ARN",
            self.sp_report_stepfunctions.state_machine_arn
        )
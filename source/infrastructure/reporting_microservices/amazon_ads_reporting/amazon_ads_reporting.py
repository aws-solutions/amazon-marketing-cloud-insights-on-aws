# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from constructs import Construct
import aws_cdk as cdk
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_kms as kms
from aws_cdk import Aws
import os
from pathlib import Path
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_cdk.aws_lambda import Runtime, Architecture
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from amc_insights.condition_aspect import ConditionAspect
from data_lake.foundations.foundations_construct import FoundationsConstruct


class AmazonAdsReporting(Construct):
    def __init__(
            self,
            scope,
            id,
            amazon_ads_secrets_manager,
            amazon_ads_secrets_manager_lambda_policy,
            lambda_layers,
            creating_resources_condition: cdk.CfnCondition,
            foundations_resources: FoundationsConstruct,
            team,
            dataset,
            reporting_shared_resources
    ) -> None:
        super().__init__(scope, id)

        # Apply condition to create Microservice resources
        cdk.Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._resource_prefix = cdk.Aws.STACK_NAME
        self.amazon_ads_secrets_manager = amazon_ads_secrets_manager
        self.amazon_ads_secrets_manager_lambda_policy = amazon_ads_secrets_manager_lambda_policy
        self.lambda_layers = lambda_layers
        self._foundations_resources = foundations_resources
        self._team = team
        self._dataset = dataset
        self.reporting_shared = reporting_shared_resources
        self.lambda_function_list = []

        self.create_lambdas()
        self.create_state_machine()

    def create_lambdas(self):
        amazon_ads_reporting_layer = lambda_.LayerVersion(
            self,
            "AmazonAdsReportingLayer",
            code=lambda_.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[0]}",
                    "reporting_service/lambda_layers/amazon_ads_reporting_layer/"
                )
            ),
            layer_version_name=f"{self._resource_prefix}-amazon-ads-reporting-layer",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
        )

        self.request_sponsored_ads_v3_lambda = lambda_.Function(
            self,
            "RequestSponsoredAdsReport",
            code=lambda_.Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}",
                                                      "reporting_service/lambdas/RequestSponsoredAdsReport")),
            handler="handler.handler",  # NOSONAR
            description="Request Amazon Ads Sponsored Ads Report",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": cdk.Aws.STACK_NAME,
                "REGION": cdk.Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "AMAZON_ADS_SECRETS_MANAGER": self.amazon_ads_secrets_manager.secret_name,
            },
            layers=[amazon_ads_reporting_layer, self.lambda_layers.microservice_layer,
                    SolutionsLayer.get_or_create(self), self.lambda_layers.metrics_layer]
        )
        self.amazon_ads_secrets_manager_lambda_policy.attach_to_role(self.request_sponsored_ads_v3_lambda.role)
        self.amazon_ads_secrets_manager.encryption_key.grant_encrypt_decrypt(self.request_sponsored_ads_v3_lambda.role)

        self.check_report_status = lambda_.Function(
            self,
            "CheckReportStatus",
            code=lambda_.Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}",
                                                      "reporting_service/lambdas/CheckReportStatus")),
            handler="handler.handler",  # NOSONAR
            description="Amazon Ads Check Report Status",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": cdk.Aws.STACK_NAME,
                "REGION": cdk.Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "AMAZON_ADS_SECRETS_MANAGER": self.amazon_ads_secrets_manager.secret_name,
            },
            layers=[amazon_ads_reporting_layer, self.lambda_layers.microservice_layer,
                    SolutionsLayer.get_or_create(self), self.lambda_layers.metrics_layer]
        )
        self.amazon_ads_secrets_manager_lambda_policy.attach_to_role(self.check_report_status.role)
        self.amazon_ads_secrets_manager.encryption_key.grant_encrypt_decrypt(self.check_report_status.role)

        self.get_profiles_lambda = lambda_.Function(
            self,
            "GetProfilesFunction",
            code=lambda_.Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}",
                                                      "reporting_service/lambdas/GetProfiles")),
            handler="handler.handler",  # NOSONAR
            description="Gets profile settings from Amazon Ads api",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": cdk.Aws.STACK_NAME,
                "REGION": cdk.Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "AMAZON_ADS_SECRETS_MANAGER": self.amazon_ads_secrets_manager.secret_name
            },
            layers=[
                amazon_ads_reporting_layer,
                self.lambda_layers.microservice_layer,
                SolutionsLayer.get_or_create(self),
                self.lambda_layers.metrics_layer
            ]
        )
        self.amazon_ads_secrets_manager_lambda_policy.attach_to_role(self.get_profiles_lambda.role)
        self.amazon_ads_secrets_manager.encryption_key.grant_encrypt_decrypt(self.get_profiles_lambda.role)

        self.download_report = lambda_.Function(
            self,
            "DownloadReport",
            code=lambda_.Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}",
                                                      "reporting_service/lambdas/DownloadReport")),
            handler="handler.handler",  # NOSONAR
            description="Download Amazon Ads Report",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": cdk.Aws.STACK_NAME,
                "REGION": cdk.Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix,
                "ADS_REPORT_BUCKET": self.reporting_shared.bucket.bucket_name,
                "ADS_REPORT_BUCKET_KMS_KEY_ID": self.reporting_shared.bucket.encryption_key.key_id,
                "TEAM": self._team,
                "DATASET": self._dataset
            },
            layers=[amazon_ads_reporting_layer, self.lambda_layers.microservice_layer,
                    SolutionsLayer.get_or_create(self), PowertoolsLayer.get_or_create(self),
                    self.lambda_layers.metrics_layer]
        )
        self.reporting_shared.bucket_access_policy.attach_to_role(self.download_report.role)

        self.invoke_ads_report_sm_lambda = lambda_.Function(
            self,
            "InvokeAdsReportSM",
            code=lambda_.Code.from_asset(os.path.join(f"{Path(__file__).parents[0]}",
                                                      "reporting_service/lambdas/InvokeAdsReportSM")),
            handler="handler.handler",  # NOSONAR
            description="Invoke state machine for Amazon Ads Reporting",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "STACK_NAME": cdk.Aws.STACK_NAME,
                "REGION": cdk.Aws.REGION,
                "RESOURCE_PREFIX": self._resource_prefix
            },
            layers=[amazon_ads_reporting_layer, self.lambda_layers.microservice_layer,
                    self.lambda_layers.metrics_layer, SolutionsLayer.get_or_create(self),
                    PowertoolsLayer.get_or_create(self)]
        )
        
        self.schedule_report_lambda = lambda_.Function(
            self,
            "ScheduleAdsReport",
            code=lambda_.Code.from_asset(
                os.path.join(f"{Path(__file__).parents[0]}", "reporting_service/lambdas/ScheduleAdsReport")),
            handler="handler.handler",  # NOSONAR,
            description="Schedule report for Amazon Ads Reporting",
            timeout=cdk.Duration.seconds(30),
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
                "INVOKE_ADS_REPORT_SM_LAMBDA_ARN": self.invoke_ads_report_sm_lambda.function_arn
            },
            layers=[amazon_ads_reporting_layer, self.lambda_layers.microservice_layer,
                    SolutionsLayer.get_or_create(self), self.lambda_layers.metrics_layer]
        )
        self.reporting_shared.cloudwatch_events_policy.attach_to_role(self.schedule_report_lambda.role)
        
        self.lambda_function_list.extend(
            [
                self.request_sponsored_ads_v3_lambda,
                self.check_report_status,
                self.get_profiles_lambda,
                self.download_report,
                self.invoke_ads_report_sm_lambda,
                self.schedule_report_lambda
            ]
        )
        for function in self.lambda_function_list:
            self.reporting_shared.cloudwatch_metrics_policy.attach_to_role(function.role)

    def create_state_machine(self):
        # constants for sonarqube
        PAYLOAD = "$.Payload"
        RESPONSE_STATUS = "$.responseStatus"

        task_request_sponsored_ads_version_3_report = sfn_tasks.LambdaInvoke(
            self,
            "Create Sponsored Ad Report",
            output_path=PAYLOAD,
            lambda_function=self.request_sponsored_ads_v3_lambda
        )
        task_check_report_status = sfn_tasks.LambdaInvoke(
            self,
            "Check Report Status",
            output_path=PAYLOAD,
            lambda_function=self.check_report_status
        )

        task_download_report = sfn_tasks.LambdaInvoke(
            self,
            "Download Report",
            output_path=PAYLOAD,
            lambda_function=self.download_report
        )

        wait_5_minutes_to_generate_report = sfn.Wait(
            self,
            'Wait 5 minutes before re-checking generation status of report',
            time=sfn.WaitTime.duration(cdk.Duration.seconds(300)))

        choice_evaluate_request_report_response = sfn.Choice(
            self,
            "Evaluate Request Report Response"
        ).when(
            sfn.Condition.or_(
                sfn.Condition.string_equals(
                    RESPONSE_STATUS,
                    "PENDING"
                ),
                sfn.Condition.string_equals(
                    RESPONSE_STATUS,
                    "PROCESSING"
                )
            ),
            wait_5_minutes_to_generate_report).when(
            sfn.Condition.or_(
                sfn.Condition.string_equals(
                    RESPONSE_STATUS,
                    "COMPLETED"
                )
            ),
            task_download_report
        ).otherwise(sfn.Fail(
            self,
            'Failed to request report'
        ))

        task_request_sponsored_ads_version_3_report.next(choice_evaluate_request_report_response)
        wait_5_minutes_to_generate_report.next(task_check_report_status)
        task_check_report_status.next(choice_evaluate_request_report_response)

        self.ads_report_stepfunctions = sfn.StateMachine(
            self,
            "AmazonAdsCreateReportSM",
            state_machine_name=f"{self._resource_prefix}-amazon-ads-reports",
            definition_body=sfn.DefinitionBody.from_chainable(task_request_sponsored_ads_version_3_report),
        )

        # Grant lambda access to invoke the state machine
        self.ads_report_stepfunctions.grant_start_execution(self.invoke_ads_report_sm_lambda.role)
        self.invoke_ads_report_sm_lambda.add_environment(
            "STEP_FUNCTION_STATE_MACHINE_ARN",
            self.ads_report_stepfunctions.state_machine_arn
        )

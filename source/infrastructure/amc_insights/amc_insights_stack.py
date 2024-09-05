# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
from typing import Any, Dict
from constructs import Construct
from dataclasses import dataclass
from aws_solutions.cdk.cfn_nag import CfnNagSuppressAll, CfnNagSuppression, CfnGuardSuppressAll
from data_lake.foundations.foundations_construct import FoundationsConstruct
from data_lake.pipelines.sdlf_pipeline import SDLFPipelineConstruct
from data_lake.datasets import SDLFDatasetConstruct
from platform_management.platform_management_sagemaker import PlatformManagerSageMaker
from amc_insights.microservices.tenant_provisioning_service import TenantProvisioningService
from amc_insights.custom_resource.anonymized_operational_metrics import OperationalMetrics
from amc_insights.custom_resource.cloudwatch_metrics.cloudwatch_metrics import CloudwatchMetrics
from amc_insights.microservices.workflow_manager_service import WorkFlowManagerService
from aws_cdk import CfnParameter, CfnCondition, Fn, Aspects
from aws_solutions.cdk.stack import SolutionStack
from data_lake.utils.datasets_config import DatasetsConfigs
from amc_insights.solution_buckets.solution_buckets import SolutionBuckets
from amc_insights.integration.integration_construct import IntegrationConstruct
from amc_insights.app_registry import AppRegistry
from amc_insights.custom_resource.user_scripts.user_scripts_construct import UserScriptsCustomResource
from amc_insights.admin_policy.admin_policy_construct import AdminPolicy
from amc_insights.custom_resource.lakeformation_settings.lakeformation_settings import LakeformationSettings
from amc_insights.cloudtrail.cloudtrail_integration import CloudTrailIntegration
from reporting_microservices.selling_partner_reporting.selling_partner_reporting import SellingPartnerReporting
from reporting_microservices.amazon_ads_reporting.amazon_ads_reporting import AmazonAdsReporting
from reporting_microservices.reporting_shared import ReportingShared
from aws_lambda_layers.lambda_layer_construct import AwsLambdaLayers
from data_lake.glue.glue_scripts_uploader import GlueScriptsUploader


@dataclass
class AMCDataset:
    dataset = "amc"
    pipeline = "insights"
    stage_a_transform = "amc_light_transform"
    stage_b_transform = "default_heavy_transform"
    description = "Amazon Marketing Cloud"


@dataclass
class AdsReportDataset:
    dataset = "ads_report"
    pipeline = "insights"
    stage_a_transform = "reports_light_transform"
    stage_b_transform = "default_heavy_transform"
    description = "Amazon Ads Reporting"


@dataclass
class SpReportDataset:
    dataset = "sp_report"
    pipeline = "insights"
    stage_a_transform = "reports_light_transform"
    stage_b_transform = "default_heavy_transform"
    description = "Selling Partner Reporting"


@dataclass
class DataLakes:
    """This data class collects all resources created for Data Lake
    """
    foundation: FoundationsConstruct
    sdlf_pipelines: Dict[str, SDLFPipelineConstruct]
    datasets: Dict[str, SDLFDatasetConstruct]


@dataclass
class Microservice:
    """This data class collects all resources created for microservice.
    """
    tps: TenantProvisioningService
    wfm: WorkFlowManagerService
    pmn: PlatformManagerSageMaker
    spr: SellingPartnerReporting
    aar: AmazonAdsReporting
    rsr: ReportingShared


class AMCInsightsStack(SolutionStack):
    name = "amcinsights"
    description = "Amazon Marketing Cloud Insights"
    template_filename = "amazon-marketing-cloud-insights.template"

    def __init__(self, scope: Construct, id: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(scope, id, *args, **kwargs)
        self.synthesizer.bind(self)

        # Aspects
        self._suppress_cfn_nag_warnings_on_stack()
        Aspects.of(self).add(AppRegistry(self, f'AppRegistry-{id}'))

        self._environment_id = "dev"
        self._team = "adtech"
        self._config_file_path = os.path.join(f"{Path(__file__).parent.parent}", "datasets_parameters.json")

        self._create_template_parameters()

        ######################################
        #           CONDITIONS               #
        ######################################
        self._is_creating_datalake_condition = CfnCondition(
            self,
            id="ShouldDeployDataLakeCondition",
            expression=Fn.condition_equals(self._is_creating_datalake.value_as_string, "Yes")
        )
        self._is_creating_microservices_condition = CfnCondition(
            self,
            id="ShouldDeployMicroservicesCondition",
            expression=Fn.condition_equals(self._is_creating_microservices.value_as_string, "Yes")
        )
        self._is_deplopying_full_app_condition = CfnCondition(
            self,
            id="DeployingFullApplication",
            expression=Fn.condition_equals(self._is_creating_microservices.value_as_string,
                                           self._is_creating_datalake.value_as_string)
        )

        ######################################
        #        DEFAULT CONSTRUCTS          #
        ######################################
        # Solution Buckets
        solution_buckets_construct = SolutionBuckets(self, "buckets")

        # User Scripts
        UserScriptsCustomResource(
            self,
            id="userscripts",
            solution_buckets=solution_buckets_construct
        )

        # Operational Metrics
        OperationalMetrics(
            self,
            "operational-metrics",
        )

        # Cloudwatch Metrics
        CloudwatchMetrics(
            self,
            "cloudwatch-metrics"
        )

        ######################################
        #       DATA LAKE CONSTRUCTS         #
        ######################################
        # Foundations
        foundations_construct = FoundationsConstruct(self, "foundations",
                                                     environment_id=self._environment_id,
                                                     creating_resources_condition=self._is_creating_datalake_condition)

        shared_layers = AwsLambdaLayers(self, "shared-layers",
                                        creating_resources_condition=self._is_creating_microservices_condition)

        # Insights Pipeline
        insights_pipeline_construct = SDLFPipelineConstruct(self, "data-lake-pipeline",
                                                            environment_id=self._environment_id,
                                                            team=self._team,
                                                            foundations_resources=foundations_construct,
                                                            pipeline=AMCDataset.pipeline,
                                                            creating_resources_condition=self._is_creating_datalake_condition
                                                            )
        
        # Uploader for all sdlf_heavy_transform glue scripts
        GlueScriptsUploader(
            self,
            id="SyncGlueScripts",
            solution_buckets=solution_buckets_construct,
        )

        # AMC Dataset
        amc_dataset_construct = SDLFDatasetConstruct(self, "data-lake-dataset-amc",
                                                     environment_id=self._environment_id,
                                                     team=self._team,
                                                     solution_buckets=solution_buckets_construct,
                                                     foundations_resources=foundations_construct,
                                                     sdlf_pipeline_stage_b=insights_pipeline_construct.stage_b_transform,
                                                     dataset_parameters=AMCDataset,
                                                     creating_resources_condition=self._is_creating_datalake_condition,
                                                     )

        # Ads Report Dataset
        aar_dataset_construct = SDLFDatasetConstruct(
            self,
            "data-lake-dataset-ads-report",
            environment_id=self._environment_id,
            team=self._team,
            solution_buckets=solution_buckets_construct,
            foundations_resources=foundations_construct,
            sdlf_pipeline_stage_b=insights_pipeline_construct.stage_b_transform,
            dataset_parameters=AdsReportDataset,
            creating_resources_condition=self._is_creating_datalake_condition,
        )

        # SP Report Dataset
        spr_dataset_construct = SDLFDatasetConstruct(
            self,
            "data-lake-dataset-sp-report",
            environment_id=self._environment_id,
            team=self._team,
            solution_buckets=solution_buckets_construct,
            foundations_resources=foundations_construct,
            sdlf_pipeline_stage_b=insights_pipeline_construct.stage_b_transform,
            dataset_parameters=SpReportDataset,
            creating_resources_condition=self._is_creating_datalake_condition,
        )

        # Reference resources in the Data Lake.
        self.data_lakes = DataLakes(foundation=foundations_construct,
                                    sdlf_pipelines={AMCDataset.pipeline: insights_pipeline_construct},
                                    datasets={
                                        AMCDataset.dataset: amc_dataset_construct,
                                        AdsReportDataset.dataset: aar_dataset_construct,
                                        SpReportDataset.dataset: spr_dataset_construct,
                                    },
                                    )

        # Additional Datasets & Pipelines
        # Check if entries were added
        if os.stat(self._config_file_path).st_size != 0:
            datasets_configs = DatasetsConfigs(
                environment_id=self._environment_id,
                config_file_path=self._config_file_path).dataset_configs

            for dataset_parameters in datasets_configs:
                # Create new pipeline if not using insights pipeline
                pipeline_name = dataset_parameters.pipeline
                if pipeline_name not in self.data_lakes.sdlf_pipelines.keys():
                    sdlf_pipeline = SDLFPipelineConstruct(self, f"data-lake-{pipeline_name}-pipeline",
                                                          environment_id=self._environment_id,
                                                          team=self._team,
                                                          foundations_resources=foundations_construct,
                                                          pipeline=pipeline_name,
                                                          creating_resources_condition=self._is_creating_datalake_condition,
                                                          )
                    # Add the new pipeline to the dataclass DataLakes for future reference.
                    self.data_lakes.sdlf_pipelines[pipeline_name] = sdlf_pipeline

                # Register datasets to the pipeline with concrete transformations and glue job
                dataset_name = dataset_parameters.dataset
                if dataset_name not in self.data_lakes.datasets.keys():
                    sdlf_dataset = SDLFDatasetConstruct(
                        self,
                        f"data-lake-dataset-{dataset_name}",
                        environment_id=self._environment_id,
                        team=self._team,
                        foundations_resources=foundations_construct,
                        sdlf_pipeline_stage_b=self.data_lakes.sdlf_pipelines.get(pipeline_name).stage_b_transform,
                        dataset_parameters=dataset_parameters,
                        solution_buckets=solution_buckets_construct,
                        creating_resources_condition=self._is_creating_datalake_condition,
                    )
                    # Add the new data to the dataclass DataLakes for future reference.
                    self.data_lakes.datasets[dataset_name] = sdlf_dataset

        ######################################
        #       MICROSERVICE CONSTRUCTS      #
        ######################################

        ### Amazon Marketing Cloud ###
        wfm_construct = WorkFlowManagerService(self, "wfm",
                                               team=self._team,
                                               email_parameter=self._email_param,
                                               creating_resources_condition=self._is_creating_microservices_condition,
                                               lambda_layers=shared_layers)

        tps_construct = TenantProvisioningService(self, "tps",
                                                  team=self._team,
                                                  dataset=AMCDataset.dataset,
                                                  workflow_manager_resources=wfm_construct,
                                                  solution_buckets=solution_buckets_construct,
                                                  creating_resources_condition=self._is_creating_microservices_condition,
                                                  data_lake_enabled=self._is_creating_datalake)

        ### Reporting Services ###
        # Shared reporting microservice resources
        reporting_shared = ReportingShared(self, "reports",
                                           creating_resources_condition=self._is_creating_microservices_condition)

        # Selling Partner microservice
        spr_construct = SellingPartnerReporting(self, "selling-partner",
                                                lambda_layers=shared_layers,
                                                reporting_shared_resources=reporting_shared,
                                                team=self._team,
                                                dataset=SpReportDataset.dataset,
                                                creating_resources_condition=self._is_creating_microservices_condition)

        # Amazon Ads microservice
        aar_construct = AmazonAdsReporting(self, "amazon-ads",
                                           amazon_ads_secrets_manager=wfm_construct.amc_secrets_manager,
                                           amazon_ads_secrets_manager_lambda_policy=wfm_construct.secrets_manager_lambda_iam_policy,
                                           lambda_layers=shared_layers,
                                           creating_resources_condition=self._is_creating_microservices_condition,
                                           foundations_resources=foundations_construct,
                                           team=self._team,
                                           dataset=AdsReportDataset.dataset,
                                           reporting_shared_resources=reporting_shared)

        ### Sagemaker Jupyter Notebooks ###
        pmn_construct = PlatformManagerSageMaker(self, "platform-manager",
                                                 environment_id=self._environment_id,
                                                 team=self._team,
                                                 workflow_manager_resources=wfm_construct,
                                                 tenant_provisioning_resources=tps_construct,
                                                 amazon_ads_reporting_resources=aar_construct,
                                                 selling_partner_reporting_resources=spr_construct,
                                                 solution_buckets=solution_buckets_construct,
                                                 creating_resources_condition=self._is_creating_microservices_condition)

        # Reference resources in the Microservice.
        self.microservice = Microservice(tps=tps_construct, wfm=wfm_construct, pmn=pmn_construct, spr=spr_construct,
                                         aar=aar_construct, rsr=reporting_shared)

        ######################################
        #     FULL APPLICATION CONSTRUCTS    #
        ######################################
        IntegrationConstruct(self, "integration",
                             tenant_provisioning_resources=tps_construct,
                             foundations_resources=foundations_construct,
                             insights_pipeline_resources=insights_pipeline_construct,
                             report_bucket=reporting_shared.bucket,
                             creating_resources_condition=self._is_deplopying_full_app_condition
                             )

        AdminPolicy(self, "admin-policy",
                    solution_buckets=solution_buckets_construct,
                    microservice=self.microservice,
                    data_lakes= self.data_lakes,
                    creating_resources_condition=self._is_deplopying_full_app_condition,
                    )

        ######################################
        #    OTHER CONDITIONAL CONSTRUCTS    #
        ######################################
        LakeformationSettings(
            self,
            "lakeformation-settings",
            dataset_resources=self.data_lakes.datasets.values(),
            pmn_resources=pmn_construct,
            datalake_condition=self._is_creating_datalake_condition,
            microservice_condition=self._is_creating_microservices_condition
        )

        CloudTrailIntegration(
            self, "cloudtrail",
            solution_buckets_resources=solution_buckets_construct,
            is_creating_datalake=self._is_creating_datalake,
            is_creating_microservice=self._is_creating_microservices,
            datalake=self.data_lakes,
            microservice=self.microservice
        )

    def _create_template_parameters(self):
        self._email_param = CfnParameter(
            self,
            id="NotificationEmail",
            type="String",
            description="Email address to notify subscriber of workflow query results.",
            max_length=50,
            allowed_pattern=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$|^$)",
            constraint_description="Must be a valid email address",
        )

        self._is_creating_microservices = CfnParameter(
            self,
            id="ShouldDeployMicroservices",
            description="Yes - Deploy the reporting microservices. \n No - Skip microservice deployment.",
            type="String",
            allowed_values=["Yes", "No"],
            default="Yes"
        )

        self._is_creating_datalake = CfnParameter(
            self,
            id="ShouldDeployDataLake",
            type="String",
            description="Yes - Deploy the data lake. \n No - Skip data lake deployment.",
            allowed_values=["Yes", "No"],
            default="Yes"
        )

    def _suppress_cfn_nag_warnings_on_stack(self):
        Aspects.of(self).add(
            CfnNagSuppressAll(
                [
                    CfnNagSuppression(
                        rule_id="W92",
                        reason="Lambda functions should define ReservedConcurrentExecutions to reserve simultaneous executions"),
                    CfnNagSuppression(
                        rule_id="W89",
                        reason="Lambda functions should be deployed inside a VPC"
                    ),
                    CfnNagSuppression(
                        rule_id="W58",
                        reason="Lambda functions require permission to write CloudWatch Logs"
                    )
                ],
                "AWS::Lambda::Function"
            )
        )
        Aspects.of(self).add(
            CfnNagSuppressAll(
                [
                    CfnNagSuppression(
                        rule_id="W84",
                        reason="CloudWatchLogs LogGroup should specify a KMS Key Id to encrypt the log data"),
                    CfnNagSuppression(
                        rule_id="W86",
                        reason="Log retention period is set to INFINITE instead of DAYS"),
                ],
                "AWS::Logs::LogGroup"
            )
        )
        Aspects.of(self).add(
            CfnNagSuppressAll(
                [
                    CfnNagSuppression(
                        rule_id="F10",
                        reason="Lambda functions with unique permissions use inline policies"),
                ],
                "AWS::IAM::Role"
            )
        )
        Aspects.of(self).add(
            CfnGuardSuppressAll(
                suppress=["CFN_NO_EXPLICIT_RESOURCE_NAMES"],
                resource_type="AWS::DynamoDB::Table"
            )
        )
        Aspects.of(self).add(
            CfnNagSuppressAll(
                [
                    CfnNagSuppression(
                        rule_id="W28",
                        reason="Third party dependency DDK Octagon tables require given resource names for functionality"),
                ],
                "AWS::DynamoDB::Table"
            ),
        )
        Aspects.of(self).add(
            CfnGuardSuppressAll(
                suppress=["IAM_POLICYDOCUMENT_NO_WILDCARD_RESOURCE"],
                resource_type="AWS::IAM::Role"
            )
        )

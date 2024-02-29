# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
from typing import Any
from constructs import Construct

from aws_solutions.cdk.cfn_nag import CfnNagSuppressAll, CfnNagSuppression
from data_lake.foundations.foundations_construct import FoundationsConstruct
from data_lake.pipelines.sdlf_pipeline import SDLFPipelineConstruct
from data_lake.datasets import SDLFDatasetConstruct
from amc_insights.microservices.platform_management_service import PlatformManagerSageMaker
from amc_insights.microservices.tenant_provisioning_service import TenantProvisioningService
from amc_insights.custom_resource.anonymized_operational_metrics import OperationalMetrics
from amc_insights.custom_resource.cloudwatch_metrics.cloudwatch_metrics import CloudwatchMetrics
from amc_insights.microservices.workflow_manager_service import WorkFlowManagerService
from aws_cdk import CfnParameter, CfnCondition, Fn, Aspects
from aws_solutions.cdk.stack import SolutionStack
from utils.datasets_config import DatasetsConfigs
from amc_insights.solution_buckets.solution_buckets import SolutionBuckets
from amc_insights.integration.integration_construct import IntegrationConstruct
from amc_insights.app_registry import AppRegistry
from amc_insights.custom_resource.user_scripts.user_scripts_construct import UserScriptsCustomResource
from amc_insights.custom_resource.user_iam import UserIam
from amc_insights.custom_resource.lakeformation_settings.lakeformation_settings import LakeformationSettings
from amc_insights.custom_resource.cloudtrail.cloudtrail_construct import CloudTrailConstruct


class _AMCDataset:
    def __init__(self):
        self.dataset = "amc"
        self.pipeline = "insights"
        self.stage_a_transform = "amc_light_transform"
        self.stage_b_transform = "amc_heavy_transform"


class AMCInsightsStack(SolutionStack):
    name = "amcinsights"
    description = "Amazon Marketing Cloud Insights"
    template_filename = "amazon-marketing-cloud-insights.template"

    def __init__(self, scope: Construct, id: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(scope, id, *args, **kwargs)
        self.synthesizer.bind(self)

        # Aspects
        self._suppress_cfn_nag_warnings_on_lambdas()
        Aspects.of(self).add(AppRegistry(self, f'AppRegistry-{id}'))

        self._environment_id = "dev"
        self._team = "adtech"
        self._amc_dataset = _AMCDataset()
        self._config_file_path = os.path.join(f"{Path(__file__).parent.parent}", "datasets_parameters.json")
        self.sdlf_dataset_constructs = []

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

        # Cloud Trail
        cloudtrail_construct = CloudTrailConstruct(
            self, "cloudtrail",
            solution_buckets_resources=solution_buckets_construct,
            data_lake_enabled=self._is_creating_datalake
        )

        ######################################
        #       DATA LAKE CONSTRUCTS         #
        ######################################
        # Foundations
        foundations_construct = FoundationsConstruct(self, "foundations",
                                                     environment_id=self._environment_id,
                                                     creating_resources_condition=self._is_creating_datalake_condition)

        # Insights Pipeline
        insights_pipeline_construct = SDLFPipelineConstruct(self, "data-lake-pipeline",
                                                            environment_id=self._environment_id,
                                                            team=self._team,
                                                            foundations_resources=foundations_construct,
                                                            pipeline=self._amc_dataset.pipeline,
                                                            creating_resources_condition=self._is_creating_datalake_condition
                                                            )

        # AMC Dataset
        amc_dataset_construct = SDLFDatasetConstruct(self, "data-lake-dataset-amc",
                                                     environment_id=self._environment_id,
                                                     team=self._team,
                                                     solution_buckets=solution_buckets_construct,
                                                     foundations_resources=foundations_construct,
                                                     dataset_parameters=self._amc_dataset,
                                                     creating_resources_condition=self._is_creating_datalake_condition
                                                     )
        self.sdlf_dataset_constructs.append(amc_dataset_construct)

        # Additional Datasets & Pipelines
        # Check if entries were added
        if os.stat(self._config_file_path).st_size != 0:
            datasets_configs = DatasetsConfigs(
                environment_id=self._environment_id,
                config_file_path=self._config_file_path).dataset_configs

            dataset_names = set()
            pipeline_names = set()

            # Do not recreate amc dataset or insights pipeline
            dataset_names.add(self._amc_dataset.dataset)
            pipeline_names.add(self._amc_dataset.pipeline)

            for dataset_parameters in datasets_configs:
                # Create new pipeline if not using insights pipeline
                pipeline_name = dataset_parameters.pipeline
                if pipeline_name not in pipeline_names:
                    pipeline_names.add(pipeline_name)
                    SDLFPipelineConstruct(self, "data-lake-pipeline",
                                          environment_id=self._environment_id,
                                          team=self._team,
                                          foundations_resources=foundations_construct,
                                          dataset_parameters=dataset_parameters,
                                          creating_resources_condition=self._is_creating_datalake_condition,
                                          )

                # Register datasets to the pipeline with concrete transformations and glue job
                dataset_name = dataset_parameters.dataset
                if dataset_name not in dataset_names:
                    dataset_names.add(dataset_name)
                    sdlf_dataset = SDLFDatasetConstruct(
                        self,
                        f"data-lake-dataset-{dataset_name}",
                        environment_id=self._environment_id,
                        team=self._team,
                        foundations_resources=foundations_construct,
                        dataset_parameters=dataset_parameters,
                        solution_buckets=solution_buckets_construct,
                        creating_resources_condition=self._is_creating_datalake_condition,
                    )
                    self.sdlf_dataset_constructs.append(sdlf_dataset)

        ######################################
        #    AMC MICROSERVICE CONSTRUCTS     #
        ######################################
        # WFM
        wfm_construct = WorkFlowManagerService(self, "wfm",
                                               team=self._team,
                                               email_parameter=self._email_param,
                                               creating_resources_condition=self._is_creating_microservices_condition)

        # TPS
        tps_construct = TenantProvisioningService(self, "tps",
                                                  team=self._team,
                                                  dataset=self._amc_dataset.dataset,
                                                  workflow_manager_resources=wfm_construct,
                                                  solution_buckets=solution_buckets_construct,
                                                  creating_resources_condition=self._is_creating_microservices_condition,
                                                  cloudtrail_resources=cloudtrail_construct,
                                                  data_lake_enabled=self._is_creating_datalake)

        # PMN
        pmn_construct = PlatformManagerSageMaker(self, "platform-manager",
                                                 environment_id=self._environment_id,
                                                 team=self._team,
                                                 workflow_manager_resources=wfm_construct,
                                                 tenant_provisioning_resources=tps_construct,
                                                 solution_buckets=solution_buckets_construct,
                                                 creating_resources_condition=self._is_creating_microservices_condition)

        ######################################
        #     FULL APPLICATION CONSTRUCTS    #
        ######################################
        IntegrationConstruct(self, "integration",
                             tenant_provisioning_resources=tps_construct,
                             foundations_resources=foundations_construct,
                             insights_pipeline_resources=insights_pipeline_construct,
                             creating_resources_condition=self._is_deplopying_full_app_condition
                             )

        UserIam(self, "useriam",
                solution_buckets=solution_buckets_construct,
                tps_resources=tps_construct,
                wfm_resources=wfm_construct,
                pmn_resources=pmn_construct,
                foundations_resources=foundations_construct,
                insights_pipeline_resources=insights_pipeline_construct,
                amc_dataset_resources=amc_dataset_construct,
                creating_resources_condition=self._is_deplopying_full_app_condition
                )

        ######################################
        #    OTHER CONDITIONAL CONSTRUCTS    #
        ######################################
        LakeformationSettings(
            self,
            "lakeformation-settings",
            dataset_resources=self.sdlf_dataset_constructs,
            pmn_resources=pmn_construct,
            datalake_condition=self._is_creating_datalake_condition,
            microservice_condition=self._is_creating_microservices_condition
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
            description="Yes - Deploy the Tenant Provisioning Service, Workflow Manager, and Platform Manager Notebooks. \n No - Skip microservice deployment.",
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

    def _suppress_cfn_nag_warnings_on_lambdas(self):
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
                ],
                "AWS::Logs::LogGroup"
            )
        )

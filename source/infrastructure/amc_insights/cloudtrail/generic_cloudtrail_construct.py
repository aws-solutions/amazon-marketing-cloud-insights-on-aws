# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List, Union

from aws_cdk import Aws, Aspects
import aws_cdk.aws_cloudtrail as cloud_trail
from constructs import Construct
from amc_insights.condition_aspect import ConditionAspect
from amc_insights.solution_buckets import SolutionBuckets
from amc_insights.microservices.workflow_manager_service import WorkFlowManagerService
from amc_insights.microservices.tenant_provisioning_service import TenantProvisioningService
from data_lake.foundations.foundations_construct import FoundationsConstruct
from data_lake.pipelines.sdlf_pipeline import SDLFPipelineConstruct


class GenericCloudTrailConstruct(Construct):
    def __init__(
            self,
            scope,
            id,
            condition,
            **constructs_to_tail
    ) -> None:
        """
        This construct creates a CloudTrail resource and sets Data Events to the CloudTrail.
        """
        super().__init__(scope, id)

        self.sdlf_pipelines: Union[List[SDLFPipelineConstruct], None] = None
        self.foundation_resources: Union[FoundationsConstruct, None] = None
        self.tps_resources: Union[TenantProvisioningService, None] = None
        self.wfm_resources: Union[WorkFlowManagerService, None] = None

        self.solution_buckets_resources: SolutionBuckets = constructs_to_tail.get("solution_buckets_resources")
        self.microservice = constructs_to_tail.get("microservice")
        self.datalake = constructs_to_tail.get("datalake")

        self.get_microservice_resources(self.microservice)
        self.get_datalake_resources(self.datalake)

        # Apply condition to resources in Construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", condition))

        self.trail = cloud_trail.Trail(
            self,
            "S3AndLambda",
            bucket=self.solution_buckets_resources.logging_bucket,
            is_multi_region_trail=False,
            include_global_service_events=True,
            management_events=cloud_trail.ReadWriteType.ALL,
        )

        self.trail.add_s3_event_selector([cloud_trail.S3EventSelector(bucket=bucket) for bucket in self.get_buckets()])
        self.trail.add_lambda_event_selector(handlers=self.get_lambda_handlers())

    def get_buckets(self):
        buckets = [self.solution_buckets_resources.artifacts_bucket]
        if self.foundation_resources:
            buckets.append(self.foundation_resources.raw_bucket)
            buckets.append(self.foundation_resources.stage_bucket)
            buckets.append(self.foundation_resources.athena_bucket)
        return buckets

    def get_lambda_handlers(self):
        handlers = []
        if self.wfm_resources:
            handlers.extend(self.wfm_resources.lambda_function_list)
        if self.tps_resources:
            handlers.extend(self.tps_resources.lambda_function_list)
        
        if self.foundation_resources:
            handlers.append(self.foundation_resources.register_function)
        if self.sdlf_pipelines:
            for sdlf_pipeline in self.sdlf_pipelines:
                handlers.append(sdlf_pipeline.routing_function)
                handlers.extend(sdlf_pipeline.stage_a_transform.lambda_functions)
                handlers.extend(sdlf_pipeline.stage_b_transform.lambda_functions)

        return handlers

    def get_microservice_resources(self, microservice):
        if microservice:
            self.wfm_resources: WorkFlowManagerService = self.microservice.wfm
            self.tps_resources: TenantProvisioningService = self.microservice.tps

    def get_datalake_resources(self, datalake):
        if datalake:
            self.foundation_resources: FoundationsConstruct = datalake.foundation
            self.sdlf_pipelines: List[SDLFPipelineConstruct] = list(datalake.sdlf_pipelines.values())

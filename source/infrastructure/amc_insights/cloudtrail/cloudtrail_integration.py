# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk import CfnCondition, Fn
from amc_insights.cloudtrail.generic_cloudtrail_construct import GenericCloudTrailConstruct
from constructs import Construct
from amc_insights.solution_buckets import SolutionBuckets


class CloudTrailIntegration(Construct):
    def __init__(
            self,
            scope,
            id,
            solution_buckets_resources: SolutionBuckets,
            is_creating_datalake,
            is_creating_microservice,
            datalake,
            microservice,
    ) -> None:
        """
        Conditionally create CloudTrail resource.
        """
        super().__init__(scope, id)

        self.solution_buckets_resources = solution_buckets_resources
        self.datalake = datalake
        self.microservice = microservice

        # There are three options to deploy: 1. deploy microservice only, 2. deploy datalake only,
        # 3. deploy both microservice and datalake.
        self.datalake_only_condition = CfnCondition(
            self,
            id="DataLakeDeploymentCondition",
            expression=Fn.condition_and(
                Fn.condition_equals(is_creating_datalake.value_as_string, "Yes"),
                Fn.condition_not(Fn.condition_equals(is_creating_microservice.value_as_string, "Yes"))
            )
        )

        self.microservice_only_condition = CfnCondition(
            self,
            id="MicroserviceDeploymentCondition",
            expression=Fn.condition_and(
                Fn.condition_equals(is_creating_microservice.value_as_string, "Yes"),
                Fn.condition_not(Fn.condition_equals(is_creating_datalake.value_as_string, "Yes"))
            )
        )
        self.full_application_condition = CfnCondition(
            self,
            id="DeployingFullApplication",
            expression=Fn.condition_equals(is_creating_microservice.value_as_string,
                                           is_creating_datalake.value_as_string)
        )

        # Create three generic cloudtrail construct here and apply condition to the construct, so only one cloudtrail
        # will be created at deployment.
        GenericCloudTrailConstruct(
            self, "DataLakeCloudTrail",
            condition=self.datalake_only_condition,
            solution_buckets_resources=self.solution_buckets_resources,
            datalake=self.datalake
        )

        GenericCloudTrailConstruct(self, "MicroserviceCloudTrail",
                                   condition=self.microservice_only_condition,
                                   solution_buckets_resources=self.solution_buckets_resources,
                                   microservice=self.microservice)

        GenericCloudTrailConstruct(self, "FullApplicationCloudTrail",
                                   condition=self.full_application_condition,
                                   solution_buckets_resources=self.solution_buckets_resources,
                                   datalake=self.datalake,
                                   microservice=self.microservice)

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
from aws_cdk.aws_lambda import LayerVersion
import aws_cdk.aws_lambda as _lambda
from constructs import Construct
from aws_cdk import Aws, Aspects, CfnCondition
from amc_insights.condition_aspect import ConditionAspect


class AwsLambdaLayers(Construct):
    def __init__(self, 
                scope: Construct, 
                id,
                creating_resources_condition: CfnCondition
    ) -> None:
        super().__init__(scope, id)
        
        # Apply condition to resources in WFM construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))
        
        self._resource_prefix = Aws.STACK_NAME
        
        self.metrics_layer = LayerVersion(
            self,
            "metrics-layer",
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "aws_lambda_layers/metrics_layer/"
                )
            ),
            layer_version_name=f"{self._resource_prefix}-metrics-layer",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
        )
        
        self.microservice_layer = LayerVersion(
            self,
            "microservice-layer",
            code=_lambda.Code.from_asset(
                path=os.path.join(
                    f"{Path(__file__).parents[1]}",
                    "aws_lambda_layers/microservice_layer/"
                )
            ),
            layer_version_name=f"{self._resource_prefix}-microservice-layer",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
        )
        
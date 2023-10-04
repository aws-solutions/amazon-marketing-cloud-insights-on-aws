# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import Any, Dict

import aws_cdk as cdk
from constructs import Construct


class RegisterConstruct(Construct):
    def __init__(self, scope, id: str, props: Dict[str, Any], register_lambda) -> None:
        super().__init__(scope, f"{id}-{props['type']}-register")
        register = cdk.CustomResource(
            self,
            f"{id}-{props['type']}-custom-resource",
            service_token=register_lambda.function_arn,
            properties={"RegisterProperties": props},
        )

        register.node.add_dependency(register_lambda)

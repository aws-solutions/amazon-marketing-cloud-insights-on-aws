# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ###############################################################################
# PURPOSE:
#   * Unit test for Workflow Manager Services.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/microservices/test_workflow_manager_service.py
###############################################################################


import uuid
from functools import wraps
from unittest.mock import MagicMock, patch
from contextlib import ExitStack, contextmanager
from amc_insights.microservices.workflow_manager_service.workflow_manager_services import WorkFlowManagerService


mock_paths = [
    "Aspects.of",
    "ConditionAspect",
    "super",
    "kms.Key",
    "Topic",
    "dynamodb.Table",
    "LayerVersion",
    "_lambda.Function",
    "Role",
    "Policy",
    "SolutionsLayer.get_or_create",
    "cdk.aws_iam.ArnPrincipal",
    "PolicyStatement",
    "stepfunctions.Choice",
    "stepfunctions.Wait",
    "stepfunctions.Fail",
    "stepfunctions.Succeed",
    "stepfunctions.Pass",
    "stepfunctions.StateMachine",
    "tasks.SnsPublish",
    "tasks.LambdaInvoke",
    "tasks.EvaluateExpression",
    "tasks.DynamoGetItem",
    "logs.LogGroup",
    "CfnOutput",
    "PowertoolsLayer.get_or_create"
]

mock_cls_path = "amc_insights.microservices.workflow_manager_service.workflow_manager_services"

@contextmanager
def handle_contexts(patched_services):
    with ExitStack() as exit_stack:
        yield [exit_stack.enter_context(patch_service) for patch_service in patched_services]


def mocked_services(test_func):
    @wraps(test_func)
    def wrapper():
        patched_services = tuple(list([patch(f"{mock_cls_path}.{mock_path}") for mock_path in mock_paths]))
        with handle_contexts(patched_services) as services:
            test_func()
            for service_index in range(0, len(patched_services)):
                services[service_index].assert_called()

    return wrapper


@mocked_services
def test_workflow_manager_service():
    mock_scope = MagicMock()
    id_num = str(uuid.uuid4())
    team = "test_team"
    email_parameter = MagicMock(value_as_string="test_wfm_email@example.com")
    creating_resources_condition = MagicMock()
    node_mock = MagicMock(node=MagicMock(return_value=None).try_get_context)
    WorkFlowManagerService.__init__(self=node_mock, scope=mock_scope, id=id_num, team=team, email_parameter=email_parameter, creating_resources_condition=creating_resources_condition)
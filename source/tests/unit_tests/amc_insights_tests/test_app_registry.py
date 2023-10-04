# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# ###############################################################################
# PURPOSE:
#   * Unit test for AmcInsight/AppRegistry.
# USAGE:
#   ./run-unit-tests.sh --test-file-name amc_insights_tests/test_app_registry.py
###############################################################################

from amc_insights.app_registry import AppRegistry
from unittest.mock import MagicMock, patch, call
import aws_cdk
from aws_cdk import Aws
from aws_cdk import aws_servicecatalogappregistry as appregistry
import pytest
import uuid

@pytest.fixture
def mock_scope():
    return MagicMock(name="test_scope", node=MagicMock(return_value=None).try_get_context)


@pytest.fixture
def mock_app_registry_cls(mock_scope):
    return MagicMock(
        solution_id=str(uuid.uuid1()),
        solution_name="test_solution",
        solution_version="0.0.0.1",
        _application={},
        _attribute_group={},
        __application_resource_association={},
        scope_name=mock_scope.name
    )


@pytest.fixture
def mock_aws_stack():
    return aws_cdk.Stack(id=f'TestAppRegistry-{str(uuid.uuid1())}', stack_name=f"test{str(uuid.uuid1())}")


@patch("amc_insights.app_registry.super")
def test_app_registry_cls_initialization(mock_super, mock_app_registry_cls, mock_scope):
    AppRegistry.__init__(mock_app_registry_cls, mock_scope, mock_scope)
    mock_super.assert_called_once()
    mock_scope.node.try_get_context.assert_called()
    mock_scope.node.try_get_context.assert_has_calls(
        [
           call("SOLUTION_NAME"),
           call("SOLUTION_ID"),
           call("SOLUTION_VERSION"),
        ]
    )
    assert mock_scope.node.try_get_context.call_count == 3
    assert mock_app_registry_cls.scope_name == mock_scope.name
    assert mock_app_registry_cls._application == {}
    assert mock_app_registry_cls._attribute_group == {}
    assert mock_app_registry_cls.__application_resource_association == {}
    assert mock_app_registry_cls._attribute_group_association == {}


def test_visit(mock_aws_stack):
    mock_cls = MagicMock()
    mock_cls.get_or_create_application.return_value="test"
    mock_cls.get_or_create_attribute_group.return_value="test"
    mock_cls.get_or_create_attribute_group_association.return_value="test"
    mock_cls.get_or_create_application_resource_association.return_value="test"

    AppRegistry.visit(mock_cls, "Test123")
    mock_cls.get_or_create_application.assert_not_called()
    mock_cls.get_or_create_attribute_group.assert_not_called()
    mock_cls.get_or_create_attribute_group_association.assert_not_called()

    AppRegistry.visit(mock_cls, mock_aws_stack)
    mock_cls.get_or_create_application.assert_called_once_with(mock_aws_stack)
    mock_cls.get_or_create_attribute_group.assert_called_once_with(mock_aws_stack)
    mock_cls.get_or_create_attribute_group_association.assert_called_once_with(
        mock_aws_stack, mock_cls.get_or_create_application(mock_aws_stack), 
        mock_cls.get_or_create_attribute_group(mock_aws_stack))


def test_get_or_create_application(mock_aws_stack, mock_app_registry_cls):
    mock_cls = mock_app_registry_cls
    application = AppRegistry.get_or_create_application(mock_cls, mock_aws_stack)
    assert application.stack.stack_name == mock_aws_stack.stack_name
    with pytest.raises(RuntimeError) as excinfo:
        appregistry.CfnApplication(
                    mock_aws_stack,
                    'AppRegistryApp',
                    name=application.name,
                    description=f"Service Catalog application to track and manage all your resources for the solution {mock_cls.solution_name}",
                    tags={
                        "Solutions:SolutionID": mock_cls.solution_id,
                        "Solutions:SolutionName": mock_cls.solution_name,
                        "Solutions:SolutionVersion": mock_cls.solution_version,
                    }
                )
    assert f"There is already a Construct with name 'AppRegistryApp' in Stack [{mock_aws_stack.to_string()}]" == str(excinfo.value)


def test_get_or_create_attribute_group(mock_aws_stack, mock_app_registry_cls):
    mock_cls = mock_app_registry_cls
    application = AppRegistry.get_or_create_attribute_group(mock_cls, mock_aws_stack)
    assert application.stack.stack_name == mock_aws_stack.stack_name
    with pytest.raises(RuntimeError) as excinfo:
        appregistry.CfnApplication(
                    mock_aws_stack,
                    'AppAttributeGroup',
                    name=application.name,
                    description="Attributes for Solutions Metadata",
                    tags={
                        "solutionID": mock_cls.solution_id,
                        "solutionName": mock_cls.solution_name,
                        "version": mock_cls.solution_version,
                    }
                )
    assert f"There is already a Construct with name 'AppAttributeGroup' in Stack [{mock_aws_stack.to_string()}]" == str(excinfo.value)


def test_get_or_create_attribute_group_association(mock_aws_stack, mock_app_registry_cls):
    mock_cls = mock_app_registry_cls
    application = AppRegistry.get_or_create_application(mock_cls, mock_aws_stack)
    attribute_group = AppRegistry.get_or_create_attribute_group(mock_cls, mock_aws_stack)
    AppRegistry.get_or_create_attribute_group_association(mock_cls, mock_aws_stack, application=application, attribute_group=attribute_group)
    with pytest.raises(RuntimeError) as excinfo:
       appregistry.CfnAttributeGroupAssociation(
                mock_aws_stack,
                'AttributeGroupAssociation',
                application=application.name,
                attribute_group=attribute_group.name,
            )
    assert f"There is already a Construct with name 'AttributeGroupAssociation' in Stack [{mock_aws_stack.to_string()}]" == str(excinfo.value)


def test_get_or_create_application_resource_association(mock_aws_stack, mock_app_registry_cls):
    application = AppRegistry.get_or_create_application(mock_app_registry_cls, mock_aws_stack)
    AppRegistry.get_or_create_application_resource_association(mock_app_registry_cls, mock_aws_stack, application=application)
    with pytest.raises(RuntimeError) as excinfo:
        appregistry.CfnResourceAssociation(
            mock_aws_stack,
            'AppResourceAssociation',
            application=application.name,
            resource=Aws.STACK_NAME,
            resource_type="CFN_STACK"
        )
    assert f"There is already a Construct with name 'AppResourceAssociation' in Stack [{mock_aws_stack.to_string()}]" == str(excinfo.value)

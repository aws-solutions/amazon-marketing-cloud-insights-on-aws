# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
This module is responsible for top-level tests against the template.
It does not test individual resources.
"""

# source/amc_insights_tests/test_amc_insights_template.py

from pathlib import Path
import pytest

from aws_cdk import App
from amc_insights.amc_insights_stack import AMCInsightsStack
from aws_cdk.assertions import Template

from aws_solutions.cdk import CDKSolution


@pytest.fixture(scope="module")
def mock_solution():
    path = Path(__file__).parent.parent.parent.parent / "infrastructure" / "cdk.json"
    return CDKSolution(cdk_json_path=path)


@pytest.fixture(scope="module")
def template(mock_solution):
    app = App(context=mock_solution.context.context)
    stack = AMCInsightsStack(
        app,
        AMCInsightsStack.name,
        description=AMCInsightsStack.description,
        template_filename=AMCInsightsStack.template_filename,
        synthesizer=mock_solution.synthesizer)
    yield Template.from_stack(stack)


def test_resource_counts(template):
    # test for a resource that will never exist
    assert len(template.find_resources("Never::Ever::Found")) == 0
    # count AWS::CloudFormation::CustomResource resources
    assert len(template.find_resources("AWS::CloudFormation::CustomResource")) > 0
    # count AWS::CloudTrail::Trail resources
    assert len(template.find_resources("AWS::CloudTrail::Trail")) > 0
    # count AWS::DynamoDB::Table resources
    assert len(template.find_resources("AWS::DynamoDB::Table")) > 0
    # count AWS::Events::Rule resources
    assert len(template.find_resources("AWS::Events::Rule")) > 0
    # count AWS::Glue::Database resources
    assert len(template.find_resources("AWS::Glue::Database")) > 0
    #  count AWS::Glue::Job resources
    assert len(template.find_resources("AWS::Glue::Job")) > 0
    # count AWS::IAM::ManagedPolicy resources
    assert len(template.find_resources("AWS::IAM::ManagedPolicy")) > 0
    # count AWS::IAM::Policy resources
    assert len(template.find_resources("AWS::IAM::Policy")) > 0
    # count AWS::IAM::Role resources
    assert len(template.find_resources("AWS::IAM::Role")) > 0
    # count AWS::KMS::Alias resources
    assert len(template.find_resources("AWS::KMS::Alias")) > 0
    # count AWS::KMS::Key resources
    assert len(template.find_resources("AWS::KMS::Key")) > 0
    # count AWS::LakeFormation::DataLakeSettings resources
    assert len(template.find_resources("AWS::LakeFormation::DataLakeSettings")) > 0
    # count AWS::LakeFormation::Permissions resources
    assert len(template.find_resources("AWS::LakeFormation::Permissions")) > 0
    # count AWS::LakeFormation::Resource
    assert len(template.find_resources("AWS::LakeFormation::Resource")) > 0
    # count AWS::Lambda::EventSourceMapping resources
    assert len(template.find_resources("AWS::Lambda::EventSourceMapping")) > 0
    # count AWS::Lambda::Function resources
    assert len(template.find_resources("AWS::Lambda::Function")) > 0
    # count AWS::Lambda::LayerVersion resources
    assert len(template.find_resources("AWS::Lambda::LayerVersion")) > 0
    # count AWS::Lambda::Permission resources
    assert len(template.find_resources("AWS::Lambda::Permission")) > 0
    # count AWS::S3::Bucket resources
    assert len(template.find_resources("AWS::S3::Bucket")) > 0
    # count AWS::S3::BucketPolicy resources
    assert len(template.find_resources("AWS::S3::BucketPolicy")) > 0
    # count AWS::SNS::Subscription resources
    assert len(template.find_resources("AWS::SNS::Subscription")) > 0
    # count AWS::SNS::Topic resources
    assert len(template.find_resources("AWS::SNS::Topic")) > 0
    # count AWS::SQS::Queue resources
    assert len(template.find_resources("AWS::SQS::Queue")) > 0
    # count AWS::SQS::QueuePolicy resources
    assert len(template.find_resources("AWS::SQS::QueuePolicy")) == 0
    # count AWS::SageMaker::NotebookInstance resources
    assert len(template.find_resources("AWS::SageMaker::NotebookInstance")) > 0
    # count AWS::SageMaker::NotebookInstanceLifecycleConfig resources
    assert len(template.find_resources(
        "AWS::SageMaker::NotebookInstanceLifecycleConfig")) > 0
    # count AWS::Serverless::Application resources
    assert len(template.find_resources("AWS::Serverless::Application")) == 0
    # count AWS::StepFunctions::StateMachine resources
    assert len(template.find_resources("AWS::StepFunctions::StateMachine")) > 0


def test_stack_parameters(template):
    template.find_parameters("NotificationEmail")
    template.find_parameters("ResourcePrefix")
    template.find_parameters("Team")
    template.find_parameters("Dataset")
    template.find_parameters("Pipeline")
    template.find_parameters("EnvironmentId")
    template.find_parameters("BootstrapVersion")

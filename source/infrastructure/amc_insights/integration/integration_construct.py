# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from amc_insights.condition_aspect import ConditionAspect
from constructs import Construct
from aws_cdk.aws_iam import Effect, PolicyStatement, Policy
from aws_cdk import Aspects

class IntegrationConstruct(Construct):
    def __init__(
            self,
            scope: Construct,
            id: str,
            tenant_provisioning_resources,
            foundations_resources,
            insights_pipeline_resources,
            creating_resources_condition
    ) -> None:
        super().__init__(scope, id)

        self._tenant_provisioning_resources = tenant_provisioning_resources
        self._foundations_resources = foundations_resources
        self._insights_pipeline_resources = insights_pipeline_resources

        # Apply condition to resources in Construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        kms_policy_statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "kms:DescribeKey",
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:ReEncrypt*",
                "kms:GenerateDataKey*",
                "kms:CreateGrant*"
            ],
            resources=[
                self._foundations_resources.customer_config_table_key.key_arn
                ],
        )
        kms_policy = Policy(
            self, "postDeployMetadataInstanceIntegrationConfigPolicy",
            statements=[kms_policy_statement]
        )
        kms_policy.attach_to_role(self._tenant_provisioning_resources._amc_instance_post_deploy_metadata.role)

        lambda_policy_statement = PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "lambda:AddPermission",
                        "lambda:RemovePermission"
                    ],
                    resources=[self._insights_pipeline_resources._routing_function.function_arn], 
        )
        lambda_policy = Policy(
            self, "routingQueueLambdaIntegrationPolicy",
            statements=[lambda_policy_statement]
        )
        lambda_policy.attach_to_role(self._tenant_provisioning_resources._add_amc_instance.role)

        dynamodb_policy = Policy(
            self, 'SDLFCustomerConfigIntegrationPolicy',
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "dynamodb:BatchGetItem",
                        "dynamodb:DescribeTable",
                        "dynamodb:GetItem",
                        "dynamodb:GetRecords",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:BatchWriteItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:PutItem"
                    ],
                    resources=[self._foundations_resources.customer_config_table.table_arn]
                )
            ]
        )
        dynamodb_policy.attach_to_role(self._tenant_provisioning_resources._amc_instance_post_deploy_metadata.role)

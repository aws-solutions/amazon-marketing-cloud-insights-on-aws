# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from amc_insights.condition_aspect import ConditionAspect
from constructs import Construct
from aws_cdk.aws_iam import Effect, PolicyStatement, Policy
from aws_cdk import Aspects, Aws
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets

class IntegrationConstruct(Construct):
    def __init__(
            self,
            scope: Construct,
            id: str,
            tenant_provisioning_resources,
            foundations_resources,
            insights_pipeline_resources,
            report_bucket,
            creating_resources_condition
    ) -> None:
        super().__init__(scope, id)

        self._tenant_provisioning_resources = tenant_provisioning_resources
        self._foundations_resources = foundations_resources
        self._insights_pipeline_resources = insights_pipeline_resources
        self._report_bucket = report_bucket

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
        kms_policy.attach_to_role(self._tenant_provisioning_resources.amc_instance_post_deploy_metadata.role)

        lambda_policy_statement = PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "lambda:AddPermission",
                        "lambda:RemovePermission"
                    ],
                    resources=[self._insights_pipeline_resources.routing_function.function_arn],
        )
        lambda_policy = Policy(
            self, "routingQueueLambdaIntegrationPolicy",
            statements=[lambda_policy_statement]
        )
        lambda_policy.attach_to_role(self._tenant_provisioning_resources.add_amc_instance.role)

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
        dynamodb_policy.attach_to_role(self._tenant_provisioning_resources.amc_instance_post_deploy_metadata.role)
        
        # create event to trigger data lake when files land in reporting bucket
        events.Rule(
            self,
            id="reports-bucket-event-capture",
            rule_name=f"{Aws.STACK_NAME}-reports-bucket-event-capture",
            description="Capture data landing in the reports bucket",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail={
                    "eventSource": [
                        "s3.amazonaws.com"
                    ],
                    "eventName": [
                            "CopyObject",
                            "CompleteMultipartUpload",
                            "PutObject",
                            "DeleteObject"
                        ],
                    "requestParameters": {
                        "bucketName": [
                            self._report_bucket.bucket_name
                        ],
                    }
                },
            ),
            targets=[targets.LambdaFunction(self._insights_pipeline_resources.routing_function)]
        )
        self._insights_pipeline_resources.routing_function.node.add_dependency(self._report_bucket)

        # grant stage-a processing lambda permission to access reporting bucket
        self._report_bucket.grant_read_write(self._insights_pipeline_resources.stage_a_transform._process_lambda)
        self._report_bucket.encryption_key.grant_decrypt(self._insights_pipeline_resources.stage_a_transform._process_lambda)

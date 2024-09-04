# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import os
from pathlib import Path
from typing import Any, Dict

import aws_cdk as cdk
import aws_cdk.aws_dynamodb as DDB
import aws_cdk.aws_kms as kms
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_s3 as s3
from aws_cdk.aws_iam import Effect, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk.aws_kms import Key
from aws_cdk.aws_lakeformation import CfnResource
from aws_cdk.aws_lambda import Code, Runtime, LayerVersion
from aws_cdk.aws_s3 import Bucket, BucketEncryption, BlockPublicAccess, BucketAccessControl
from aws_cdk.aws_ssm import StringParameter
from constructs import Construct
from aws_solutions.cdk.cfn_nag import CfnNagSuppression, add_cfn_nag_suppressions
from aws_cdk import Aws, Aspects, CfnOutput
from amc_insights.condition_aspect import ConditionAspect
from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from data_lake.foundations import REGISTER_LAMBDA_FUNCTION_PATH
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer


class FoundationsConstruct(Construct):
    def __init__(
            self,
            scope: Construct,
            id: str,
            environment_id: str,
            creating_resources_condition,
    ) -> None:
        super().__init__(scope, id)

        # Apply condition to resources in foundations construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._environment_id: str = environment_id
        self._resource_prefix = Aws.STACK_NAME
        self._creating_resources_condition = creating_resources_condition

        # CustomerConfig and Octagon DDB Table
        self._create_customer_config_table()
        self._create_octagon_tables()

        # Create Lambda layers
        self._create_lambda_layers()

        # Registers Datasets, Pipelines and Stages
        self._create_register()

        self._create_lakeformation_bucket_registration_role()

        # Create Buckets
        self.raw_bucket, self.raw_bucket_key = self._create_bucket(name="raw")
        self.stage_bucket, self.stage_bucket_key = self._create_bucket(name="stage")
        self.athena_bucket, self.athena_bucket_key = self._create_bucket(name="athena")

        # Create Output Links for Buckets
        self._create_bucket_output_link(id_name="Raw", bucket_name=self.raw_bucket.bucket_name)
        self._create_bucket_output_link(id_name="Stage", bucket_name=self.stage_bucket.bucket_name)
        self._create_bucket_output_link(id_name="Athena", bucket_name=self.athena_bucket.bucket_name)

        # Create Output Links for Tables
        self._create_table_output_link(id_name="Octagon Datasets", table_name=self.datasets.table_name)
        self._create_table_output_link(id_name="Octagon Metadata", table_name=self.object_metadata.table_name)
        self._create_table_output_link(id_name="Octagon Pipeline Execution History", table_name=self.peh.table_name)
        self._create_table_output_link(id_name="Octagon Pipelines", table_name=self.pipelines.table_name)

    def _create_customer_config_table(self) -> None:
        self.customer_config_table, self.customer_config_table_key = self._create_customer_config_ddb_table(
            name=f"data-lake-customer-config-{self._environment_id}",
            ddb_props={"partition_key": DDB.Attribute(name="customer_hash_key", type=DDB.AttributeType.STRING),
                       "sort_key": DDB.Attribute(name="hash_key", type=DDB.AttributeType.STRING)},
        )

    def _create_octagon_tables(self) -> None:
        self.object_metadata = self._create_octagon_ddb_table(
            id="metadata",
            name=f"octagon-ObjectMetadata-{self._environment_id}-{self._resource_prefix}",
            ddb_props={"partition_key": DDB.Attribute(name="id", type=DDB.AttributeType.STRING)},
        )

        self.datasets = self._create_octagon_ddb_table(
            id="datasets",
            name=f"octagon-Datasets-{self._environment_id}-{self._resource_prefix}",
            ddb_props={"partition_key": DDB.Attribute(name="name", type=DDB.AttributeType.STRING)},
        )

        self.pipelines = self._create_octagon_ddb_table(
            id="pipelines",
            name=f"octagon-Pipelines-{self._environment_id}-{self._resource_prefix}",
            ddb_props={"partition_key": DDB.Attribute(name="name", type=DDB.AttributeType.STRING)},
        )
        self.peh = self._create_octagon_ddb_table(
            id="peh",
            name=f"octagon-PipelineExecutionHistory-{self._environment_id}-{self._resource_prefix}",
            ddb_props={"partition_key": DDB.Attribute(name="id", type=DDB.AttributeType.STRING)},
        )

        for table in [self.object_metadata, self.datasets, self.pipelines, self.peh]:
            add_cfn_nag_suppressions(
                table.node.default_child,
                [
                    CfnNagSuppression(rule_id="W28",
                                      reason="Resource found with an explicit name, this disallows updates that require replacement of this resource"),
                ]
            )

    def _create_customer_config_ddb_table(self, name: str, ddb_props: Dict[str, Any]) -> DDB.Table:
        # ddb kms key resource
        table_key: Key = kms.Key(
            self,
            id="customer-config-table-key",
            description="Customer Config DDB Table Key",
            alias=f"alias/{self._resource_prefix}-{name}-ddb-table-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # ddb resource
        table: DDB.Table = DDB.Table(
            self,
            "sdlf-CustomerConfig",
            encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=table_key,
            billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
            **ddb_props,
        )

        table.add_global_secondary_index(
            index_name="amc-index",
            partition_key=DDB.Attribute(name="hash_key", type=DDB.AttributeType.STRING)
        )

        # SSM for ddb table arn
        StringParameter(
            self,
            "customer-config-table-arn",
            parameter_name=f"/{self._resource_prefix}/DynamoDB/DataLake/CustomerConfig",
            simple_name=True,
            string_value=table.table_name,
        )

        return table, table_key

    def _create_octagon_ddb_table(self, id: str, name: str, ddb_props: Dict[str, Any]) -> DDB.Table:
        table_name = name.split("-")[1]

        # ddb kms key resource
        table_key: Key = kms.Key(
            self,
            id=f"{id}-table-key",
            description=f"{id.title()} Table Key",
            alias=f"alias/{self._resource_prefix}-{name}-ddb-table-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # ddb resource
        table: DDB.Table = DDB.Table(
            self,
            f"{id}-table",
            table_name=name,
            encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=table_key,
            billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
            **ddb_props,
        )

        # SSM for ddb table name
        # only ObjectMetadata nad Datasets ssm are used
        StringParameter(
            self,
            f"{table_name}-table-name",
            parameter_name=f"/{self._resource_prefix}/DynamoDB/{table_name}",
            simple_name=True,
            string_value=table.table_name,
        )

        return table

    def _create_register(self) -> None:
        self.register_function = SolutionsPythonFunction(
            self,
            "RegisterFunction",
            REGISTER_LAMBDA_FUNCTION_PATH / "foundations" / "lambdas" / "register" / "handler.py",
            "event_handler",
            memory_size=256,
            description="Registers Datasets, Pipelines and Stages into their respective DynamoDB tables",
            timeout=cdk.Duration.minutes(1),
            runtime=Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "OCTAGON_DATASET_TABLE_NAME": self.datasets.table_name,
                "OCTAGON_PIPELINE_TABLE_NAME": self.pipelines.table_name,
            },
            layers=[
                SolutionsLayer.get_or_create(self),
                self.powertools_layer
            ],
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="register-function-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-register-function-lambda-alarm",
            lambda_function=self.register_function
        )

        self.datasets.grant_read_write_data(self.register_function)
        self.pipelines.grant_read_write_data(self.register_function)

        self.register_function.node.add_dependency(self.powertools_layer)

    def _create_lakeformation_bucket_registration_role(self) -> None:
        self.lakeformation_bucket_registration_role: Role = Role(
            self,
            "lakeformation-bucket-registry-role",
            assumed_by=ServicePrincipal("lakeformation.amazonaws.com"),
            inline_policies={
                "LakeFormationDataAccessPolicyForS3": PolicyDocument(
                    statements=[
                        PolicyStatement(
                            effect=Effect.ALLOW,
                            actions=["s3:ListBucket"],
                            resources=[
                                f"arn:{Aws.PARTITION}:s3:::{self._resource_prefix}*"
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceAccount": [
                                        f"{Aws.ACCOUNT_ID}"
                                    ]
                                }
                            }
                        ),
                        PolicyStatement(
                            effect=Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject"

                            ],
                            resources=[
                                f"arn:{Aws.PARTITION}:s3:::{self._resource_prefix}*/*"
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceAccount": [
                                        f"{Aws.ACCOUNT_ID}"
                                    ]
                                }
                            }
                        ),
                    ]
                )
            },
        )

    def _create_bucket(self, name: str) -> tuple[Bucket, Key]:
        bucket_key: Key = kms.Key(
            self,
            id=f"{name}-bucket-key",
            description=f"{name.title()} Bucket Key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # Only StageBucketKeyArn is used in the data lake library amc_light_transform and amc_heavy_transform
        StringParameter(
            self,
            f"{name}-bucket-key-arn",
            parameter_name=f"/{self._resource_prefix}/KMS/{name.title()}BucketKeyArn",
            simple_name=True,
            string_value=bucket_key.key_arn,
        )

        bucket: Bucket = s3.Bucket(
            self,
            id=f"{name}-bucket",
            encryption=BucketEncryption.KMS,
            encryption_key=bucket_key,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            block_public_access=BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            versioned=True,
            enforce_ssl=True
        )

        # Only StageBucket is used in the data lake library
        StringParameter(
            self,
            f"{name}-bucket-name",
            parameter_name=f"/{self._resource_prefix}/S3/{name.title()}Bucket",
            simple_name=True,
            string_value=bucket.bucket_name,
        )

        CfnResource(
            self,
            f"{name}-bucket-lakeformation-registry",
            resource_arn=bucket.bucket_arn,
            use_service_linked_role=False,
            role_arn=self.lakeformation_bucket_registration_role.role_arn,
        )

        bucket_key.add_to_resource_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey*",
                    "kms:ReEncrypt*",
                ],
                resources=["*"],
                principals=[self.lakeformation_bucket_registration_role],
            )
        )

        add_cfn_nag_suppressions(
            bucket.node.default_child,
            [
                CfnNagSuppression(rule_id="W51", reason="S3 bucket should likely have a bucket policy"),
                CfnNagSuppression(rule_id="W35", reason="S3 Bucket should have access logging configured")

            ]
        )

        return bucket, bucket_key

    def _create_lambda_layers(self) -> None:
        # Update Supported AWS Regions in IG
        self.wrangler_layer = LayerVersion.from_layer_version_arn(
            self,
            "data-wrangler-layer",
            # https://aws-sdk-pandas.readthedocs.io/en/3.9.0/layers.html
            f"arn:aws:lambda:{Aws.REGION}:336392948345:layer:AWSSDKPandas-Python311-Arm64:16"
        )

        self.powertools_layer = PowertoolsLayer.get_or_create(self)

        self.data_lake_library_layer = LayerVersion(
            self,
            "data-lake-layer",
            layer_version_name="data-lake-library",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambda_layers/data_lake_library")),
            compatible_runtimes=[Runtime.PYTHON_3_11],
            description=f"{self._resource_prefix} Data Lake Library",
            license="Apache-2.0",
        )

    def _create_bucket_output_link(self, id_name: str, bucket_name):
        CfnOutput(
            self,
            id=f"{id_name}Bucket",
            description=f"Use this link to access the {id_name} bucket",
            value=f"https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}",
            condition=self._creating_resources_condition
        )

    def _create_table_output_link(self, id_name: str, table_name):
        CfnOutput(
            self,
            id=f"{id_name}Table",
            description=f"Use this link to access the {id_name} table",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/dynamodbv2/home?region={Aws.REGION}#table?name={table_name}",
            condition=self._creating_resources_condition
        )

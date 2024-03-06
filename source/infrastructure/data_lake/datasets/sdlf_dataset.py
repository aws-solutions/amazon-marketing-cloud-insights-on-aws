# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from aws_cdk.aws_glue import CfnJob
from aws_cdk.aws_sqs import DeadLetterQueue, QueueEncryption
from aws_cdk.aws_glue import CfnDatabase
from aws_cdk.aws_iam import ServicePrincipal, PolicyDocument, PolicyStatement, Effect, ManagedPolicy, Role
from aws_cdk.aws_lakeformation import CfnPermissions
import aws_cdk.aws_lakeformation as lakeformation
from aws_cdk.aws_events import CfnRule
from aws_cdk.aws_lambda import CfnPermission
import aws_cdk.aws_kms as kms
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_cloudwatch as cloudwatch
from constructs import Construct
from aws_cdk import Duration, RemovalPolicy
from aws_cdk.aws_ssm import StringParameter
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from data_lake.register.register_construct import RegisterConstruct
from data_lake.foundations.foundations_construct import FoundationsConstruct
from aws_cdk import Aws, Aspects
from amc_insights.condition_aspect import ConditionAspect
from data_lake.glue.glue_scripts_uploader import GlueScriptsUploader


class SDLFDatasetConstruct(Construct):
    """
    Register a dataset for the SDLF pipeline
    """

    def __init__(self,
                 scope: Construct,
                 id: str,
                 environment_id: str,
                 team: str,
                 foundations_resources: FoundationsConstruct,
                 dataset_parameters,
                 solution_buckets,
                 creating_resources_condition,
                 ) -> None:
        super().__init__(scope, id)

        # Apply condition to resources pipeline construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._environment_id: str = environment_id
        self._team = team
        self._pipeline = dataset_parameters.pipeline
        self._dataset = dataset_parameters.dataset
        self._foundations_resources = foundations_resources
        self._stage_a_transform = dataset_parameters.stage_a_transform
        self._stage_b_transform = dataset_parameters.stage_b_transform
        self._solution_buckets = solution_buckets

        self._resource_prefix = Aws.STACK_NAME

        # glue script location in S3 bucket
        self._glue_prefix = "data_lake/sdlf_heavy_transform/glue"
        self._glue_script_path = f"{self._glue_prefix}/{self._team}/{self._dataset}/main.py"

        # glue script location
        self._glue_script_local_file_path = f"sdlf_heavy_transform/{self._team}/{self._dataset}/main.py"

        GlueScriptsUploader(
            self,
            "SyncGlueScripts",
            self._solution_buckets,
            self._dataset,
            self._glue_prefix,
            self._glue_script_path,
            self._glue_script_local_file_path
        )

        self._register_octagon_configs()

        self._create_sdlf_glue_job_role()
        self._create_sdlf_stage_b_glue_job()
        self._create_glue_database()

        self._create_routing_queue_and_event_bridge_rule()

    def _register_octagon_configs(self):
        self.stage_a_transform: str = self._stage_a_transform if self._stage_a_transform else "light_transform_blueprint"
        self.stage_b_transform: str = self._stage_b_transform if self._stage_b_transform else "heavy_transform_blueprint"

        self._props = {
            "id": "sdlf-dataset",
            "description": "sdlf dataset",
            "name": f"{self._team}-{self._dataset}",
            "type": "octagon_dataset",
            "pipeline": self._pipeline,
            "max_items_process": {
                "stage_b": 100,
                "stage_c": 100
            },
            "min_items_process": {
                "stage_b": 1,
                "stage_c": 1
            },
            "version": 1,
            "transforms": {
                "stage_a_transform": self.stage_a_transform,
                "stage_b_transform": self.stage_b_transform,
            }
        }

        RegisterConstruct(self, self._props["id"], props=self._props,
                          register_lambda=self._foundations_resources.register_function)

    def _create_sdlf_glue_job_role(self):
        glue_policy_document = PolicyDocument(
            statements=[
                PolicyStatement(
                    actions=[
                        "glue:*Database*",
                        "glue:*Table*",
                        "glue:*Partition*",
                    ],
                    resources=[
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{self._resource_prefix}_datalake_{self._environment_id}_{self._team}_{self._dataset}_db",
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{self._resource_prefix}_datalake_{self._environment_id}_{self._team}_{self._dataset}_db/*",
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/default",
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/global_temp",
                    ],
                    effect=Effect.ALLOW,
                ),
                # Grant permission to record metrics in cloudwatch.
                # This is needed for anonymized metrics.
                PolicyStatement(
                    actions=[
                        "cloudwatch:PutMetricData"
                    ],
                    resources=[
                        "*"  # NOSONAR
                    ],
                    conditions={
                        "StringEquals": {
                            "cloudwatch:namespace": self.node.try_get_context("METRICS_NAMESPACE")
                        }
                    },
                    effect=Effect.ALLOW,
                ),
                PolicyStatement(
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources=[
                        f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws-glue/*"
                    ],
                    effect=Effect.ALLOW,
                ),
            ]
        )
        self.glue_role: Role = Role(
            self,
            "glue-stageb-job-role",
            assumed_by=ServicePrincipal("glue.amazonaws.com"),
            inline_policies={
                "GlueRolePolicy": glue_policy_document,
            },
        )

        add_cfn_nag_suppressions(
            self.glue_role.node.default_child,
            [
                CfnNagSuppression(rule_id="W11",
                                  reason="IAM role should not allow * resource on its permissions policy")
            ]
        )

        ManagedPolicy(
            self,
            "glue-job-policy",
            roles=[self.glue_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey",
                            "kms:GenerateDataKeyPair",
                            "kms:GenerateDataKeyPairWithoutPlaintext",
                            "kms:GenerateDataKeyWithoutPlaintext",
                            "kms:ReEncryptTo",
                            "kms:ReEncryptFrom"
                        ],
                        resources=[
                            self._solution_buckets.artifacts_bucket_key.key_arn,
                            self._foundations_resources.raw_bucket_key.key_arn,
                            self._foundations_resources.stage_bucket_key.key_arn
                        ],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket"
                        ],
                        resources=[
                            self._solution_buckets.artifacts_bucket.bucket_arn,
                            f"{self._solution_buckets.artifacts_bucket.bucket_arn}/*",
                            self._foundations_resources.raw_bucket.bucket_arn,
                            f"{self._foundations_resources.raw_bucket.bucket_arn}/*",
                            self._foundations_resources.stage_bucket.bucket_arn,
                            f"{self._foundations_resources.stage_bucket.bucket_arn}/*"
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
            ),
        )

    def _create_sdlf_stage_b_glue_job(self) -> None:
        self.job: CfnJob = CfnJob(
            self,
            "sdlf-heavy-transform-glue-job",
            name=f"{self._resource_prefix}-{self._team}-{self._dataset}-glue-job",
            glue_version="2.0",
            allocated_capacity=2,
            execution_property=CfnJob.ExecutionPropertyProperty(max_concurrent_runs=4),
            command=CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self._solution_buckets.artifacts_bucket.bucket_name}/{self._glue_script_path}",
            ),
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "",
                "--additional-python-modules": "awswrangler==2.4.0,aws-lambda-powertools==2.15.0",
                "--enable-job-insights": "true",
                "--SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "--SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "--METRICS_NAMESPACE": self.node.try_get_context("METRICS_NAMESPACE"),
                "--RESOURCE_PREFIX": self._resource_prefix,
            },
            role=self.glue_role.role_arn,
        )

        StringParameter(
            self,
            f"amc-heavy-transform-{self._team}-{self._dataset}-job-name",
            parameter_name=f"/{self._resource_prefix}/Glue/{self._team}/{self._dataset}/SDLFHeavyTransformJobName",
            simple_name=True,
            string_value=self.job.name,  # type: ignore
        )

    def _create_glue_database(self) -> None:
        datalake_settings = lakeformation.CfnDataLakeSettings(
            self,
            "SDLFDatasetDataLakeSettings",
            admins=[
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=self.glue_role.role_arn
                )])
        datalake_settings.node.add_dependency(self.glue_role)

        database_name = f"{self._resource_prefix}_datalake_{self._environment_id}_{self._team}_{self._dataset}_db"
        database: CfnDatabase = CfnDatabase(
            self,
            "database",
            database_input=CfnDatabase.DatabaseInputProperty(
                name=database_name
            ),
            catalog_id=Aws.ACCOUNT_ID,
        )

        CfnPermissions(
            self,
            "glue-job-database-lakeformation-permissions",
            data_lake_principal=CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=self.glue_role.role_arn
            ),
            resource=CfnPermissions.ResourceProperty(
                database_resource=CfnPermissions.DatabaseResourceProperty(name=database.ref)
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP"],
        )

        StringParameter(
            self,
            f"amc-{self._team}-{self._dataset}-stage-catalog",
            parameter_name=f"/{self._resource_prefix}/Glue/{self._team}/{self._dataset}/StageDataCatalog",
            simple_name=True,
            string_value=database_name
        )

    def _create_routing_queue_and_event_bridge_rule(self):
        # SQS and DLQ
        # sqs kms key resource
        sqs_key = kms.Key(
            self,
            id="sqs-key-b",
            description="SQS Key Stage B",
            alias=f"alias/{self._resource_prefix}-{self._team}-{self._dataset}-sqs-stage-b-key",
            enable_key_rotation=True,
            pending_window=Duration.days(30),
            removal_policy=RemovalPolicy.DESTROY,
        )

        routing_dlq = DeadLetterQueue(
            max_receive_count=1,
            queue=sqs.Queue(self,
                            id='amc-dlq-b',
                            queue_name=f'{self._resource_prefix}-{self._team}-{self._dataset}-dlq-b.fifo',
                            fifo=True,
                            visibility_timeout=Duration.seconds(60),
                            encryption=QueueEncryption.KMS,
                            encryption_master_key=sqs_key))

        cloudwatch.Alarm(
            self,
            id='alarm-dlq-b',
            alarm_description='CloudWatch Alarm for Routing DLQ B',
            metric=routing_dlq.queue.metric('ApproximateNumberOfMessagesVisible', period=Duration.seconds(60)),
            evaluation_periods=1,
            datapoints_to_alarm=1,
            threshold=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
        )

        StringParameter(
            self,
            'amc-dlq-b.fifo-ssm',
            parameter_name=f"/{self._resource_prefix}/SQS/{self._team}/{self._dataset}StageBDLQ",
            simple_name=True,
            string_value=f'{self._resource_prefix}-{self._team}-{self._dataset}-dlq-b.fifo',
        )

        sqs.Queue(
            self,
            id='amc-queue-b',
            queue_name=f'{self._resource_prefix}-{self._team}-{self._dataset}-queue-b.fifo',
            fifo=True,
            visibility_timeout=Duration.seconds(60),
            encryption=QueueEncryption.KMS,
            encryption_master_key=sqs_key,
            dead_letter_queue=routing_dlq)

        StringParameter(
            self,
            'amc-queue-b.fifo-ssm',
            parameter_name=f"/{self._resource_prefix}/SQS/{self._team}/{self._dataset}StageBQueue",
            simple_name=True,
            string_value=f'{self._resource_prefix}-{self._team}-{self._dataset}-queue-b.fifo',
        )

        # Eventbridge and event source mapping
        post_state_rule = CfnRule(
            self,
            "rule-b",
            name=f"{self._resource_prefix}-{self._team}-{self._dataset}-rule-b",
            schedule_expression="cron(*/5 * * * ? *)",
            state="ENABLED",
            targets=[CfnRule.TargetProperty(
                arn=f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:{self._resource_prefix}-{self._team}-{self._pipeline}-routing-b",
                id="target-rule-b",
                input=json.dumps({
                    "team": self._team,
                    "pipeline": self._pipeline,
                    "pipeline_stage": "StageB",
                    "dataset": self._dataset,
                    "env": self._environment_id
                }, indent=4)
            )])

        CfnPermission(
            self,
            "sdlf-dataset-routing-b",
            action="lambda:InvokeFunction",
            function_name=f"{self._resource_prefix}-{self._team}-{self._pipeline}-routing-b",
            principal="events.amazonaws.com",
            source_arn=post_state_rule.attr_arn
        )

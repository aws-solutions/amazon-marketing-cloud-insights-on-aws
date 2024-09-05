# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from constructs import Construct
import aws_cdk.aws_iam as iam
from aws_cdk import Aws, CfnOutput, Aspects
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression

from amc_insights.condition_aspect import ConditionAspect


class AdminPolicy(Construct):
    def __init__(
            self,
            scope,
            id,
            solution_buckets,
            microservice,
            data_lakes,
            creating_resources_condition,
    ) -> None:
        super().__init__(scope, id)

        self._solution_buckets = solution_buckets
        self._microservice = microservice
        self._foundations_resources = data_lakes.foundation
        self._resource_prefix = Aws.STACK_NAME
        self._insights_pipeline_resources = data_lakes.sdlf_pipelines["insights"]
        self._dataset_resources = data_lakes.datasets.values()
        self._amc_secret = microservice.wfm.amc_secrets_manager
        self._sp_secret = microservice.spr.selling_partner_secrets_manager
        self._creating_resources_condition = creating_resources_condition

        # Apply condition to resources in Construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._create_microservice_admin_policy()
        self._create_datalake_admin_policy()
        self._create_cfn_output()

    def _create_microservice_admin_policy(self):
        STACK_NAME = Aws.STACK_NAME
        APPLICATION_REGION = Aws.REGION
        APPLICATION_ACCOUNT = Aws.ACCOUNT_ID

        SAGEMAKER_NOTEBOOK = f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:notebook-instance/{self._microservice.pmn.notebook_instance.attr_notebook_instance_name}"
        SAGEMAKER_NOTEBOOK_LC = f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:notebook-instance-lifecycle-config/{self._microservice.pmn.sagemaker_lifecycle_config.attr_notebook_instance_lifecycle_config_name}"

        TPS_INITIALIZE_SM_NAME = self._microservice.tps._sm.attr_name
        WFM_WORKFLOWS_SM_NAME = self._microservice.wfm.statemachine_workflows_sm.state_machine_name
        WFM_WORKFLOW_EXECUTION_SM_NAME = self._microservice.wfm.statemachine_workflow_executions_sm.state_machine_name
        AMAZON_ADS_REPORTS_SM_NAME = self._microservice.aar.ads_report_stepfunctions.state_machine_name
        SELLING_PARTNET_REPORTS_SM_NAME = self._microservice.spr.sp_report_stepfunctions.state_machine_name

        WFM_CUSTOMER_TABLE = self._microservice.wfm.dynamodb_customer_config_table.table_arn
        WFM_WORKFLOWS_TABLE = self._microservice.wfm.dynamodb_workflows_table.table_arn
        WFM_WORKFLOW_EXECUTION_TABLE = self._microservice.wfm.dynamodb_execution_status_table.table_arn
        TPS_CUSTOMER_TABLE = self._microservice.tps._customer_config_ddb.table_arn

        REPORTS_BUCKET = self._microservice.rsr.bucket.bucket_arn

        REPORTS_BUCKET_KEY = self._microservice.rsr.bucket_key.key_arn
        WFM_TABLE_KEY = self._microservice.wfm.kms_key.key_arn
        TPS_TABLE_KEY = self._microservice.tps.tps_kms_key.key_arn
        LOGGING_BUCKET_KEY = self._solution_buckets.logging_bucket_key.key_arn

        AMC_SECRET_NAME = self._amc_secret.secret_name
        AMC_SECRET_KEY = self._amc_secret.encryption_key.key_arn
        SP_SECRET_NAME = self._sp_secret.secret_name
        SP_SECRET_KEY = self._sp_secret.encryption_key.key_arn

        self.microservice_admin_policy = iam.ManagedPolicy(
            self,
            "MicroserviceAdminPolicy",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "cloudformation:ListStacks",
                            "lambda:GetAccountSettings",
                            "lambda:ListFunctions",
                            "glue:Get*",
                            "glue:SearchTables",
                            "sagemaker:ListNotebookInstances"
                        ],
                        resources=[
                            "*",  # NOSONAR
                        ],
                        conditions={
                            "StringEquals": {
                                "aws:ResourceAccount": [
                                    f"{APPLICATION_ACCOUNT}"
                                ]
                            }
                        }
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "athena:GetQueryExecution",
                            "athena:GetQueryResults",
                            "athena:GetWorkGroup",
                            "athena:ListNamedQueries",
                            "athena:ListQueryExecutions",
                            "athena:StartQueryExecution",
                            "dynamodb:DescribeTable",
                            "dynamodb:ListTables",
                            "events:Describe*",
                            "events:List*",
                            "iam:ListRoles",
                            "iam:ListUsers",
                            "logs:DescribeLogGroups",
                            "logs:DescribeLogStreams",
                            "s3:ListAllMyBuckets",
                            "states:DescribeStateMachine",
                            "states:ListStateMachines"
                        ],
                        resources=[
                            f"arn:aws:athena:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:*",
                            f"arn:aws:dynamodb:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:*",
                            f"arn:aws:events:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:*",
                            f"arn:aws:iam::{APPLICATION_ACCOUNT}:*",
                            f"arn:aws:logs:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:*",
                            "arn:aws:s3:::*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:*",
                        ],
                        conditions={
                            "StringEquals": {
                                "aws:ResourceAccount": [
                                    f"{APPLICATION_ACCOUNT}"
                                ]
                            }
                        }
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "secretsmanager:DescribeSecret",
                            "secretsmanager:GetSecretValue",
                            "secretsmanager:PutSecretValue",
                        ],
                        resources=[
                            f"arn:aws:secretsmanager:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:secret:{AMC_SECRET_NAME}*",
                            f"arn:aws:secretsmanager:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:secret:{SP_SECRET_NAME}*",
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "sagemaker:*"
                        ],
                        resources=[
                            SAGEMAKER_NOTEBOOK,
                            SAGEMAKER_NOTEBOOK_LC
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "states:*"
                        ],
                        resources=[
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{TPS_INITIALIZE_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{TPS_INITIALIZE_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{WFM_WORKFLOWS_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{WFM_WORKFLOWS_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{WFM_WORKFLOW_EXECUTION_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{WFM_WORKFLOW_EXECUTION_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{AMAZON_ADS_REPORTS_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{AMAZON_ADS_REPORTS_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{SELLING_PARTNET_REPORTS_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{SELLING_PARTNET_REPORTS_SM_NAME}*",
                        ]
                    ),

                    iam.PolicyStatement(
                        actions=[
                            "dynamodb:*"
                        ],
                        resources=[
                            f"{TPS_CUSTOMER_TABLE}*",
                            f"{WFM_CUSTOMER_TABLE}*",
                            f"{WFM_WORKFLOWS_TABLE}*",
                            f"{WFM_WORKFLOW_EXECUTION_TABLE}*",
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "kms:*"
                        ],
                        resources=[
                            TPS_TABLE_KEY,
                            WFM_TABLE_KEY,
                            LOGGING_BUCKET_KEY,
                            AMC_SECRET_KEY,
                            SP_SECRET_KEY,
                            REPORTS_BUCKET_KEY
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "events:*"
                        ],
                        resources=[
                            f"arn:aws:events:*:{APPLICATION_ACCOUNT}:rule/amc*",
                            f"arn:aws:events:*:{APPLICATION_ACCOUNT}:rule/{STACK_NAME}*"
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "lambda:*"
                        ],
                        resources=[
                            f"arn:aws:lambda:*:{APPLICATION_ACCOUNT}:function:{STACK_NAME}*"
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "logs:*"
                        ],
                        resources=[
                            f"arn:aws:logs:*:{APPLICATION_ACCOUNT}:log-group:/aws/*/{STACK_NAME}*:*",
                            f"arn:aws:logs:*:{APPLICATION_ACCOUNT}:log-group:/aws-glue/jobs/output:log-stream:*",
                        ]
                    ),

                    iam.PolicyStatement(
                        actions=[
                            "cloudformation:*"
                        ],
                        resources=[
                            f"arn:aws:cloudformation:*:{APPLICATION_ACCOUNT}:stack/{STACK_NAME}*"
                        ]
                    ),

                    iam.PolicyStatement(
                        actions=[
                            "s3:*"
                        ],
                        resources=[
                            REPORTS_BUCKET,
                            f"{REPORTS_BUCKET}/*"
                        ]
                    ),

                    iam.PolicyStatement(
                        actions=[
                            "s3:ListBucket"
                        ],
                        resources=[
                            "arn:aws:s3:::amc*",
                            "arn:aws:s3:::amc*/*",
                        ]
                    ),
                ]
            )
        )

        add_cfn_nag_suppressions(
            self.microservice_admin_policy.node.default_child,
            [
                CfnNagSuppression(
                    rule_id="F5",
                    reason="Managed Policy is created for Admin User access to the Solution"),
                CfnNagSuppression(
                    rule_id="W12",
                    reason="* permissions required for admin using this policy to access all deployed resources/actions through the console.")
            ]
        )

    def _create_datalake_admin_policy(self):
        APPLICATION_REGION = Aws.REGION
        APPLICATION_ACCOUNT = Aws.ACCOUNT_ID

        STAGE_A_TRANSFORM_SM_NAME = self._insights_pipeline_resources.stage_a_transform.sm_a.attr_name
        STAGE_B_TRANSFORM_SM_NAME = self._insights_pipeline_resources.stage_b_transform.sm_b.attr_name

        DATALAKE_CUSTOMER_TABLE = self._foundations_resources.customer_config_table.table_arn
        OCTAGON_DATASETS_TABLE = self._foundations_resources.datasets.table_arn
        OCTAGON_OBJECT_METADATA_TABLE = self._foundations_resources.object_metadata.table_arn
        OCTAGON_PIPELINE_EXECUTION_TABLE = self._foundations_resources.peh.table_arn
        OCTAGON_PIPELINE_TABLE = self._foundations_resources.pipelines.table_arn

        ARTIFACTS_BUCKET = self._solution_buckets.artifacts_bucket.bucket_arn
        LOGGING_BUCKET = self._solution_buckets.logging_bucket.bucket_arn
        RAW_BUCKET = self._foundations_resources.raw_bucket.bucket_arn
        STAGE_BUCKET = self._foundations_resources.stage_bucket.bucket_arn
        ATHENA_BUCKET = self._foundations_resources.athena_bucket.bucket_arn

        DATALAKE_CUSTOMER_TABLE_KEY = self._foundations_resources.customer_config_table_key.key_arn
        ARTIFACTS_BUCKET_KEY = self._solution_buckets.artifacts_bucket_key.key_arn

        RAW_BUCKET_KEY = self._foundations_resources.raw_bucket_key.key_arn
        STAGE_BUCKET_KEY = self._foundations_resources.stage_bucket_key.key_arn
        ATHENA_BUCKET_KEY = self._foundations_resources.athena_bucket_key.key_arn
        OCTAGON_DATASETS_TABLE_KEY = self._foundations_resources.datasets.encryption_key.key_arn
        OCTAGON_OBJECT_METADATA_TABLE_KEY = self._foundations_resources.object_metadata.encryption_key.key_arn
        OCTAGON_PIPELINE_EXECUTION_TABLE_KEY = self._foundations_resources.peh.encryption_key.key_arn
        OCTAGON_PIPELINES_TABLE_KEY = self._foundations_resources.pipelines.encryption_key.key_arn

        LAKE_FORMATION_CATALOG = f"arn:aws:lakeformation:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog:{Aws.ACCOUNT_ID}"

        GLUE_JOB_NAMES = [dataset.job.name for dataset in self._dataset_resources]

        self.datalake_admin_policy = iam.ManagedPolicy(
            self,
            "DataLakeAdminPolicy",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "s3:*"
                        ],
                        resources=[
                            RAW_BUCKET,
                            f"{RAW_BUCKET}/*",
                            STAGE_BUCKET,
                            f"{STAGE_BUCKET}/*",
                            ATHENA_BUCKET,
                            f"{ATHENA_BUCKET}/*",
                            LOGGING_BUCKET,
                            f"{LOGGING_BUCKET}/*",
                            ARTIFACTS_BUCKET,
                            f"{ARTIFACTS_BUCKET}/*",
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "lakeformation:*"
                        ],
                        resources=[
                            LAKE_FORMATION_CATALOG
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "glue:*"
                        ],
                        resources=[
                            f"arn:aws:glue:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:job/{glue_job_name}" for
                            glue_job_name in GLUE_JOB_NAMES
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "kms:*"
                        ],
                        resources=[
                            DATALAKE_CUSTOMER_TABLE_KEY,
                            RAW_BUCKET_KEY,
                            STAGE_BUCKET_KEY,
                            ATHENA_BUCKET_KEY,
                            ARTIFACTS_BUCKET_KEY,
                            OCTAGON_DATASETS_TABLE_KEY,
                            OCTAGON_OBJECT_METADATA_TABLE_KEY,
                            OCTAGON_PIPELINE_EXECUTION_TABLE_KEY,
                            OCTAGON_PIPELINES_TABLE_KEY
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "dynamodb:*"
                        ],
                        resources=[
                            f"{DATALAKE_CUSTOMER_TABLE}*",
                            f"{OCTAGON_DATASETS_TABLE}*",
                            f"{OCTAGON_OBJECT_METADATA_TABLE}*",
                            f"{OCTAGON_PIPELINE_EXECUTION_TABLE}*",
                            f"{OCTAGON_PIPELINE_TABLE}*",
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "states:*"
                        ],
                        resources=[
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{STAGE_A_TRANSFORM_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{STAGE_A_TRANSFORM_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:stateMachine:{STAGE_B_TRANSFORM_SM_NAME}*",
                            f"arn:aws:states:{APPLICATION_REGION}:{APPLICATION_ACCOUNT}:execution:{STAGE_B_TRANSFORM_SM_NAME}*"
                        ]
                    ),
                ],
            )
        )

        add_cfn_nag_suppressions(
            self.datalake_admin_policy.node.default_child,
            [
                CfnNagSuppression(
                    rule_id="F5",
                    reason="Managed Policy is created for Admin User access to the Solution")
            ]
        )

    def _create_cfn_output(self):
        admin_policy_link = f'''
            https://us-east-1.console.aws.amazon.com/iam/home#/policies/details/arn:aws:iam::{Aws.ACCOUNT_ID}:policy%2F{self.microservice_admin_policy.managed_policy_name}?section=permissions,
            https://us-east-1.console.aws.amazon.com/iam/home#/policies/details/arn:aws:iam::{Aws.ACCOUNT_ID}:policy%2F{self.datalake_admin_policy.managed_policy_name}?section=permissions,
            '''
        self._admin_group_output = CfnOutput(
            self,
            "AdminPolicyOutput",
            description="Use these links to view the admin IAM Policies for this stack",
            value=admin_policy_link,
            condition=self._creating_resources_condition
        )

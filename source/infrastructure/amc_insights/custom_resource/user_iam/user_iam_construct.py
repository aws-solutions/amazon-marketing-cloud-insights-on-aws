# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk.aws_lambda import Runtime, Architecture
from constructs import Construct
import aws_cdk.aws_iam as iam
from aws_cdk import Duration, CustomResource, Aws, CfnOutput, Aspects

from aws_lambda_layers.aws_solutions.layer import SolutionsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.python.lambda_alarm import SolutionsLambdaFunctionAlarm
from amc_insights.custom_resource import AMC_INSIGHTS_CUSTOM_RESOURCE_PATH
from amc_insights.condition_aspect import ConditionAspect
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer


class UserIam(Construct):
    def __init__(
            self,
            scope,
            id,
            solution_buckets,
            tps_resources,
            wfm_resources,
            pmn_resources,
            foundations_resources,
            insights_pipeline_resources,
            amc_dataset_resources,
            creating_resources_condition
    ) -> None:
        super().__init__(scope, id)

        self._solution_buckets = solution_buckets
        self._tps_resources = tps_resources
        self._wfm_resources = wfm_resources
        self._pmn_resources = pmn_resources
        self._foundations_resources = foundations_resources
        self._resource_prefix = Aws.STACK_NAME
        self._insights_pipeline_resources = insights_pipeline_resources
        self._amc_dataset_resources = amc_dataset_resources
        self._creating_resources_condition = creating_resources_condition

        # Apply condition to resources in Construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._create_iam_policy_for_custom_resource_lambda()
        self._create_user_iam_lambda()
        self._create_user_iam_custom_resource()
        self._create_cfn_output()

    def _create_iam_policy_for_custom_resource_lambda(self):
        artifacts_bucket_prefix_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:List*",
                "s3:Get*",
                "s3:Put*",
                "s3:Delete*",
            ],
            resources=[
                f"arn:aws:s3:::{self._solution_buckets.artifacts_bucket.bucket_name}/user-iam/*",
            ]
        )
        policy_statements: list[iam.PolicyStatement] = [artifacts_bucket_prefix_statement]

        self._sync_cfn_template_lambda_iam_policy = iam.Policy(
            self, "CreateUserIAMLambdaIamPolicy",
            statements=policy_statements
        )

    def _create_user_iam_lambda(self):
        """
        This function is responsible for placing the user iam resources in the S3 artifacts bucket.
        """
        self._user_iam_lambda = SolutionsPythonFunction(
            self,
            "CreateUserIAMResources",
            AMC_INSIGHTS_CUSTOM_RESOURCE_PATH / "user_iam" / "lambdas" / "create_user_iam.py",
            "event_handler",
            runtime=Runtime.PYTHON_3_9,
            description="Lambda function for custom resource for creating and placing the user iam resources in the S3 artifacts bucket",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=Architecture.ARM_64,
            environment={
                "SOLUTION_ID": self.node.try_get_context("SOLUTION_ID"),
                "SOLUTION_VERSION": self.node.try_get_context("SOLUTION_VERSION"),
                "STACK_NAME": Aws.STACK_NAME,
                "APPLICATION_REGION": Aws.REGION,
                "APPLICATION_ACCOUNT": Aws.ACCOUNT_ID,
                "SAGEMAKER_NOTEBOOK": f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:notebook-instance/{self._pmn_resources.notebook_instance.attr_notebook_instance_name}",
                "SAGEMAKER_NOTEBOOK_LC": f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:notebook-instance-lifecycle-config/{self._pmn_resources.sagemaker_lifecycle_config.attr_notebook_instance_lifecycle_config_name}",
                "DATALAKE_CUSTOMER_TABLE": self._foundations_resources.customer_config_table.table_arn,
                "TPS_INITIALIZE_SM_NAME": self._tps_resources._sm.attr_name,
                "WFM_WORKFLOWS_SM_NAME": self._wfm_resources.statemachine_workflows_sm.state_machine_name,
                "WFM_WORKFLOW_EXECUTION_SM_NAME": self._wfm_resources.statemachine_workflow_executions_sm.state_machine_name,
                "STAGE_A_TRANSFORM_SM_NAME": self._insights_pipeline_resources._stage_a_transform.sm_a.attr_name,
                "STAGE_B_TRANSFORM_SM_NAME": self._insights_pipeline_resources._stage_b_transform.sm_b.attr_name,
                "WFM_CUSTOMER_TABLE": self._wfm_resources.dynamodb_customer_config_table.table_arn,
                "WFM_WORKFLOWS_TABLE": self._wfm_resources.dynamodb_workflows_table.table_arn,
                "WFM_WORKFLOW_EXECUTION_TABLE": self._wfm_resources.dynamodb_execution_status_table.table_arn,
                "TPS_CUSTOMER_TABLE": self._tps_resources._customer_config_ddb.table_arn,
                "OCTAGON_DATASETS_TABLE": self._foundations_resources.datasets.table_arn,
                "OCTAGON_OBJECT_METADATA_TABLE": self._foundations_resources.object_metadata.table_arn,
                "OCTAGON_PIPELINE_EXECUTION_TABLE": self._foundations_resources.peh.table_arn,
                "OCTAON_PIPELINE_TABLE": self._foundations_resources.pipelines.table_arn,
                "DATALAKE_CUSTOMER_TABLE_KEY": self._foundations_resources.customer_config_table_key.key_arn,
                "WFM_TABLE_KEY": self._wfm_resources.kms_key.key_arn,
                "TPS_TABLE_KEY": self._tps_resources.tps_kms_key.key_arn,
                "ARTIFACTS_BUCKET": self._solution_buckets.artifacts_bucket.bucket_arn,
                "ARTIFACTS_BUCKET_KEY": self._solution_buckets.artifacts_bucket_key.key_arn,
                "LOGGING_BUCKET": self._solution_buckets.logging_bucket.bucket_arn,
                "LOGGING_BUCKET_KEY": self._solution_buckets.logging_bucket_key.key_arn,
                "RAW_BUCKET": self._foundations_resources.raw_bucket.bucket_arn,
                "RAW_BUCKET_KEY": self._foundations_resources.raw_bucket_key.key_arn,
                "STAGE_BUCKET": self._foundations_resources.stage_bucket.bucket_arn,
                "STAGE_BUCKET_KEY": self._foundations_resources.stage_bucket_key.key_arn,
                "ATHENA_BUCKET": self._foundations_resources.athena_bucket.bucket_arn,
                "ATHENA_BUCKET_KEY": self._foundations_resources.athena_bucket_key.key_arn,
                "LAKE_FORMATION_CATALOG": f"arn:aws:lakeformation:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog:{Aws.ACCOUNT_ID}",
                "OCTAGON_DATASETS_TABLE_KEY": self._foundations_resources.datasets.encryption_key.key_arn,
                "OCTAGON_OBJECT_METADATA_TABLE_KEY": self._foundations_resources.object_metadata.encryption_key.key_arn,
                "OCTAGON_PIPELINE_EXECUTION_TABLE_KEY": self._foundations_resources.peh.encryption_key.key_arn,
                "OCTAGON_PIPELINES_TABLE_KEY": self._foundations_resources.pipelines.encryption_key.key_arn,
                "GLUE_JOB_NAME": self._amc_dataset_resources.job.name
            },
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self)
            ]
        )

        SolutionsLambdaFunctionAlarm(
            self,
            id="user-iam-lambda-alarm",
            alarm_name=f"{self._resource_prefix}-user-iam-lambda-alarm",
            lambda_function=self._user_iam_lambda
        )

        self._sync_cfn_template_lambda_iam_policy.attach_to_role(self._user_iam_lambda.role)

        self._solution_buckets.artifacts_bucket_key.grant_encrypt_decrypt(
            self._user_iam_lambda.role)

        self._user_iam_lambda.node.add_dependency(self._solution_buckets.artifacts_bucket)

    def _create_user_iam_custom_resource(self):
        """
        This function creates the custom resource for placing the user iam resources in the S3 artifacts bucket
        """
        self._user_iam_custom_resource = CustomResource(
            self,
            "UserIAMResourcesLambdaCustomResource",
            service_token=self._user_iam_lambda.function_arn,
            properties={
                "artifacts_bucket_name": self._solution_buckets.artifacts_bucket.bucket_name,
                "artifacts_key_prefix": "user-iam/",
            },
        )
        self._user_iam_custom_resource.node.add_dependency(self._sync_cfn_template_lambda_iam_policy)

    def _create_cfn_output(self):
        user_operational_policy_output_string = f'''
            https://s3.console.aws.amazon.com/s3/object/{self._solution_buckets.artifacts_bucket.bucket_name}?region={Aws.REGION}&prefix=user-iam/IAM_POLICY_OPERATE.json
            '''
        self._user_operational_policy_output = CfnOutput(
            self,
            "UserOperationalPolicyOutput",
            description="Use this link to view and download the operational IAM policy for this stack",
            value=user_operational_policy_output_string,
            condition=self._creating_resources_condition
        )

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from constructs import Construct
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Aws,
    Fn,
    Aspects,
    CfnCondition,
    CfnOutput,
    aws_kms as kms,
    aws_sagemaker as sagemaker,
)
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from amc_insights.condition_aspect import ConditionAspect
from platform_management.custom_resource.platform_manager_uploader import PlatformManagerUploader


class PlatformManagerSageMaker(Construct):
    microservice_name = "platform-manager"

    def __init__(
            self,
            scope: Construct,
            id: str,
            environment_id: str,
            team: str,
            workflow_manager_resources,
            tenant_provisioning_resources,
            amazon_ads_reporting_resources,
            selling_partner_reporting_resources,
            creating_resources_condition: CfnCondition,
            solution_buckets,
    ) -> None:
        super().__init__(scope, id)

        # Apply condition to resources in PMN construct
        Aspects.of(self).add(ConditionAspect(self, "ConditionAspect", creating_resources_condition))

        self._environment_id: str = environment_id
        self._team = team
        self._resource_prefix = Aws.STACK_NAME
        self._workflow_manager_resources = workflow_manager_resources
        self._tenant_provisioning_resources = tenant_provisioning_resources
        self._amazon_ads_reporting_resources = amazon_ads_reporting_resources
        self._selling_partner_reporting_resources = selling_partner_reporting_resources
        self._solution_buckets = solution_buckets
        self._creating_resources_condition = creating_resources_condition

        self._notebook_samples_prefix = "platform_notebook_manager_samples"
        self._notebook_instance_name = f"{self._resource_prefix}-amc-insights-platform-manager-notebooks"

        PlatformManagerUploader(
            self, "SyncPlatformManager",
            self._resource_prefix,
            self._solution_buckets,
            self._notebook_samples_prefix
        )

        self._create_sagemaker_kms_key()
        self._create_sagemaker_role()
        self._create_sagemaker_lifecycle_config()
        self._create_sagemaker_notebook_instance()
        self._create_notebook_instance_output_link()

    def _create_sagemaker_kms_key(self) -> None:
        """kms key and alias"""

        self._sagemaker_kms_key = kms.Key(
            self,
            id=f"{self.microservice_name}-table-key",
            description=f"{self.microservice_name.title()} Notebook Key",
            alias=f"alias/{self._resource_prefix}-pmn-sagemaker-cmk",
            enable_key_rotation=True,
            pending_window=Duration.days(30),
            removal_policy=RemovalPolicy.DESTROY
        )

    def _create_sagemaker_role(self) -> None:
        self.sagemaker_role: Role = Role(
            self,
            f"{self.microservice_name}-role",
            assumed_by=ServicePrincipal("sagemaker.amazonaws.com"),
        )

        ManagedPolicy(
            self,
            f"{self.microservice_name}-policy",
            roles=[self.sagemaker_role],
            document=PolicyDocument(
                statements=[
                    # give sagemaker permission to execute Lifecycle configuration script to stop the instance if it's idle
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["sagemaker:DescribeNotebookInstance",
                                 "sagemaker:StopNotebookInstance"
                                 ],
                        resources=[
                            f"arn:aws:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:notebook-instance/{self._notebook_instance_name}",
                        ]
                    ),

                    # give sagemaker permission to invoke the microservice state machine startup lambdas
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["lambda:InvokeFunction"],
                        resources=[
                            self._workflow_manager_resources.lambda_invoke_workflow_execution_sm.function_arn,
                            self._workflow_manager_resources.lambda_invoke_workflow_sm.function_arn,
                            self._workflow_manager_resources.lambda_create_workflow_schedule.function_arn,
                            self._workflow_manager_resources.lambda_delete_workflow_schedule.function_arn,
                            self._tenant_provisioning_resources.lambda_invoke_tps_initialize_sm.function_arn,
                            self._workflow_manager_resources.lambda_amc_auth.function_arn,
                            self._amazon_ads_reporting_resources.invoke_ads_report_sm_lambda.function_arn,
                            self._amazon_ads_reporting_resources.get_profiles_lambda.function_arn,
                            self._amazon_ads_reporting_resources.schedule_report_lambda.function_arn,
                            self._selling_partner_reporting_resources.selling_partner_invoke_state_machine.function_arn,
                            self._selling_partner_reporting_resources.selling_partner_schedule_report.function_arn
                        ],
                    ),
                    # give sagemaker permission to copy the platform manager files from the foundations artifact bucket
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:ListBucket"
                        ],
                        resources=[
                            self._solution_buckets.artifacts_bucket.bucket_arn,
                        ],
                        conditions={
                            "StringLike": {
                                "s3:prefix": [f"{self._notebook_samples_prefix}/*"]
                            }
                        }
                    ),

                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                        ],
                        resources=[
                            f"{self._solution_buckets.artifacts_bucket.bucket_arn}/{self._notebook_samples_prefix}/*",
                        ]
                    ),

                    PolicyStatement(
                        sid="AccessS3BucketsOwnedBySpecificAWSAccountsOnly",
                        effect=Effect.DENY,
                        actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:ListBucket"
                        ],
                        resources=[
                            self._solution_buckets.artifacts_bucket.bucket_arn,
                            f"{self._solution_buckets.artifacts_bucket.bucket_arn}/*",
                        ],
                        conditions={
                            "StringNotEquals": {
                                "aws:ResourceAccount": [
                                    f"{Aws.ACCOUNT_ID}"
                                ]
                            }
                        }
                    ),
                ]
            )
        )

    def _create_sagemaker_lifecycle_config(self) -> None:
        """lifecycle config and notebook instance"""

        self._solution_buckets.artifacts_bucket_key.grant_encrypt_decrypt(self.sagemaker_role)

        self.sagemaker_lifecycle_config = sagemaker.CfnNotebookInstanceLifecycleConfig(
            self,
            f"{self.microservice_name}-lc",
            on_start=[sagemaker.CfnNotebookInstanceLifecycleConfig.NotebookInstanceLifecycleHookProperty(
                content=Fn.base64(f"""
                    #!/bin/bash
                                
                    set -e

                    # Parameters
                    S3_BUCKET={self._solution_buckets.artifacts_bucket.bucket_name}
                    INVOKE_WORKFLOW_EXECUTION_SM_NAME="INVOKE_WORKFLOW_EXECUTION_SM_NAME={self._workflow_manager_resources.lambda_invoke_workflow_execution_sm.function_name}"
                    INVOKE_WORKFLOW_SM_NAME="INVOKE_WORKFLOW_SM_NAME={self._workflow_manager_resources.lambda_invoke_workflow_sm.function_name}"
                    CREATE_WORKFLOW_SCHEDULE_NAME="CREATE_WORKFLOW_SCHEDULE_NAME={self._workflow_manager_resources.lambda_create_workflow_schedule.function_name}"
                    INVOKE_TPS_SM_NAME="INVOKE_TPS_SM_NAME={self._tenant_provisioning_resources.lambda_invoke_tps_initialize_sm.function_name}"
                    DELETE_WORKFLOW_SCHEDULE_NAME="DELETE_WORKFLOW_SCHEDULE_NAME={self._workflow_manager_resources.lambda_delete_workflow_schedule.function_name}"
                    AMAZON_ADS_AUTH_LAMBDA_NAME="AMAZON_ADS_AUTH_LAMBDA_NAME={self._workflow_manager_resources.lambda_amc_auth.function_name}"
                    INVOKE_ADS_REPORT_SM_NAME="INVOKE_ADS_REPORT_SM_NAME={self._amazon_ads_reporting_resources.invoke_ads_report_sm_lambda.function_name}"
                    SCHEDULE_ADS_REPORT_NAME="SCHEDULE_ADS_REPORT_NAME={self._amazon_ads_reporting_resources.schedule_report_lambda.function_name}"
                    GET_PROFILES_NAME="GET_PROFILES_NAME={self._amazon_ads_reporting_resources.get_profiles_lambda.function_name}"
                    INVOKE_SP_REPORT_SM_NAME="INVOKE_SP_REPORT_SM_NAME={self._selling_partner_reporting_resources.selling_partner_invoke_state_machine.function_name}"
                    SCHEDULE_SP_REPORT_NAME="SCHEDULE_SP_REPORT_NAME={self._selling_partner_reporting_resources.selling_partner_schedule_report.function_name}"
                    REGION="REGION={Aws.REGION}"
                    RULE_PREFIX="RULE_PREFIX={self._resource_prefix}"

                    # Load notebook files to instance       
                    aws s3 sync s3://$S3_BUCKET/{self._notebook_samples_prefix}/ /home/ec2-user/SageMaker/

                    # Set instance r/w/x permissions
                    chmod -R 777 /home/ec2-user/SageMaker/

                    # Create environment variables for interface library
                    FILE=/home/ec2-user/SageMaker/platform_manager/.env
                    touch $FILE
                    grep -qF "$INVOKE_WORKFLOW_EXECUTION_SM_NAME" "$FILE" || echo "$INVOKE_WORKFLOW_EXECUTION_SM_NAME" >> "$FILE"
                    grep -qF "$INVOKE_WORKFLOW_SM_NAME" "$FILE" || echo "$INVOKE_WORKFLOW_SM_NAME" >> "$FILE"
                    grep -qF "$CREATE_WORKFLOW_SCHEDULE_NAME" "$FILE" || echo "$CREATE_WORKFLOW_SCHEDULE_NAME" >> "$FILE"
                    grep -qF "$INVOKE_TPS_SM_NAME" "$FILE" || echo "$INVOKE_TPS_SM_NAME" >> "$FILE"
                    grep -qF "$DELETE_WORKFLOW_SCHEDULE_NAME" "$FILE" || echo "$DELETE_WORKFLOW_SCHEDULE_NAME" >> "$FILE"
                    grep -qF "$AMAZON_ADS_AUTH_LAMBDA_NAME" "$FILE" || echo "$AMAZON_ADS_AUTH_LAMBDA_NAME" >> "$FILE"
                    grep -qF "$INVOKE_ADS_REPORT_SM_NAME" "$FILE" || echo "$INVOKE_ADS_REPORT_SM_NAME" >> "$FILE"
                    grep -qF "$GET_PROFILES_NAME" "$FILE" || echo "$GET_PROFILES_NAME" >> "$FILE"
                    grep -qF "$SCHEDULE_ADS_REPORT_NAME" "$FILE" || echo "$SCHEDULE_ADS_REPORT_NAME" >> "$FILE"
                    grep -qF "$INVOKE_SP_REPORT_SM_NAME" "$FILE" || echo "$INVOKE_SP_REPORT_SM_NAME" >> "$FILE"
                    grep -qF "$SCHEDULE_SP_REPORT_NAME" "$FILE" || echo "$SCHEDULE_SP_REPORT_NAME" >> "$FILE"
                    grep -qF "$REGION" "$FILE" || echo "$REGION" >> "$FILE"
                    grep -qF "$RULE_PREFIX" "$FILE" || echo "$RULE_PREFIX" >> "$FILE"

                    # Set default jupyter kernel
                    sudo -u ec2-user -i << 'EOF'
                    python -m ipykernel install --user --name python3 --display-name "amcinsights"
                    pip install python-dotenv           
                    EOF

                    # Load autostop script
                    echo "Fetching the autostop script"
                    wget https://raw.githubusercontent.com/aws-samples/amazon-sagemaker-notebook-instance-lifecycle-config-samples/master/scripts/auto-stop-idle/autostop.py
                    echo "Starting the SageMaker autostop script in cron"
                    (crontab -l 2>/dev/null; echo "*/5 * * * * $(which python) $PWD/autostop.py --time 3600 --ignore-connections >> /var/log/autostop.log 2>&1") | crontab -

                    # Remove lost+found folder
                    rm -rf /home/ec2-user/SageMaker/lost+found
                """)
            )]
        )

        self.sagemaker_lifecycle_config.node.add_dependency(self.sagemaker_role)

    def _create_sagemaker_notebook_instance(self):
        self.notebook_instance = sagemaker.CfnNotebookInstance(
            self,
            f"{self.microservice_name}-nb",
            instance_type='ml.t2.medium',
            role_arn=self.sagemaker_role.role_arn,
            kms_key_id=self._sagemaker_kms_key.key_id,
            lifecycle_config_name=self.sagemaker_lifecycle_config.attr_notebook_instance_lifecycle_config_name,
            notebook_instance_name=self._notebook_instance_name,
            root_access="Enabled",
        )

    def _create_notebook_instance_output_link(self):
        CfnOutput(
            self,
            "SageMakerNotebookInstance",
            description="Use this link to access the Platform Manager notebook instance",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/sagemaker/home?region={Aws.REGION}#/notebook-instances/{self.notebook_instance.notebook_instance_name}",
            condition=self._creating_resources_condition
        )

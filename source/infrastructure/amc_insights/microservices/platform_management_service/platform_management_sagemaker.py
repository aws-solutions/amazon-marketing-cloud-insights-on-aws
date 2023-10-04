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
    aws_lakeformation as lakeformation,
    aws_kms as kms,
    aws_sagemaker as sagemaker,
)
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from amc_insights.condition_aspect import ConditionAspect
from amc_insights.custom_resource.platform_management_service.platform_manager_uploader import PlatformManagerUploader


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
        self._solution_buckets = solution_buckets
        self._creating_resources_condition = creating_resources_condition

        self._notebook_samples_prefix = "platform_notebook_manager_samples"

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
            description=f"{self.microservice_name.title()} Table Key",
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
                    # give sagemaker permission to invoke the WFM SM startup lambdas
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["lambda:InvokeFunction"],
                        resources=[
                            self._workflow_manager_resources.lambda_invoke_workflow_execution_sm.function_arn,
                            self._workflow_manager_resources.lambda_invoke_workflow_sm.function_arn,
                            self._workflow_manager_resources.lambda_create_workflow_schedule.function_arn,
                            self._workflow_manager_resources.lambda_delete_workflow_schedule.function_arn,
                            self._tenant_provisioning_resources._lambda_invoke_tps_initialize_sm.function_arn
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
                    IDLE_TIME=900
                    S3_BUCKET={self._solution_buckets.artifacts_bucket.bucket_name}
                    INVOKE_WORKFLOW_EXECUTION_SM_NAME="INVOKE_WORKFLOW_EXECUTION_SM_NAME={self._workflow_manager_resources.lambda_invoke_workflow_execution_sm.function_name}"
                    INVOKE_WORKFLOW_SM_NAME="INVOKE_WORKFLOW_SM_NAME={self._workflow_manager_resources.lambda_invoke_workflow_sm.function_name}"
                    CREATE_WORKFLOW_SCHEDULE_NAME="CREATE_WORKFLOW_SCHEDULE_NAME={self._workflow_manager_resources.lambda_create_workflow_schedule.function_name}"
                    INVOKE_TPS_SM_NAME="INVOKE_TPS_SM_NAME={self._tenant_provisioning_resources._lambda_invoke_tps_initialize_sm.function_name}"
                    DELETE_WORKFLOW_SCHEDULE_NAME="DELETE_WORKFLOW_SCHEDULE_NAME={self._workflow_manager_resources.lambda_delete_workflow_schedule.function_name}"
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
                    (crontab -l 2>/dev/null; echo "*/5 * * * * /usr/bin/python $PWD/autostop.py --time $IDLE_TIME --ignore-connections") | crontab -

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
            notebook_instance_name=f"{self._resource_prefix}-amc-insights-platform-manager-notebooks",
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

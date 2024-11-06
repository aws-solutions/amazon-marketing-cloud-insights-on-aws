#!/bin/bash
##############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Run functional tests for custom extended dataset
###############################################################################

usage() {
  msg "$msg"
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] --profile PROFILE --stack-name STACK_NAME --region REGION --email Email --role-arn ROLE_ARN

Available options:

-h, --help        Print this help and exit (optional)
-v, --verbose     Print script debug info (optional)
--stack-name      Name of the Cloudformation stack where the solution is running
--region          AWS Region, formatted like us-east-1
--profile         AWS profile for CLI commands
--email           Email to receive notifications
--role-arn        Cloudformation role to assume when updating the stack (optional)
EOF
  exit 1
}

msg() {
  echo >&2 -e "${1-}"
}

parse_params() {
  # default values of variables set from params
  flag=0
  param=''

  while :; do
    case "${1-}" in
    -h | --help) usage ;;
    -v | --verbose) set -x ;;
    --stack-name)
      stack_name="${2}"
      shift
      ;;
    --region)
      region="${2}"
      shift
      ;;
    --profile)
      profile="${2}"
      shift
      ;;
    --email)
      email="${2}"
      shift
      ;;
    --role-arn)
      role_arn="${2}"
      shift
      ;;
    *) break ;;
    esac
    shift
  done

  echo "$@"
  args=("$@")

  # check required params and arguments
  [[ -z "${stack_name}" ]] && usage "Missing required parameter: stack-name"
  [[ -z "${region}" ]] && usage "Missing required parameter: region"
  [[ -z "${profile}" ]] && usage "Missing required parameter: profile"
  [[ -z "${email}" ]] && usage "Missing required parameter: email"
#  [[ -z "${role_arn}" ]] && usage "Missing required parameter: role_arn"
  return 0
}

#########################
#####   RUN SCRIPT  #####
#########################
parse_params "$@"
msg "Parameters:"
msg "- Stack name: ${stack_name}"
msg "- Region: ${region}"
msg "- Profile: ${profile}"
msg "- Email: ${email}"
msg "- Role arn: ${role_arn}"

echo ""
sleep 3

[ "$DEBUG" == 'true' ] && set -x

# Get reference for all important folders
current_dir="$PWD"
source_dir="$(
  cd $current_dir/../../../../source
  pwd -P
)"
deployment_dir="$(
  cd $current_dir/../../../../deployment
  pwd -P
)"
infrastructure_dir="$source_dir/infrastructure"
functional_tests_dir="$source_dir/tests/functional_tests"
echo $current_dir

export REGION=$region
export AWS_PROFILE=$profile
export STACK=$stack_name
export EMAIL=$email
export ROLE_ARN=$role_arn

# Create a temporary Python virtualenv if no venv is active.
source $functional_tests_dir/helper/create_venv.sh
create_venv

echo "Remove build folder from solution helper package"
rm -r "$source_dir/cdk_solution_helper_py/helpers_common/build"


echo "------------------------------------"
echo "CONFIGURE EXTENDED DATASET"
echo "------------------------------------"
cd $current_dir
export DATASET_PARAMETERS_JSON_FILEPATH="$infrastructure_dir/datasets_parameters.json"
export MOCK_SCRIPTS_DIR="$current_dir/mock_scripts"
export GLUE_DIR="$infrastructure_dir/data_lake/glue/lambdas/sdlf_heavy_transform/adtech"
export DATALAKE_LAYERS_TRANSFORMATIONS_DIR="$infrastructure_dir/data_lake/lambda_layers/data_lake_library/python/datalake_library/transforms"
python3 python/configure_extended_dataset.py

echo "-----------------------------------------"
echo "CREATE BUCKETS FOR TEMPLATE AND ASSETS"
echo "-----------------------------------------"
template_bucket_name="$stack_name$(LC_ALL=C tr -dc a-z </dev/urandom | head -c 4)-func-tests"
assets_bucket_name="$template_bucket_name-$region"
echo $template_bucket_name
echo $assets_bucket_name
if [[ $region == us-east-1 ]]; then
  aws s3api create-bucket --bucket $template_bucket_name --region $region
  aws s3api create-bucket --bucket $assets_bucket_name --region $region
else
  aws s3api create-bucket --bucket $template_bucket_name --region $region --create-bucket-configuration LocationConstraint=$region
  aws s3api create-bucket --bucket $assets_bucket_name --region $region --create-bucket-configuration LocationConstraint=$region
fi

echo "-----------------------------------------"
echo "SYNTH TEMPLATE FOR EXTENDED DATASET"
echo "-----------------------------------------"

cd $deployment_dir

version_code="v3.1.2"

build-s3-cdk-dist deploy \
  --source-bucket-name $template_bucket_name \
  --solution-name amcinsights \
  --version-code $version_code \
  --cdk-app-path ../source/infrastructure/app.py \
  --cdk-app-entrypoint app:build_app \
  --region $region --sync

export TEMPLATE_URL="https://$template_bucket_name.s3.amazonaws.com/amcinsights/$version_code/amazon-marketing-cloud-insights.template"

echo "-----------------------------------------"
echo "UPDATE STACK"
echo "-----------------------------------------"

cd $current_dir
pytest python/test_stack_update.py -s -W ignore::DeprecationWarning -p no:cacheproviders -vv

echo "-----------------------------------------"
echo "TEST EXTENDED DATASET"
echo "-----------------------------------------"
export MOCK_DATA_DIR="$current_dir/mock_data"
export STAGE_BUCKET=$(aws cloudformation describe-stack-resource --stack-name $stack_name --region $region --logical-resource-id foundationsstagebucket7D53680B --query StackResourceDetail.PhysicalResourceId --output text)
export RAW_BUCKET=$(aws cloudformation describe-stack-resource --stack-name $stack_name --region $region --logical-resource-id foundationsrawbucket6964B12D --query StackResourceDetail.PhysicalResourceId --output text)
pytest python/test_extended_dataset.py -s -W ignore::DeprecationWarning -p no:cacheproviders -vv

echo "-----------------------------------------"
echo "CLEAN EXTENDED DATASET TEST SETUP"
echo "-----------------------------------------"
cd $current_dir
python3 python/cleanup_resources.py

exit 0

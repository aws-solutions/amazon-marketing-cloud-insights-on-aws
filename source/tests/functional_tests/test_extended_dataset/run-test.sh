#!/bin/bash
##############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Run functional tests
#
# USAGE:
#  ./run-test.sh [-h] [-v] --stack-name {STACK_NAME} --region {REGION} --profile {PROFILE} --email {EMAIL}
#    STACK_NAME name of the Cloudformation stack where the solution is running.
#    REGION needs to be in a format like us-east-1
#    PROFILE the profile that you have setup in ~/.aws/credentials
#      that you want to use for AWS CLI commands.
#    EMAIL an valid email to receive notifications from the solution
#
#    The following options are available:
#
#     -h | --help       Print usage
#     -v | --verbose    Print script debug info
#
###############################################################################

usage() {
  msg "$msg"
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] --profile PROFILE --stack-name STACK_NAME --region REGION --email Email

Available options:

-h, --help        Print this help and exit (optional)
-v, --verbose     Print script debug info (optional)
--stack-name      Name of the Cloudformation stack where the solution is running
--region          AWS Region, formatted like us-east-1
--profile         AWS profile for CLI commands
--email           Email to receive notifications
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
    -?*) die "Unknown option: $1" ;;
    *) break ;;
    esac
    shift
  done

  args=("$@")

  # check required params and arguments
  [[ -z "${stack_name}" ]] && usage "Missing required parameter: stack-name"
  [[ -z "${region}" ]] && usage "Missing required parameter: region"
  [[ -z "${profile}" ]] && usage "Missing required parameter: profile"
  [[ -z "${email}" ]] && usage "Missing required parameter: email"

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

echo ""
sleep 3

[ "$DEBUG" == 'true' ] && set -x

# Get reference for all important folders
current_dir="$PWD"
deployment_dir="$(
  cd $current_dir/../../../../deployment
  pwd -P
)"
source_dir="$(
  cd $current_dir/../../../../source
  pwd -P
)"
root_dir="$current_dir/../../../.."
infrastructure_dir="$source_dir/infrastructure"
echo $current_dir

export AWS_REGION=$region
export AWS_PROFILE=$profile
export STACK=$stack_name
export EMAIL=$email

#########################################
#####   CONFIGURE EXTENDED DATASET  #####
#########################################
echo "Configure extended dataset for stack $stack_name"
export DATASET_PARAMETERS_JSON_FILEPATH="$infrastructure_dir/datasets_parameters.json"
export MOCK_SCRIPTS_DIR="$current_dir/mock_scripts"
export GLUE_DIR="$infrastructure_dir/data_lake/glue/lambdas/sdlf_heavy_transform/adtech"
export DATALAKE_LAYERS_TRANSFORMATIONS_DIR="$infrastructure_dir/data_lake/lambda_layers/data_lake_library/python/datalake_library/transforms"
python3 python/configure_extended_dataset.py

######################################################
######   CREATE BUCKETS FOR TEMPLATE AND ASSETS  #####
######################################################
template_bucket_name="$stack_name-functional-tests"
assets_bucket_name="$template_bucket_name-$region"
echo $template_bucket_name
echo $assets_bucket_name
aws s3api create-bucket --bucket $template_bucket_name --region $region
aws s3api create-bucket --bucket $assets_bucket_name --region $region

#############################
######   UPDATE STACK   #####
#############################
cd $deployment_dir

build-s3-cdk-dist deploy \
  --source-bucket-name $template_bucket_name \
  --solution-name amcinsights \
  --version-code v2.0.0 \
  --cdk-app-path ../source/infrastructure/app.py \
  --cdk-app-entrypoint app:build_app \
  --region $region --sync

export TEMPLATE_URL="https://$template_bucket_name.s3.amazonaws.com/amcinsights/v2.0.0/amazon-marketing-cloud-insights.template"

cd $current_dir
pytest python/test_stack_update.py -s -W ignore::DeprecationWarning -p no:cacheproviders

#####################################
######   TEST EXTENDED DATASET  #####
#####################################
export MOCK_DATA_DIR="$current_dir/mock_data"
export STAGE_BUCKET=$(aws cloudformation describe-stack-resource --stack-name $stack_name --logical-resource-id foundationsstagebucket7D53680B --query StackResourceDetail.PhysicalResourceId --output text)
export RAW_BUCKET=$(aws cloudformation describe-stack-resource --stack-name $stack_name --logical-resource-id foundationsrawbucket6964B12D --query StackResourceDetail.PhysicalResourceId --output text)
pytest python/test_extended_dataset.py -s -W ignore::DeprecationWarning -p no:cacheproviders

###########################
#####   CLEAN SETUP   #####
###########################
cd $current_dir
python3 python/cleanup_resources.py

exit 0

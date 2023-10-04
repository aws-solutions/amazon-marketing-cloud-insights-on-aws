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
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] --profile PROFILE --stack-name STACK_NAME --region REGION --email Email --role-arn ROLE_ARN

Available options:

-h, --help        Print this help and exit (optional)
-v, --verbose     Print script debug info (optional)
--stack-name      Name of the Cloudformation stack where the solution is running
--region          AWS Region, formatted like us-east-1
--profile         AWS profile for CLI commands
--email           Email to receive notifications
--role-arn        Cloudformation role to assume when deploying
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
  [[ -z "${role_arn}" ]] && usage "Missing required parameter: role_arn"

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
deployment_dir="$(
  cd $current_dir/../../../../deployment
  pwd -P
)"
source_dir="$(
  cd $current_dir/../../../../source
  pwd -P
)"

export AWS_REGION=$region
export AWS_PROFILE=$profile
export STACK=$stack_name
export EMAIL=$email
export ROLE_ARN=$role_arn

# Remove build folder from solution helper package
rm -r "$source_dir/cdk_solution_helper_py/helpers_common/build"

#####################################################
#####   CREATE BUCKETS FOR TEMPLATE AND ASSETS  #####
#####################################################
template_bucket_name="$stack_name-functional-tests"
assets_bucket_name="$template_bucket_name-$region"
echo $template_bucket_name
echo $assets_bucket_name
aws s3api create-bucket --bucket $template_bucket_name --region $region
aws s3api create-bucket --bucket $assets_bucket_name --region $region

##############################################
######   TEST STACK SYNTHESIZE & DEPLOY  #####
##############################################
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
pytest python/test_stack_creation.py -s -W ignore::DeprecationWarning -p no:cacheproviders

# exit with return code from pytest so parent script can check the result
exit $?
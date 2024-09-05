#!/bin/bash

###############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Run functional tests for the amc dataset
#
# PRELIMINARY:
#  Deploy the solution before running this script
###############################################################################

#########################
###  SETUP FUNCTIONS  ###
#########################

usage() {
  msg "$msg"
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] --profile PROFILE --stack-name STACK_NAME --region REGION

Available options:

-h, --help           Print this help and exit (optional)
-v, --verbose        Print script debug info (optional)
--no-clean           Do not clean tests after running (optional)
--bucket-profile     If testing cross-account, the AWS profile to deploy the amc instance bucket (optional)
--skip-cross-region  Skip cross-region tests (optional)
--stack-name         Name of the Cloudformation stack where the solution is running
--region             AWS Region, formatted like us-east-1
--profile            AWS profile for CLI commands
EOF
  exit 1
}

cleanup_and_die() {
  trap - SIGINT SIGTERM ERR
  echo "Trapped signal."
  cleanup
  die 1
}

cleanup() {
  # Remove any created resources from the tests
  if [[ $no_clean ]]; then
    echo "Skipping test cleanup"
  else
    python3 python/cleanup_tests.py
  fi
}

msg() {
  echo >&2 -e "${1-}"
}

die() {
  local msg=$1
  local code=${2-1} # default exit status 1
  msg "$msg"
  exit "$code"
}

parse_params() {
  # default values of variables set from params
  declare flag=0
  declare param=''

  while :; do
    case "${1-}" in
    -h | --help) usage ;;
    -v | --verbose) set -x ;;
    --no-clean) no_clean=true ;;
    --skip-cross-region) skip_cross_region=true ;;
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
    --bucket-profile)
      bucket_profile="${2}"
      shift
      ;;
    *) break ;;
    esac
    shift
  done

  # check required params and arguments
  [[ -z ${stack_name} ]] && usage "Missing required parameter: stack-name"
  [[ -z ${region} ]] && usage "Missing required parameter: region"
  [[ -z ${profile} ]] && usage "Missing required parameter: profile"

  return 0
}

#########################
#####   RUN SCRIPT  #####
#########################

trap cleanup_and_die SIGINT SIGTERM ERR

parse_params "$@"
msg "Parameters:"
msg "- Stack name: ${stack_name}"
msg "- Region: ${region}"
msg "- Profile: ${profile}"

# Get reference for all important folders
current_dir="$PWD"
source_dir="$(
  cd $current_dir/../../../../source
  pwd -P
)"
functional_tests_dir="$source_dir/tests/functional_tests"

export REGION=$region
export STACK_PROFILE=$profile
export STACK=$stack_name
export CUSTOMER_ID=$(LC_ALL=C tr -dc a-z </dev/urandom | head -c 8)
export AMC_INSTANCE_ID="amc$(LC_ALL=C tr -dc a-z </dev/urandom | head -c 8)"
export AMAZON_ADS_ADVERTISER_ID=$(LC_ALL=C tr -dc A-Z </dev/urandom | head -c 8)
export AMAZON_ADS_MARKETPLACE_ID=$(LC_ALL=C tr -dc A-Z </dev/urandom | head -c 8)
if [[ $bucket_profile ]]; then
  echo "--bucket-profile provided, running cross account test"
  export BUCKET_PROFILE=$bucket_profile
fi
if [[ $skip_cross_region ]]; then
    echo "--skip-cross-region provided, skipping cross region test"
    export TEST_CROSS_REGION="No"
  else
    export TEST_CROSS_REGION="Yes"
fi

echo ""
sleep 3

# Make sure aws cli is installed
if [[ ! -x "$(command -v aws)" ]]; then
  echo "ERROR: This script requires the AWS CLI to be installed. Please install it then run again."
  exit 1
fi

# Create a temporary Python virtualenv if no venv is active.
source $functional_tests_dir/helper/create_venv.sh
create_venv

echo "------------------------------------------------------------------------------"
echo "Checking deployed stack resources"
echo "------------------------------------------------------------------------------"

# check that amc dataset has been deployed in this stack
export TEST_AMC_DATASET=$(aws cloudformation describe-stacks --stack-name "$stack_name" --region "$region" --profile "$profile" --query "Stacks[].Parameters[?ParameterKey=='ShouldDeployDataLake'].ParameterValue" --output text)
if [[ "$TEST_AMC_DATASET" == "Yes" ]]; then
  export STAGE_BUCKET=$(aws cloudformation describe-stack-resource --stack-name "$STACK" --region "$region" --profile "$profile" --logical-resource-id foundationsstagebucket7D53680B --query StackResourceDetail.PhysicalResourceId --output text)
  export SDLF_CUSTOMER_TABLE=$(aws cloudformation describe-stack-resource --stack-name "$STACK" --region "$region" --profile "$profile" --logical-resource-id foundationssdlfCustomerConfig45371CE6 --query StackResourceDetail.PhysicalResourceId --output text)
else
    echo "AMC Dataset has not been deployed. There is nothing to test"
    cleanup_and_die
fi

# grab important resources from the stack that will be used in testing/cleanup
export WFM_CUSTOMER_TABLE=$(aws cloudformation describe-stack-resource --stack-name "$STACK" --region "$region" --profile "$profile" --logical-resource-id wfmwfmCustomerConfig2F0732CB --query StackResourceDetail.PhysicalResourceId --output text)
export TPS_CUSTOMER_TABLE=$(aws cloudformation describe-stack-resource --stack-name "$STACK" --region "$region" --profile "$profile" --logical-resource-id tpstpsCustomerConfigDCD9A325 --query StackResourceDetail.PhysicalResourceId --output text)
export CLOUDTRAIL_NAME=$(aws cloudformation describe-stack-resource --stack-name "$STACK" --region "$region" --profile "$profile" --logical-resource-id cloudtrailFullApplicationCloudTrailS3AndLambda833794F2 --query StackResourceDetail.PhysicalResourceId --output text)

echo "------------------------------------------------------------------------------"
echo "Running pytest"
echo "------------------------------------------------------------------------------"

pytest python/test_amc.py -s -W ignore::DeprecationWarning -p no:cacheproviders -vv

echo "------------------------------------------------------------------------------"
echo "Finishing process"
echo "------------------------------------------------------------------------------"

cleanup

echo "------------------------------------------------------------------------------"
echo "Done"
echo "------------------------------------------------------------------------------"
exit 0

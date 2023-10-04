#!/bin/bash

###############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Run functional tests depending on configuration deployed
#
# PRELIMINARY:
#  Deploy the solution before running this script
#
# USAGE:
#  ./run-test.sh [-h] [-v] [-n] --stack-name {STACK_NAME} --region {REGION} --profile {PROFILE}
#    STACK_NAME name of the Cloudformation stack where the solution is running.
#    REGION needs to be in a format like us-east-1
#    PROFILE the profile that you have setup in ~/.aws/credentials
#      that you want to use for AWS CLI commands.
#
#    The following options are available:
#
#     -h | --help       Print usage
#     -v | --verbose    Print script debug info
#
###############################################################################

#########################
###  SETUP FUNCTIONS  ###
#########################

usage() {
  msg "$msg"
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] --profile PROFILE --stack-name STACK_NAME --region REGION

Available options:

-h, --help        Print this help and exit (optional)
-v, --verbose     Print script debug info (optional)
--no-clean        Do not clean tests after running (optional)
--in-venv         Run tests in a virtual environment (optional)
--bucket-profile  If testing cross-account, the AWS profile to deploy the amc instance bucket (optional)
--stack-name      Name of the Cloudformation stack where the solution is running
--region          AWS Region, formatted like us-east-1
--profile         AWS profile for CLI commands
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
  if [ $no_clean ]; then
    echo "Skipping test cleanup"
  else
    python3 python/cleanup_tests.py
  fi
  # Deactivate and remove the temporary python virtualenv used to run this script
  if [ $in_venv ]; then
    echo "Run tests in virtualenv, no deactivate"
  else
    if [[ "$VIRTUAL_ENV" != "" ]]; then
      deactivate
      rm -rf $VENV
      rm -rf __pycache__
      echo "------------------------------------------------------------------------------"
      echo "Clean up complete"
      echo "------------------------------------------------------------------------------"
    fi
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
  flag=0
  param=''

  while :; do
    case "${1-}" in
    -h | --help) usage ;;
    -v | --verbose) set -x ;;
    --no-clean) no_clean=true ;;
    --in-venv) in_venv=true ;;
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

export AWS_DEFAULT_REGION=$region
export AWS_PROFILE=$profile
export STACK=$stack_name
export CUSTOMER_ID=$(LC_ALL=C tr -dc a-z </dev/urandom | head -c 8)
if [ $bucket_profile ]; then
  echo "--bucket-profile provided, running cross account test"
  export BUCKET_PROFILE=$bucket_profile
  export TEST_CROSS_ACCOUNT="Yes"
else
  export TEST_CROSS_ACCOUNT="No"
fi

echo ""
sleep 3

# Make sure aws cli is installed
if [[ ! -x "$(command -v aws)" ]]; then
  echo "ERROR: This script requires the AWS CLI to be installed. Please install it then run again."
  exit 1
fi

echo "------------------------------------------------------------------------------"
echo "Creating a temporary Python virtualenv for this script"
echo "------------------------------------------------------------------------------"

command -v python3
if [ $? -ne 0 ]; then
  echo "ERROR: install Python3 before running this script"
  exit 1
fi
if [ $in_venv ]; then
  echo "Using virtual python environment:"
else
  VENV=$(mktemp -d) && echo "$VENV"
  command -v python3 >/dev/null
  if [ $? -ne 0 ]; then
    echo "ERROR: install Python3 before running this script"
    exit 1
  fi
  python3 -m venv "$VENV"
  source "$VENV"/bin/activate
  pip3 install wheel
  pip3 install --quiet -r requirements-test.txt
fi
echo "------------------------------------------------------------------------------"
echo "Checking deployed stack resources"
echo "------------------------------------------------------------------------------"

export TEST_AMC_MICROSERVICES=$(aws cloudformation describe-stacks --stack-name $stack_name --query "Stacks[].Parameters[?ParameterKey=='ShouldDeployMicroservices'].ParameterValue" --output text)
echo Testing AMC Microservices? $TEST_AMC_MICROSERVICES
if [ "$TEST_MICROSERVICES" == "No" ]; then
  echo "There is nothing to test"
  cleanup_and_die
fi
export TEST_AMC_DATASET=$(aws cloudformation describe-stacks --stack-name $stack_name --query "Stacks[].Parameters[?ParameterKey=='ShouldDeployDataLake'].ParameterValue" --output text)
echo Testing AMC Dataset? $TEST_AMC_DATASET
if [ "$TEST_AMC_DATASET" == "Yes" ]; then
  export STAGE_BUCKET=$(aws cloudformation describe-stack-resource --stack-name $STACK --logical-resource-id foundationsstagebucket7D53680B --query StackResourceDetail.PhysicalResourceId --output text)
  export SDLF_CUSTOMER_TABLE=$(aws cloudformation describe-stack-resource --stack-name $STACK --logical-resource-id foundationssdlfCustomerConfig45371CE6 --query StackResourceDetail.PhysicalResourceId --output text)
fi

export WFM_CUSTOMER_TABLE=$(aws cloudformation describe-stack-resource --stack-name $STACK --logical-resource-id wfmwfmCustomerConfig2F0732CB --query StackResourceDetail.PhysicalResourceId --output text)
export TPS_CUSTOMER_TABLE=$(aws cloudformation describe-stack-resource --stack-name $STACK --logical-resource-id tpstpsCustomerConfigDCD9A325 --query StackResourceDetail.PhysicalResourceId --output text)
export CLOUDTRAIL_NAME=$(aws cloudformation describe-stack-resource --stack-name $STACK --logical-resource-id cloudtrailS3AndLambda4948E025 --query StackResourceDetail.PhysicalResourceId --output text)

echo "------------------------------------------------------------------------------"
echo "Running pytest"
echo "------------------------------------------------------------------------------"

pytest python/test_amc.py -s -W ignore::DeprecationWarning -p no:cacheproviders

echo "------------------------------------------------------------------------------"
echo "Finishing process"
echo "------------------------------------------------------------------------------"

cleanup

echo "------------------------------------------------------------------------------"
echo "Done"
echo "------------------------------------------------------------------------------"
exit 0

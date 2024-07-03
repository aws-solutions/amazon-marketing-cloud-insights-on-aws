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
    python3 python/cleanup_tests.py
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
    --extras)
      extras="${2} ${3}"
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
msg "- Extras: ${extras}"

# Because we are using pytest, certain env variables are being loaded and overiding
# our exported env variables set here. We have to use variable names other
# than AWS_REGION, STACK_NAME, AWS_PROFILE, etc.
export STACK_REGION="$region" 
export STACK_PROFILE="$profile"
export STACK="$stack_name"

# Make sure aws cli is installed
if [[ ! -x "$(command -v aws)" ]]; then
  echo "ERROR: This script requires the AWS CLI to be installed. Please install it then run again."
  exit 1
fi

echo "------------------------------------------------------------------------------"
echo "Checking deployed stack resources"
echo "------------------------------------------------------------------------------"

# check that amc microservices have been deployed in this stack
export TEST_AMC_MICRO=$(aws cloudformation describe-stacks --stack-name "$stack_name" --region "$region" --profile "$profile" --query "Stacks[].Parameters[?ParameterKey=='ShouldDeployMicroservices'].ParameterValue" --output text)
if [[ "$TEST_AMC_MICRO" == "Yes" ]]; then
  export CREATE_SCHEDULE_LAMBDA=$(aws cloudformation describe-stack-resource --stack-name "$stack_name" --region "$region" --profile "$profile" --logical-resource-id wfmwfmCreateWorkflowSchedule332BD3C0 --query StackResourceDetail.PhysicalResourceId --output text)
  echo $CREATE_SCHEDULE_LAMBDA
  export DELETE_SCHEDULE_LAMBDA=$(aws cloudformation describe-stack-resource --stack-name "$stack_name" --region "$region" --profile "$profile" --logical-resource-id wfmwfmDeleteWorkflowSchedule7328E733 --query StackResourceDetail.PhysicalResourceId --output text)
  echo $DELETE_SCHEDULE_LAMBDA
  export RULE_SUFFIX=$(LC_ALL=C tr -dc a-z </dev/urandom | head -c 8)
  echo $RULE_SUFFIX
else
    echo "AMC Microservices have not been deployed. There is nothing to test"
    cleanup_and_die
fi

echo "------------------------------------------------------------------------------"
echo "Running pytest"
echo "------------------------------------------------------------------------------"

pytest python/test_scheduler.py ${extras-} -s -W ignore::DeprecationWarning -p no:cacheproviders -vv

echo "------------------------------------------------------------------------------"
echo "Finishing process"
echo "------------------------------------------------------------------------------"

cleanup

echo "------------------------------------------------------------------------------"
echo "Done"
echo "------------------------------------------------------------------------------"
exit 0
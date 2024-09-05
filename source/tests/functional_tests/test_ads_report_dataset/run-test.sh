#!/bin/bash

###############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Run functional tests for the ads report dataset
#
# PRELIMINARY:
#  Deploy the solution before running this script
###############################################################################

#########################
###  SETUP FUNCTIONS  ###
#########################

usage() {
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] --profile PROFILE --stack-name STACK_NAME --region REGION

Available options:

-h, --help           Print this help and exit (optional)
-v, --verbose        Print script debug info (optional)
--no-clean           Do not clean tests after running (optional)
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
  declare flag=0
  declare param=''

  while :; do
    case "${1-}" in
    -h | --help) usage ;;
    -v | --verbose) set -x ;;
    --no-clean) no_clean=true ;;
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
    *) break ;;
    esac
    shift
  done

  [[ -z ${stack_name} ]] && usage "Missing required parameter: stack-name"
  [[ -z ${region} ]] && usage "Missing required parameter: region"
  [[ -z ${profile} ]] && usage "Missing required parameter: profile"

  return 0
}

#########################
#####   RUN SCRIPT  #####
#########################
echo "------------------------------------------------------------------------------"
echo "** Initializing functional test for Ads Report Dataset **"
echo "------------------------------------------------------------------------------"

trap cleanup_and_die SIGINT SIGTERM ERR

parse_params "$@"
msg "Parameters:"
msg "- Stack name: ${stack_name}"
msg "- Region: ${region}"
msg "- Profile: ${profile}"

current_dir="$PWD"
source_dir="$(
  cd "$current_dir/../../../../source"
  pwd -P
)"
functional_tests_dir="$source_dir/tests/functional_tests"

export REGION=$region
export STACK_PROFILE=$profile
export STACK=$stack_name

echo ""
sleep 3

if [[ ! -x "$(command -v aws)" ]]; then
  echo "ERROR: This script requires the AWS CLI to be installed. Please install it then run again."
  exit 1
fi

source "$functional_tests_dir/helper/create_venv.sh"
create_venv

echo "------------------------------------------------------------------------------"
echo "** Check and assign deployed stack resources **"
echo "------------------------------------------------------------------------------"

export TEST_ADS_REPORT_DATASET=$(aws cloudformation describe-stacks --stack-name "$stack_name" --region "$region" --profile "$profile" --query "Stacks[].Parameters[?ParameterKey=='ShouldDeployDataLake'].ParameterValue" --output text)
if [[ "$TEST_ADS_REPORT_DATASET" == "Yes" ]]; then
  export STAGE_BUCKET=$(aws cloudformation describe-stack-resource --stack-name "$STACK" --region "$region" --profile "$profile" --logical-resource-id foundationsstagebucket7D53680B --query StackResourceDetail.PhysicalResourceId --output text)
  echo "STAGE_BUCKET: ${STAGE_BUCKET}"
  export REPORTS_BUCKET=$(aws cloudformation describe-stack-resource --stack-name "$STACK" --region "$region" --profile "$profile" --logical-resource-id reportsBucket430E871A --query StackResourceDetail.PhysicalResourceId --output text)
  echo "REPORTS_BUCKET: ${REPORTS_BUCKET}"
else
  echo "Ads Report Dataset has not been deployed. There is nothing to test"
  die 1
fi

echo "------------------------------------------------------------------------------"
echo "** Create and assign test input **"
echo "------------------------------------------------------------------------------"

random_suffix=$(LC_ALL=C tr -dc 'a-z' </dev/urandom | head -c 6)
export TEST_TABLE="test_${random_suffix}"
echo "TEST_TABLE: ${TEST_TABLE}"
export TEST_FILENAME="report-123456789"
echo "TEST_FILENAME: ${TEST_FILENAME}"
export MOCK_DATA="ads-report-mock-data.json.gz"
echo "MOCK_DATA: ${MOCK_DATA}"

echo "------------------------------------------------------------------------------"
echo "** Running pytest **"
echo "------------------------------------------------------------------------------"

export PYTHONPATH=$functional_tests_dir
pytest python/test_ads_report.py -s -W ignore::DeprecationWarning -p no:cacheproviders -vv

echo "------------------------------------------------------------------------------"
echo "** Cleaning tests **"
echo "------------------------------------------------------------------------------"

cleanup

echo "------------------------------------------------------------------------------"
echo "** Done **"
echo "------------------------------------------------------------------------------"
exit 0

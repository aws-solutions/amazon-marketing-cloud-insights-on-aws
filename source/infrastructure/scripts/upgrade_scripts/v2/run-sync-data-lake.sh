#!/bin/bash

###############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Backfill data in your amc instance buckets to v2 of the solution
#
# PRELIMINARY:
#  Deploy v2 of the solution with all relevent customers configured through the TPS service.
#   Refer to the implementation guide for more information.
#
# USAGE:
#  sh run-sync-data-lake.sh [-h] [-v] --v2-stack-name {STACK_NAME} --v2-region {REGION} --v2-profile {PROFILE}
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
#####   FUNCTIONS   #####
#########################

usage() {
  msg "$msg"
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] --v2-stack-name V2_STACK_NAME [--v2-region V2_REGION] [--v2-profile V2_PROFILE]

Available options:

-h, --help        Print this help and exit (optional)
-v, --verbose     Print script debug info (optional)
--v2-stack-name   Name of the Cloudformation stack where v2 of the solution is running
--v2-region       AWS Region where v2 is deployed, formatted like us-east-1
--v2-profile      AWS profile for CLI commands in the v2-deployed AWS account
--bucket-profile  If migrating cross-account, the AWS profile for the deployed the amc instance bucket (optional)
EOF
  exit 1
}

cleanup_and_die() {
  trap - SIGINT SIGTERM ERR
  echo "Trapped signal."
  rollback
  cleanup
  die 1
}

rollback() {
  rm -rf data/results
}

cleanup() {
  # Deactivate and remove the temporary python virtualenv used to run this script
  if [[ "$VIRTUAL_ENV" != "" ]];
  then
    deactivate
    rm -rf $VENV
    rm -rf __pycache__
    echo "------------------------------------------------------------------------------"
    echo "Clean up complete"
    echo "------------------------------------------------------------------------------"
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
    --v2-stack-name)
      v2_stack_name="${2}"
      shift
      ;;
    --v2-region)
      v2_region="${2}"
      shift
      ;;
    --v2-profile)
      v2_profile="${2}"
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
  [[ -z "${v2_stack_name}" ]] && usage "Missing required parameter: v2-stack-name"

  return 0
}

#########################
#####   RUN SCRIPT  #####
#########################

trap cleanup_and_die SIGINT SIGTERM ERR

# remove any leftover data from previous run
rollback

parse_params "$@"
msg "Parameters:"
msg "- v2 stack name: ${v2_stack_name}"
[[ ! -z "${v2_region}" ]] && msg "- v2 Region: ${v2_region}"
[[ ! -z "${v2_profile}" ]] && msg "- v2 Profile: ${v2_profile}"

echo ""
sleep 3

# make sure aws cli is installed
if [[ ! -x "$(command -v aws)" ]]; then
  echo "ERROR: This script requires the AWS CLI to be installed. Please install it then run again."
  exit 1
fi

# assign environment variables
export V2_STACK_NAME=$v2_stack_name

if [[ -z "${v2_profile}" ]]; then
    echo "--v2-profile not set. Using profile $AWS_PROFILE."
    export V2_PROFILE=$AWS_PROFILE
else
    export V2_PROFILE=$v2_profile
fi

if [[ -z "${v2_region}" ]]; then
    echo "--v2-region not set. Using region $AWS_DEFAULT_REGION."
    export V2_REGION=$AWS_DEFAULT_REGION
else
    export V2_REGION=$v2_region
fi

if [[ $bucket_profile ]]; then
  export BUCKET_PROFILE=$bucket_profile
fi

echo "------------------------------------------------------------------------------"
echo "Creating a temporary Python virtualenv for this script"
echo "------------------------------------------------------------------------------"

python3 -c "import os; print (os.getenv('VIRTUAL_ENV'))" | grep -q None
if [ $? -ne 0 ]; then
  echo "ERROR: Do not run this script inside Virtualenv. Type \`deactivate\` and run again.";
  exit 1;
fi
command -v python3
if [ $? -ne 0 ]; then
  echo "ERROR: install Python3 before running this script"
  exit 1
fi
echo "Using virtual python environment:"
VENV=$(mktemp -d) && echo "$VENV"
command -v python3 > /dev/null
if [ $? -ne 0 ]; then
  echo "ERROR: install Python3 before running this script"
  exit 1
fi
python3 -m venv "$VENV"
source "$VENV"/bin/activate
pip3 install wheel
pip3 install --quiet -r requirements-upgrade.txt

echo "------------------------------------------------------------------------------"
echo "Checking deployed v2 resources"
echo "------------------------------------------------------------------------------"

export AWS_PROFILE=$V2_PROFILE
export AWS_DEFAULT_REGION=$V2_REGION

export V2_TPS_CUSTOMER_CONFIG=$(aws cloudformation describe-stack-resource --stack-name $V2_STACK_NAME --logical-resource-id tpstpsCustomerConfigDCD9A325 --query StackResourceDetail.PhysicalResourceId --output text)
export V2_DATA_LAKE_TRIGGER_LAMBDA=$(aws cloudformation describe-stack-resource --stack-name $V2_STACK_NAME --logical-resource-id datalakepipelinedatalakeroutingBEE8F1BC --query StackResourceDetail.PhysicalResourceId --output text)

echo "------------------------------------------------------------------------------"
echo "Creating file directories"
echo "------------------------------------------------------------------------------"

if [ ! -d "data" ]; then
  mkdir data
fi

mkdir data/results

echo "------------------------------------------------------------------------------"
echo "Starting data sync to v2"
echo "------------------------------------------------------------------------------"

python3 python/data_lake_sync.py ./data/results

echo "------------------------------------------------------------------------------"
echo "Finishing process"
echo "------------------------------------------------------------------------------"

cleanup

echo "------------------------------------------------------------------------------"
echo "Done"
echo "------------------------------------------------------------------------------"
exit 0


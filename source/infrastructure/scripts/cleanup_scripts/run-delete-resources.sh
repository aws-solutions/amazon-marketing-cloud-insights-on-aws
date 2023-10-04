#!/bin/bash

###############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Run delete resource scripts to remove all remaining soluton resources
#
# PRELIMINARY:
#  Perform manual deletion steps before running this script.
#   Refer to the implementation guide for instruction.
#
# USAGE:
#  ./run-delete-resources.sh [-h] [-v] --stack-name {STACK_NAME} --region {REGION} --profile {PROFILE}
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
--stack-name      Name of the Cloudformation stack where the solution is running
--region          AWS Region, formatted like us-east-1
--profile         AWS profile for CLI commands
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

rollback() {
  rm -rf delete_file.json
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

# remove any leftover data from previous run
rollback

parse_params "$@"
msg "Parameters:"
msg "- Stack name: ${stack_name}"
msg "- Region: ${region}"
msg "- Profile: ${profile}"

export AWS_DEFAULT_REGION=$region
export AWS_PROFILE=$profile
export STACK_NAME=$stack_name

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
pip3 install --quiet -r requirements-cleanup.txt

echo "------------------------------------------------------------------------------"
echo "Setting up files"
echo "------------------------------------------------------------------------------"

touch delete_file.json

echo "------------------------------------------------------------------------------"
echo "Listing resources to delete"
echo "------------------------------------------------------------------------------"

python3 ./list_resources_to_delete.py $STACK_NAME $AWS_PROFILE

echo "------------------------------------------------------------------------------"
echo "Deleting resources"
echo "------------------------------------------------------------------------------"

python3 ./delete_resources.py $AWS_PROFILE

echo "------------------------------------------------------------------------------"
echo "Deleting stack"
echo "------------------------------------------------------------------------------"

aws cloudformation delete-stack --stack-name $STACK_NAME --profile $AWS_PROFILE

echo "------------------------------------------------------------------------------"
echo "Finishing process"
echo "------------------------------------------------------------------------------"

cleanup

echo "------------------------------------------------------------------------------"
echo "Done"
echo "------------------------------------------------------------------------------"
exit 0
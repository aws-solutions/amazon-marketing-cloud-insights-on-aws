#!/bin/bash

###############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Download data locally from v1 of the solution
#
# PRELIMINARY:
#  Copy and paste the ddk.json file from your deployed v1 solution into the v1_ddk.json file located
#   in this scripts folder. Refer to the implementation guide for more information.
#
# USAGE:
#  sh run-download-microservices.sh [-h] [-v] --v1-region {REGION} --v1-profile {PROFILE}
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
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] [--v1-profile V1_PROFILE] [--v1-region V1_REGION]

Available options:

-h, --help        Print this help and exit (optional)
-v, --verbose     Print script debug info (optional)
--v1-region       AWS Region where v1 is deployed, formatted like us-east-1
--v1-profile      AWS profile for CLI commands in the v1-deployed AWS account
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
  rm -rf data/download
  rm -rf data/upload
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
    --v1-profile)
      v1_profile="${2}"
      shift
      ;;
    --v1-region)
      v1_region="${2}"
      shift
      ;;
    -?*) die "Unknown option: $1" ;;
    *) break ;;
    esac
    shift
  done

  args=("$@")

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
[[ ! -z "${v1_region}" ]] && msg "- v1 Region: ${v1_region}"
[[ ! -z "${v1_profile}" ]] && msg "- v1 Profile: ${v1_profile}"

echo ""
sleep 3

# make sure aws cli is installed
if [[ ! -x "$(command -v aws)" ]]; then
  echo "ERROR: This script requires the AWS CLI to be installed. Please install it then run again."
  exit 1
fi

# assign environment variables
if [[ -z "${v1_profile}" ]]; then
    echo "--v1-profile not set. Using profile $AWS_PROFILE."
    export V1_PROFILE=$AWS_PROFILE
else
    export V1_PROFILE=$v1_profile
fi

if [[ -z "${v1_region}" ]]; then
    echo "--v1-region not set. Using region $AWS_DEFAULT_REGION."
    export V1_REGION=$AWS_DEFAULT_REGION
else
    export V1_REGION=$v1_region
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
echo "Checking deployed v1 resources"
echo "------------------------------------------------------------------------------"

if [[ -z $(grep '[^[:space:]]' v1_ddk.json) ]] ; then
  echo "ERROR: v1_ddk.json file is empty. Please copy and paste the required data then run again."
  exit 1
fi

echo "------------------------------------------------------------------------------"
echo "Creating file directories"
echo "------------------------------------------------------------------------------"

if [ ! -d "data" ]; then
  mkdir data
fi

mkdir data/download
mkdir data/upload
mkdir data/upload/ready
mkdir data/upload/invalid

echo "------------------------------------------------------------------------------"
echo "Downloading data from v1"
echo "------------------------------------------------------------------------------"

export AWS_PROFILE=$V1_PROFILE
export AWS_DEFAULT_REGION=$V1_REGION
export DDK_PATH="./v1_ddk.json"
export WRITE_PATH="./data/download"

python3 python/download_records.py

echo "------------------------------------------------------------------------------"
echo "Formatting data for upload to v2"
echo "------------------------------------------------------------------------------"

export READ_PATH="./data/download"
export SUCCESS_PATH="./data/upload/ready"
export FAILED_PATH="./data/upload/invalid"
python3 python/format_records.py

echo "------------------------------------------------------------------------------"
echo "Finishing process"
echo "------------------------------------------------------------------------------"

cleanup

echo "------------------------------------------------------------------------------"
echo "Done"
echo "------------------------------------------------------------------------------"
exit 0
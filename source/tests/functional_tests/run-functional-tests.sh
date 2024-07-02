#!/bin/bash
###############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Run functional tests depending on configuration deployed
###############################################################################

usage() {
  msg "$msg"
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v] --profile PROFILE --stack-name STACK_NAME --region REGION --email EMAIL

Available options:

-h, --help           Print this help and exit (optional)
-v, --verbose        Print script debug info (optional)
--no-clean           Do not clean tests after running (optional)
--in-venv            Run test in an existing virtual environment [--in-venv 1] (optional)
--stack-name         Name of the Cloudformation stack where the solution is running (optional)
--bucket-profile     If testing cross-account, the AWS profile to deploy the amc instance bucket (optional)
--region             AWS Region, formatted like us-east-1
--profile            AWS profile for CLI commands
--email              Email to receive notifications
--skip-cross-region  Skip cross-region tests (optional)
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
    --no-clean) no_clean=true ;;
    --skip-cross-region) skip_cross_region=true ;;
    --stack-name)
      stack_name="${2}"
      using_existing_stack=true
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
    --bucket-profile)
      bucket_profile="${2}"
      shift
      ;;
    --in-venv)
      in_venv="${2}"
      shift
      ;;
    *) break ;;
    esac
    shift
  done

  args=("$@")

  # check required params and arguments
  [[ -z "${region}" ]] && usage "Missing required parameter: region"
  [[ -z "${profile}" ]] && usage "Missing required parameter: profile"
  [[ -z "${email}" ]] && usage "Missing required parameter: email"

  return 0
}

# Create a random stack name if --stack-name argument is not passed
stack_name="amci$(LC_ALL=C tr -dc a-z </dev/urandom | head -c 4)"

parse_params "$@"

msg "Parameters:"
msg "- Stack name: ${stack_name}"
msg "- Region: ${region}"
msg "- Profile: ${profile}"
msg "- Email: ${email}"
msg "- in_venv: ${in_venv}"

role_arn=0
current_dir="$PWD"
source_dir="$(
  cd $current_dir/../../../source
  pwd -P
)"

# Make sure aws cli is installed
if [[ ! -x "$(command -v aws)" ]]; then
  echo "ERROR: This script requires the AWS CLI to be installed. Please install it then run again."
  exit 1
fi

# Create a temporary Python virtualenv if no venv is active.
source $current_dir/helper/create_venv.sh
create_venv in_venv

if [ $using_existing_stack ]; then
  echo "Use an existing stack, skip testing stack synthesize and deploy"
else
  echo "------------------------------------------------------------------------------"
  echo "Deploying a new AMC Insights stack"
  echo "------------------------------------------------------------------------------"

  # create a new cloudformation role with IAM_POLICY_INSTALL.json to test when deploying
  echo "Creating cloudformation role with IAM_POLICY_INSTALL.json"
  cd "$current_dir/test_install_policy"
  export INSTALL_ROLE_NAME="${stack_name}-TestInstall"
  return_values=$(python3 create_role.py $profile $region)
  policy_arn=$(cut -d'%' -f1 <<<"$return_values")
  role_arn=$(cut -d'%' -f2 <<<"$return_values")

  echo "Test stack $stack_name synthesize and deploy"
  cd "$current_dir/test_stack_deploy"
  ./run-test.sh --stack-name $stack_name --profile $profile --region $region --email $email --role-arn $role_arn

  if [[ $? != 0 ]]; then
    echo "Ending tests - stack failed to deploy. Check the Cloudformation console for error logs."
    exit
  fi
fi

echo "------------------------------------------------------------------------------"
echo "Running AMC Dataset/Microservice tests"
echo "------------------------------------------------------------------------------"
cd "$current_dir/test_amc_dataset"
if [ $no_clean ]; then
  if [ $bucket_profile ]; then
    ./run-test.sh --stack-name $stack_name --profile $profile --region $region --no-clean --bucket-profile $bucket_profile
  else
    ./run-test.sh --stack-name $stack_name --profile $profile --region $region --no-clean
  fi
else
  if [ $bucket_profile ]; then
    ./run-test.sh --stack-name $stack_name --profile $profile --region $region --bucket-profile $bucket_profile
  else
    ./run-test.sh --stack-name $stack_name --profile $profile --region $region
  fi
fi

echo "------------------------------------------------------------------------------"
echo "Running Extended Dataset tests **$role_arn"
echo "------------------------------------------------------------------------------"
cd "$current_dir/test_extended_dataset"
./run-test.sh --stack-name $stack_name --profile $profile --region $region --email $email --role-arn $role_arn

echo "------------------------------------"
echo "CLEAN UP STACK RESOURCES"
echo "------------------------------------"
export AWS_DEFAULT_REGION=$region
export AWS_PROFILE=$profile
export STACK=$stack_name

if [ $using_existing_stack ] || [ $no_clean ]; then
  if [ $using_existing_stack ]; then
    echo "Using an existing stack, doesn't delete stack after testing"
  else
    echo "--no-clean param passed, doesn't delete stack after testing"
  fi
else
  echo "------------------------------------------------------------------------------"
  echo "Deleting stack"
  echo "------------------------------------------------------------------------------"

  cd "$current_dir/test_stack_deploy"
  python3 python/cleanup_resources.py
  exit_status=$?
  if [ $exit_status -ne 0 ]; then
    echo "An error occurred while deleting stack. IAM_POLICY_INSTALL role is retained."
  else
      echo "Deleting test IAM_POLICY_INSTALL role and policy"
      cd "$current_dir/test_install_policy"
      python3 delete_role.py $profile $region $policy_arn
  fi
fi

if [[ ${in_venv:-0} -ne 1 ]]; then
  echo "Deactivate virtualenv"
  deactivate
else
  echo "Run tests in virtualenv, no deactivate"
fi

echo "End of all tests"
exit 0

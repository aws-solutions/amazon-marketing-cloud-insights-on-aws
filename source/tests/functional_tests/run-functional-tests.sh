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

-h, --help        Print this help and exit (optional)
-v, --verbose     Print script debug info (optional)
--no-clean        Do not clean tests after running (optional)
--in-venv         Run tests in a virtual environment (optional)
--stack-name      Name of the Cloudformation stack where the solution is running (optional)
--bucket-profile  If testing cross-account, the AWS profile to deploy the amc instance bucket (optional)
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
    --no-clean) no_clean=true ;;
    --in-venv) in_venv=true ;;
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
stack_name="amci$(LC_ALL=C tr -dc a-z </dev/urandom | head -c 8)"

parse_params "$@"

msg "Parameters:"
msg "- Stack name: ${stack_name}"
msg "- Region: ${region}"
msg "- Profile: ${profile}"
msg "- Email: ${email}"

# Make sure aws cli is installed
if [[ ! -x "$(command -v aws)" ]]; then
  echo "ERROR: This script requires the AWS CLI to be installed. Please install it then run again."
  exit 1
fi

create_venv() {
  if [ $in_venv ]; then
    echo "Run tests in a virtualenv, skip creating virtualenv"
  else
    echo "------------------------------------------------------------------------------"
    echo "Creating a temporary Python virtualenv for this script"
    echo "------------------------------------------------------------------------------"
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
}

create_venv

current_dir="$PWD"
source_dir="$(
  cd $current_dir/../../../source
  pwd -P
)"

##############################################
######   TEST STACK SYNTHESIZE & DEPLOY  #####
##############################################
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
  return_values=`python3 create_role.py $profile $region`
  policy_arn=$(cut -d'%' -f1 <<< "$return_values")
  role_arn=$(cut -d'%' -f2 <<< "$return_values")

  echo "Test stack $stack_name synthesize and deploy"
  cd "$current_dir/test_stack_deploy"
  sh run-test.sh --stack-name $stack_name --profile $profile --region $region --email $email --role-arn $role_arn
  
  if [[ $? != 0 ]]; then
    echo "Ending tests - stack failed to deploy. Check the Cloudformation console for error logs."
    exit
  fi
fi

#######################
#####   TEST AMC  #####
#######################
cd "$source_dir/infrastructure/scripts/test_scripts"

echo "------------------------------------------------------------------------------"
echo "Running AMC Dataset/Microservice tests"
echo "------------------------------------------------------------------------------"
if [ $no_clean ]; then
  if [ $bucket_profile ]; then
    sh run-test.sh --stack-name $stack_name --profile $profile --region $region --no-clean --in-venv --bucket-profile $bucket_profile
  else
    sh run-test.sh --stack-name $stack_name --profile $profile --region $region --no-clean --in-venv
  fi
else
  if [ $bucket_profile ]; then
    sh run-test.sh --stack-name $stack_name --profile $profile --region $region --in-venv --bucket-profile $bucket_profile
  else
    sh run-test.sh --stack-name $stack_name --profile $profile --region $region --in-venv
  fi
fi

####################################
#####   TEST EXTENDED DATASET  #####
####################################
cd "$current_dir/test_extended_dataset"

echo "------------------------------------------------------------------------------"
echo "Running Extended Dataset tests"
echo "------------------------------------------------------------------------------"
sh run-test.sh --stack-name $stack_name --profile $profile --region $region --email $email

########################################
#####   CLEAN UP STACK RESOURCES   #####
########################################
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

  echo "Delete test IAM_POLICY_INSTALL role and policy"
  cd "$current_dir/test_install_policy"
  python3 delete_role.py $profile $region $policy_arn
fi

if [ $in_venv ]; then
  echo "Run tests in virtualenv, no deactivate"
else
  echo "Deactivate virtualenv"
  deactivate
fi

echo "End of all tests"
exit 0
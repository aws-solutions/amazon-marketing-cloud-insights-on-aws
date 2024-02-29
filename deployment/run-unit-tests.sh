#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

#
# This assumes all of the OS-level configuration has been completed and git repo has already been cloned
#
# This script should be run from the repo's deployment directory
# cd deployment
# ./run-unit-tests.sh
#

[ "$DEBUG" == 'true' ] && set -x
# set -e

# Get reference for all important folders
template_dir="$PWD"
source_dir="$(cd $template_dir/../source; pwd -P)"
root_dir="$template_dir/.."
venv_folder=".venv-temp"
tests_folder="$source_dir/tests/unit_tests"

while :; do
  case "${1-}" in
  --test-file-name)
      TEST_FILE_NAME="${2}" ;;
  *) break ;;
  esac
  shift
done


# check if we need a new testing venv, or use active (workstation testing)
python3 ./venv_check.py
if [ $? == 1 ]; then
    echo "------------------------------------------------------------------------------"
    echo "[Env] Create clean virtual environment and install dependencies"
    echo "------------------------------------------------------------------------------"
    cd $root_dir
    if [ -d $venv_folder ]; then
      rm -rf $venv_folder
    fi
    python3.11 -m venv $venv_folder
    source $venv_folder/bin/activate

    using_test_venv=1
    # configure the environment
    cd $source_dir
    pip install --upgrade pip
    pip install -r $source_dir/requirements.txt
else
  using_test_venv=0
  echo "------------------------------------------------------------------------------"
  echo "[Env] Using active virtual environment for tests"
  echo "------------------------------------------------------------------------------"
  python_version=$(python --version 2>&1 | cut -d ' ' -f 2)
  if [[ "$python_version" != "11"* ]]; then
    echo "You are using Python version $python_version. Python version 11 is required."
    echo "Update your environment or run tests again without an active environment."
    exit 1
  fi
  echo ''
fi


echo "------------------------------------------------------------------------------"
echo "[Test] Run pytest with coverage"
echo "------------------------------------------------------------------------------"
cd $source_dir
# setup coverage report path
coverage_report_path=$tests_folder/coverage-reports/source.coverage.xml
echo "coverage report path set to $coverage_report_path"
TEST_FILE_NAME=$tests_folder/$TEST_FILE_NAME

pytest $TEST_FILE_NAME --cov=$source_dir/infrastructure --cov-report term-missing --cov-report term --cov-report "xml:$coverage_report_path" --cov-config=$source_dir/.coveragerc -vv

# The pytest --cov with its parameters and .coveragerc generates a xml cov-report with `coverage/sources` list
# with absolute path for the source directories. To avoid dependencies of tools (such as SonarQube) on different
# absolute paths for source directories, this substitution is used to convert each absolute source directory
# path to the corresponding project relative path. The $source_dir holds the absolute path for source directory.
sed -i -e "s,<source>$source_dir,<source>source,g" $coverage_report_path

if [ $using_test_venv == 1 ]; then
  echo "------------------------------------------------------------------------------"
  echo "[Env] Deactivating test virtual environment"
  echo "------------------------------------------------------------------------------"
  echo ''
  # deactivate the virtual environment
  deactivate
else
  echo "------------------------------------------------------------------------------"
  echo "[Env] Leaving virtual environment active"
  echo "------------------------------------------------------------------------------"
  echo ''

fi

cd $template_dir


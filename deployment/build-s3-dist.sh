#!/bin/bash
#
# This assumes all of the OS-level configuration has been completed and git repo has already been cloned
#
# This script should be run from the repo's deployment directory
# cd deployment
# ./build-s3-dist.sh source-bucket-base-name solution-name version-code
#
# Paramenters:
#  - source-bucket-base-name: Name for the S3 bucket location where the template will source the Lambda
#    code from. The template will append '-[region_name]' to this bucket name.
#    For example: ./build-s3-dist.sh solutions v1.0.0
#    The template will then expect the source code to be located in the solutions-[region_name] bucket
#
#  - solution-name: name of the solution for consistency
#
#  - version-code: version of the package

# set -euo pipefail

# Check to see if input has been provided:
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
    echo "Please provide the base source bucket name, trademark approved solution name and version where the lambda code will eventually reside."
    echo "For example: ./build-s3-dist.sh solutions trademarked-solution-name v1.0.0"
    exit 1
fi

SOLUTION_ID="SO0193"

# Get reference for all important folders
template_dir="$PWD"
template_dist_dir="$template_dir/global-s3-assets"
build_dist_dir="$template_dir/regional-s3-assets"
source_dir="$template_dir/../source"
cdk_out="$template_dir/cdk.out"
venv_folder=".venv-temp"

echo "------------------------------------------------------------------------------"
echo "[Init] Clean old dist, node_modules and bower_components folders"
echo "------------------------------------------------------------------------------"
echo "rm -rf $template_dist_dir"
rm -rf $template_dist_dir
echo "mkdir -p $template_dist_dir"
mkdir -p $template_dist_dir
echo "rm -rf $build_dist_dir"
rm -rf $build_dist_dir
echo "mkdir -p $build_dist_dir"
mkdir -p $build_dist_dir
# remove old cdk.out folder before build
rm -rf $cdk_out

echo "------------------------------------------------------------------------------"
echo "[Rebuild] CDK Solution"
echo "------------------------------------------------------------------------------"

build_venv_name=".venv_build"
build_venv_dir="$template_dir/$build_venv_name"

# clean up testing venv if present
rm -rf $build_venv_dir

# check if we need a new testing venv
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

# generate the templates (unbundled)
cd $source_dir/infrastructure
cdk synth -o $cdk_out
#!/bin/bash
###############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# PURPOSE:
#  Check a temporary Python virtualenv to run tests
###############################################################################

create_venv() {
  in_venv="$1"
  if [[ ${in_venv:-0} -ne 1 ]]; then
    echo "------------------------------------------------------------------------------"
    echo "Creating a temporary Python virtualenv for this run test script"
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
  else
    echo "------------------------------------------------------------------------------"
    echo "[Env] Using active virtual environment for tests"
    echo "------------------------------------------------------------------------------"
  fi
}

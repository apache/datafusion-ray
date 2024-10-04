#!/bin/bash

# This script helps change the path to parquet files in expected plans for
# local development and CI

set -e

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <mode>"
  echo "Modes: pre-ci, local-dev"
  exit 1
fi

# Assign the parameter to the mode variable
mode=$1

ci_dir="home/runner/work/datafusion-ray/datafusion-ray"
current_dir=$(pwd)
current_dir_no_leading_slash="${current_dir#/}"
expected_plans_dir="./testdata/expected-plans"

# Function to replace paths in files
replace_paths() {
  local search=$1
  local replace=$2
  find "$expected_plans_dir" -type f -exec sed -i "s|$search|$replace|g" {} +
  echo "Replaced all occurrences of '$search' with '$replace' in files within '$expected_plans_dir'."
}

# Handle the modes
case $mode in
pre-ci)
  replace_paths "$current_dir_no_leading_slash" "$ci_dir"
  ;;
local-dev)
  replace_paths "$ci_dir" "$current_dir_no_leading_slash"
  ;;
*)
  echo "Invalid mode: $mode"
  echo "Usage: $0 <mode>"
  echo "Modes: pre-ci, local-dev"
  exit 1
  ;;
esac

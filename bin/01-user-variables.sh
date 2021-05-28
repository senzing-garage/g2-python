#!/usr/bin/env bash

SENZING_VERSION=1.3.0

SOURCE_PYTHON_DIR=/opt/senzing-1.3.0/g2/python

export GITHUB_ACCESS_TOKEN=1c8f6b7e7600214afddbbac7fe5f566d8f690e78

GIT_ACCOUNT_DIR=~/senzing-g2.git
TARGET_PYTHON_DIR=${GIT_ACCOUNT_DIR}/g2-python/g2/python

GITHUB_UTIL_DIR=~/senzing.git/github-util
GIT_REPOSITORY_DIR=~/senzing-g2.git/g2-python

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

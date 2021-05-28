#!/usr/bin/env bash

SCRIPT_VERSION=1.0.0

# Get absolute directory.

GIT_REPOSITORY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# Read metadata.

source ${GIT_REPOSITORY_DIR}/00-submodules.sh

# Backup prior data.

export OUTPUT_DIR=${GIT_REPOSITORY_DIR}/g2/python

# Process each entry.

cp /opt/senzing-${}/g2/python
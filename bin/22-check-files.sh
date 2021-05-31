#!/usr/bin/env bash

# Read metadata.

source 01-user-variables.sh

diff -r /opt/senzing-${SENZING_VERSION}/g2/python ${TARGET_PYTHON_DIR}

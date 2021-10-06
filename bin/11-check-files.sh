#!/usr/bin/env bash

# Read metadata.

source 01-user-variables.sh

diff -r ${SOURCE_PYTHON_DIR} ${TARGET_PYTHON_DIR}

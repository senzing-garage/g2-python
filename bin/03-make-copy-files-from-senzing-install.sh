#!/usr/bin/env bash

# Read metadata.

source 01-user-variables.sh

# Create 03-copy-files-from-senzing-install.sh

${GITHUB_UTIL_DIR}/github-util.py print-copy-files-from-senzing-install > ${BIN_DIR}/04-copy-files-from-senzing-install.sh

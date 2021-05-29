#!/usr/bin/env bash

# Read metadata.

source 01-user-variables.sh

# Tag the current version of the collection.

cd ${GIT_REPOSITORY_DIR}
git add ${OUTPUT_DIR}/*
git commit -a -m "#2 SenzingAPI ${SENZING_VERSION}"
git push
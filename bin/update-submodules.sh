#!/usr/bin/env bash

# Establish absolute paths.

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
GIT_REPOSITORY_DIR="$(dirname ${BIN_DIR})"
TARGET_PYTHON_DIR=${GIT_REPOSITORY_DIR}/g2/python

# Populate submodules.

git pull
git submodule update --init --recursive

# Process each submodule.

export SUBMODULES=$(${BIN_DIR}/github-submodule-versions.py list-submodule-versions)
for SUBMODULE in ${SUBMODULES[@]};
do

    # Get metadata.

    IFS=";" read -r -a SUBMODULE_DATA <<< "${SUBMODULE}"
    SUBMODULE_NAME="${SUBMODULE_DATA[0]}"
    SUBMODULE_VERSION="${SUBMODULE_DATA[1]}"
    SUBMODULE_ARTIFACT="${SUBMODULE_DATA[2]}"

    echo "Processing ${SUBMODULE_NAME}:${SUBMODULE_VERSION}"

    # Get requested version of submodule.

    cd ${GIT_REPOSITORY_DIR}/${SUBMODULE_NAME}
    git checkout main
    git pull
    git checkout ${SUBMODULE_VERSION}

    echo "Copy ${SUBMODULE_NAME}/${SUBMODULE_ARTIFACT}"

    # Copy artifact into collection.

    cd ${GIT_REPOSITORY_DIR}/${SUBMODULE_NAME}
    cp ${SUBMODULE_ARTIFACT} ${TARGET_PYTHON_DIR}/

done

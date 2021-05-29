#!/usr/bin/env bash

# Read metadata.

source 01-user-variables.sh
source 11-submodules.sh

# Populate submodules.

cd ${GIT_REPOSITORY_DIR}
git submodule update --init --recursive

# Process each entry.

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
done

# Update submodules.

cd ${GIT_REPOSITORY_DIR}
git commit -a -m "#2 Update submodules for SenzingAPI ${SENZING_VERSION}"
git push
git submodule update --init --recursive
#!/usr/bin/env bash

# Read metadata.

source 01-user-variables.sh
source 11-submodules.sh

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
    git checkout ${GIT_SUBMODULE_BRANCH}
    git merge origin/main
    git push
done

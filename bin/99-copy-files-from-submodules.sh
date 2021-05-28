#!/usr/bin/env bash

SCRIPT_VERSION=1.0.0

# Get absolute directory.

GIT_REPOSITORY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# Read metadata.

source ${GIT_REPOSITORY_DIR}/00-submodules.sh

# Backup prior data.

export OUTPUT_DIR=${GIT_REPOSITORY_DIR}/g2/python
mv ${OUTPUT_DIR} ${OUTPUT_DIR}.$(date +%s)
mkdir -p ${OUTPUT_DIR}

# Process each entry.

for SUBMODULE in ${SUBMODULES[@]};
do

    # Get metadata.
    
    IFS=";" read -r -a SUBMODULE_DATA <<< "${SUBMODULE}"
    SUBMODULE_NAME="${SUBMODULE_DATA[0]}"
    SUBMODULE_VERSION="${SUBMODULE_DATA[1]}"
    SUBMODULE_ARTIFACT="${SUBMODULE_DATA[2]}"

    echo "Copy ${SUBMODULE_NAME}/${SUBMODULE_ARTIFACT}"

    # Copy artifact into collection.
    
    cd ${GIT_REPOSITORY_DIR}/${SUBMODULE_NAME}
    cp ${SUBMODULE_ARTIFACT} ${OUTPUT_DIR}/

    
done    

exit

# Tag the current version of the collection.

cd ${GIT_REPOSITORY_DIR}
git add ${OUTPUT_DIR}/*
git commit -a -m "#2 SenzingAPI ${SENZING_VERSION}"
git push
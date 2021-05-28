#!/usr/bin/env bash

SCRIPT_VERSION=1.0.0

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
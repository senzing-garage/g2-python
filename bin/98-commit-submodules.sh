#!/usr/bin/env bash

SCRIPT_VERSION=1.0.0

# Usage / help.

USAGE="Tag a collection version.
Usage:
    $(basename "$0") git-version-comment
Where:
    git-version-comment = A comment for 'git commit'
Version:
    ${SCRIPT_VERSION}
"

# Parse positional input parameters.

GITHUB_COMMENT=$1

# Verify input.

if [[ ( -z ${GITHUB_COMMENT} ) ]]; then
    echo "${USAGE}"
    echo "ERROR: Missing git-version-comment."
    exit 1
fi

# Get absolute directory.

GIT_REPOSITORY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# Backup prior data.

export OUTPUT_DIR=${GIT_REPOSITORY_DIR}/g2/python
mv ${OUTPUT_DIR} ${OUTPUT_DIR}.$(date +%s)
mkdir -p ${OUTPUT_DIR}

# Read metadata.

source ${GIT_REPOSITORY_DIR}/submodules.sh

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
    
    echo "${SUBMODULE_NAME}:${SUBMODULE_VERSION}"
    
    # Get requested version of submodule.
    
    cd ${GIT_REPOSITORY_DIR}/${SUBMODULE_NAME}
    git checkout main
    git pull
    git checkout ${SUBMODULE_VERSION}
done

# Update submodules.

cd ${GIT_REPOSITORY_DIR}
git commit -a -m "#2 Update submodules for ${GITHUB_COMMENT}"
git push
git submodule update --init --recursive

# Copy residual from 

for SUBMODULE in ${SUBMODULES[@]};
do

    # Get metadata.
    
    IFS=";" read -r -a SUBMODULE_DATA <<< "${SUBMODULE}"
    SUBMODULE_NAME="${SUBMODULE_DATA[0]}"
    SUBMODULE_VERSION="${SUBMODULE_DATA[1]}"
    SUBMODULE_ARTIFACT="${SUBMODULE_DATA[2]}"

    echo "Copy ${SUBMODULE_NAME}/${SUBMODULE_ARTIFACT}"

    # Copy artifact into collection.
    
#    cp ${GIT_REPOSITORY_DIR}/${SUBMODULE_NAME}/${SUBMODULE_ARTIFACT} ${OUTPUT_DIR}/
    cd ${GIT_REPOSITORY_DIR}/${SUBMODULE_NAME}
    cp ${SUBMODULE_ARTIFACT} ${OUTPUT_DIR}/

    
done    

exit

# Tag the current version of the collection.

cd ${GIT_REPOSITORY_DIR}
git add ${OUTPUT_DIR}/*
git commit -a -m "#2 ${GITHUB_COMMENT}"
git push
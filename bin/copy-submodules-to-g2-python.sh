#!/usr/bin/env bash

# Establish absolute paths.

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
GIT_REPOSITORY_DIR="$(dirname ${BIN_DIR})"
TARGET_PYTHON_DIR=${GIT_REPOSITORY_DIR}/g2/python

# Read metadata.

SUBMODULES=(
    "compressedfile;CompressedFile.py"
    "dumpstack;DumpStack.py"
    "g2audit;G2Audit.py"
    "g2command;G2Command.py"
    "g2configtables;G2ConfigTables.py"
    "g2configtool;G2ConfigTool.py"
    "g2configtool;G2ConfigTool.readme"
    "g2createproject;G2CreateProject.py"
    "g2database;G2Database.py"
    "g2explorer;G2Explorer.py"
    "g2export;G2Export.py"
    "g2loader;G2Loader.py"
    "g2paths;G2Paths.py"
    "g2project;G2Project.py"
    "g2s3;G2S3.py"
    "g2setupconfig;G2SetupConfig.py"
    "g2snapshot;G2Snapshot.py"
    "g2updateproject;G2UpdateProject.py"
)

# Copy artifacts from submodules to g2/python.

for SUBMODULE in ${SUBMODULES[@]};
do

    # Get metadata.

    IFS=";" read -r -a SUBMODULE_DATA <<< "${SUBMODULE}"
    SUBMODULE_NAME="${SUBMODULE_DATA[0]}"
    SUBMODULE_ARTIFACT="${SUBMODULE_DATA[1]}"

    echo "Copy ${SUBMODULE_NAME}/${SUBMODULE_ARTIFACT}"

    # Copy artifact into collection.

    cd ${GIT_REPOSITORY_DIR}/${SUBMODULE_NAME}
    cp ${SUBMODULE_ARTIFACT} ${TARGET_PYTHON_DIR}/

done

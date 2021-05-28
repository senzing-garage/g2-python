#!/usr/bin/env bash

export SENZING_PYTHON_DIR=/opt/senzing-1.3.0/g2/python
export GIT_ACCOUNT_DIR=~/senzing-g2.git

sudo cp ${SENZING_PYTHON_DIR}/CompressedFile.py ${GIT_ACCOUNT_DIR}/compressedfile
sudo rm ${SENZING_PYTHON_DIR}/CompressedFile.py

sudo cp ${SENZING_PYTHON_DIR}/DumpStack.py ${GIT_ACCOUNT_DIR}/dumpstack
sudo rm ${SENZING_PYTHON_DIR}/DumpStack.py

sudo cp ${SENZING_PYTHON_DIR}/G2AnonModule.py ${GIT_ACCOUNT_DIR}/g2anon
sudo rm ${SENZING_PYTHON_DIR}/G2AnonModule.py

sudo cp ${SENZING_PYTHON_DIR}/G2Audit.py ${GIT_ACCOUNT_DIR}/g2audit
sudo rm ${SENZING_PYTHON_DIR}/G2Audit.py
sudo cp ${SENZING_PYTHON_DIR}/G2AuditModule.py ${GIT_ACCOUNT_DIR}/g2audit
sudo rm ${SENZING_PYTHON_DIR}/G2AuditModule.py

sudo cp ${SENZING_PYTHON_DIR}/G2Command.py ${GIT_ACCOUNT_DIR}/g2command
sudo rm ${SENZING_PYTHON_DIR}/G2Command.py

sudo cp ${SENZING_PYTHON_DIR}/G2ConfigTool.py ${GIT_ACCOUNT_DIR}/g2configtool
sudo rm ${SENZING_PYTHON_DIR}/G2ConfigTool.py
sudo cp ${SENZING_PYTHON_DIR}/G2ConfigTool.readme ${GIT_ACCOUNT_DIR}/g2configtool
sudo rm ${SENZING_PYTHON_DIR}/G2ConfigTool.readme

sudo cp ${SENZING_PYTHON_DIR}/G2Config.py ${GIT_ACCOUNT_DIR}/g2config
sudo rm ${SENZING_PYTHON_DIR}/G2Config.py
sudo cp ${SENZING_PYTHON_DIR}/G2ConfigModule.py ${GIT_ACCOUNT_DIR}/g2config
sudo rm ${SENZING_PYTHON_DIR}/G2ConfigModule.py

sudo cp ${SENZING_PYTHON_DIR}/G2ConfigTables.py ${GIT_ACCOUNT_DIR}/g2configtables
sudo rm ${SENZING_PYTHON_DIR}/G2ConfigTables.py

sudo cp ${SENZING_PYTHON_DIR}/G2Database.py ${GIT_ACCOUNT_DIR}/g2database
sudo rm ${SENZING_PYTHON_DIR}/G2Database.py

sudo cp ${SENZING_PYTHON_DIR}/G2Engine.py ${GIT_ACCOUNT_DIR}/g2engine
sudo rm ${SENZING_PYTHON_DIR}/G2Engine.py

sudo cp ${SENZING_PYTHON_DIR}/G2Exception.py ${GIT_ACCOUNT_DIR}/g2exception
sudo rm ${SENZING_PYTHON_DIR}/G2Exception.py

sudo cp ${SENZING_PYTHON_DIR}/G2Export.py ${GIT_ACCOUNT_DIR}/g2export
sudo rm ${SENZING_PYTHON_DIR}/G2Export.py

sudo cp ${SENZING_PYTHON_DIR}/G2Loader.py ${GIT_ACCOUNT_DIR}/g2loader
sudo rm ${SENZING_PYTHON_DIR}/G2Loader.py

sudo cp ${SENZING_PYTHON_DIR}/G2Module.ini ${GIT_ACCOUNT_DIR}/g2module
sudo rm ${SENZING_PYTHON_DIR}/G2Module.ini
sudo cp ${SENZING_PYTHON_DIR}/G2Module.py ${GIT_ACCOUNT_DIR}/g2module
sudo rm ${SENZING_PYTHON_DIR}/G2Module.py

sudo cp ${SENZING_PYTHON_DIR}/G2Product.py ${GIT_ACCOUNT_DIR}/g2product
sudo rm ${SENZING_PYTHON_DIR}/G2Product.py
sudo cp ${SENZING_PYTHON_DIR}/G2ProductModule.py ${GIT_ACCOUNT_DIR}/g2product
sudo rm ${SENZING_PYTHON_DIR}/G2ProductModule.py

sudo cp ${SENZING_PYTHON_DIR}/G2Project.ini ${GIT_ACCOUNT_DIR}/g2project
sudo rm ${SENZING_PYTHON_DIR}/G2Project.ini
sudo cp ${SENZING_PYTHON_DIR}/G2Project.py ${GIT_ACCOUNT_DIR}/g2project
sudo rm ${SENZING_PYTHON_DIR}/G2Project.py

sudo cp ${SENZING_PYTHON_DIR}/G2Report.py ${GIT_ACCOUNT_DIR}/g2report
sudo rm ${SENZING_PYTHON_DIR}/G2Report.py

sudo cp ${SENZING_PYTHON_DIR}/G2S3.py ${GIT_ACCOUNT_DIR}/g2s3
sudo rm ${SENZING_PYTHON_DIR}/G2S3.py

sudo cp ${SENZING_PYTHON_DIR}/G2Service.py ${GIT_ACCOUNT_DIR}/g2service
sudo rm ${SENZING_PYTHON_DIR}/G2Service.py

sudo cp ${SENZING_PYTHON_DIR}/G2VCompare.py ${GIT_ACCOUNT_DIR}/g2vcompare
sudo rm ${SENZING_PYTHON_DIR}/G2VCompare.py

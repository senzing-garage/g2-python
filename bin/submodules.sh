#!/usr/bin/env bash

SENZING_VERSION=1.3.0

# Format: repository;version;artifact

SUBMODULES=(
    "compressedfile;1.0.0;CompressedFile.py"
    "dumpstack;1.0.0;DumpStack.py"
    "g2anon;1.1.0;G2AnonModule.py"
    "g2audit;1.1.0;G2AuditModule.py"
    "g2command;1.2.0;G2Command.py"
    "g2config;1.1.0;G2ConfigModule.py"
    "g2configtables;1.0.0;G2ConfigTables.py"
    "g2configtool;1.0.0;G2ConfigTool.py"
    "g2configtool;1.0.0;G2ConfigTool.readme"
    "g2database;1.1.0;G2Database.py"
    "g2exception;1.0.0;G2Exception.py"
    "g2export;1.1.0;G2Export.py"
    "g2loader;1.2.0;G2Loader.py"
    "g2module;1.2.0;G2Module.ini"
    "g2module;1.2.0;G2Module.py"
    "g2project;1.2.0;G2Project.ini"
    "g2project;1.2.0;G2Project.py"
)

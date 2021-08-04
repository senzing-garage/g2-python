# g2-python

## Synopsis

The aggregation of repositories that comprise `g2/python`.

## Overview

The aggregated files look like this:

```console
$ tree g2/python
/opt/my-senzing/g2/python
├── CompressedFile.py
├── demo
│   ├── sample
│   │   ├── project.csv
│   │   ├── project.json
│   │   ├── sample_company.csv
│   │   ├── sample_company.json
│   │   ├── sample_person.csv
│   │   └── sample_person.json
│   └── truth
│       ├── project.csv
│       ├── project.json
│       ├── truthset-person-v1-set1-data.csv
│       ├── truthset-person-v1-set1-key.csv
│       ├── truthset-person-v1-set1.sh
│       ├── truthset-person-v1-set2-data.csv
│       ├── truthset-person-v1-set2-key.csv
│       └── truthset-person-v1-set2.sh
├── DumpStack.py
├── G2Audit.py
├── G2Command.py
├── G2ConfigMgr.py
├── G2Config.py
├── G2ConfigTables.py
├── G2ConfigTool.py
├── G2ConfigTool.readme
├── G2CreateProject.py
├── G2Database.py
├── G2Diagnostic.py
├── G2Engine.py
├── G2Exception.py
├── G2Explorer.py
├── G2Export.py
├── G2Hasher.py
├── G2Health.py
├── G2IniParams.py
├── G2Loader.py
├── G2Paths.py
├── G2Product.py
├── G2Project.py
├── g2purge.umf
├── G2S3.py
├── G2SetupConfig.py
├── G2Snapshot.py
├── G2UpdateProject.py
└── governor_postgres_xid.py
```

These files are packaged in `senzingapi-M.m.P-00000.x86_64.rpm` and `senzingapi-M.m.P-00000.x86_64.deb`.

### Contents

1. [Preamble](#preamble)
    1. [Legend](#legend)
1. [Related artifacts](#related-artifacts)
1. [Expectations](#expectations)
1. [Demonstrate using Command Line Interface](#demonstrate-using-command-line-interface)
    1. [Prerequisites for CLI](#prerequisites-for-cli)
    1. [Download](#download)
    1. [Environment variables for CLI](#environment-variables-for-cli)
    1. [Run command](#run-command)
1. [Demonstrate using Docker](#demonstrate-using-docker)
    1. [Prerequisites for Docker](#prerequisites-for-docker)
    1. [Docker volumes](#docker-volumes)
    1. [Docker network](#docker-network)
    1. [Docker user](#docker-user)
    1. [Database support](#database-support)
    1. [External database](#external-database)
    1. [Run Docker container](#run-docker-container)
1. [Directives](#directives)
1. [Develop](#develop)
    1. [Prerequisites for development](#prerequisites-for-development)
    1. [Clone repository](#clone-repository)
    1. [Build Docker image](#build-docker-image)
1. [Examples](#examples)
    1. [Examples of CLI](#examples-of-cli)
    1. [Examples of Docker](#examples-of-docker)
1. [Advanced](#advanced)
    1. [Configuration](#configuration)
1. [Errors](#errors)
1. [References](#references)

## Preamble

At [Senzing](http://senzing.com),
we strive to create GitHub documentation in a
"[don't make me think](https://github.com/Senzing/knowledge-base/blob/master/WHATIS/dont-make-me-think.md)" style.
For the most part, instructions are copy and paste.
Whenever thinking is needed, it's marked with a "thinking" icon :thinking:.
Whenever customization is needed, it's marked with a "pencil" icon :pencil2:.
If the instructions are not clear, please let us know by opening a new
[Documentation issue](https://github.com/Senzing/template-python/issues/new?template=documentation_request.md)
describing where we can improve.   Now on with the show...

### Legend

1. :thinking: - A "thinker" icon means that a little extra thinking may be required.
   Perhaps there are some choices to be made.
   Perhaps it's an optional step.
1. :pencil2: - A "pencil" icon means that the instructions may need modification before performing.
1. :warning: - A "warning" icon means that something tricky is happening, so pay attention.

## Develop

The following instructions are used when modifying and building the Docker image.

### Prerequisites for development

:thinking: The following tasks need to be complete before proceeding.
These are "one-time tasks" which may already have been completed.

1. The following software programs need to be installed:
    1. [git](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-git.md)

### Clone repository

For more information on environment variables,
see [Environment Variables](https://github.com/Senzing/knowledge-base/blob/master/lists/environment-variables.md).

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=g2-python
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

1. Using the environment variables values just set, follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/clone-repository.md) to install the Git repository.

### Create branch

1. :pencil2: Using [github.com/Senzing/g2-python](https://github.com/Senzing/g2-python), create a branch.
   Then, identify the name of the branch created.
   Example:

    ```console
    export GIT_BRANCH=my-test-branch
    ```

1. Checkout branch.
   Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    git checkout main
    git pull
    git checkout ${GIT_BRANCH}
    ```

### Update files from submodules

1. :pencil2: Set `GITHUB_ACCESS_TOKEN`.
   This is needed to access GitHub above the "public" limit.
   For information on how to obtain an access token, see
   [Creating a personal access token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token).

    ```console
    export GITHUB_ACCESS_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    ```

1. Update submodules and copy files from submodules.
   Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}/bin
    ./update-submodules.sh
    ```

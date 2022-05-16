# g2-python

## Synopsis

The aggregation of repositories that comprise `g2/python` in the Senzing SDK API.

## Overview

The aggregated files look like this:

```console
$ tree g2/python

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
├── G2IniParams.py
├── G2Loader.py
├── G2Paths.py
├── G2Product.py
├── G2Project.py
├── G2S3.py
├── G2SetupConfig.py
├── G2Snapshot.py
├── G2UpdateProject.py
└── governor_postgres_xid.py
```

These files are packaged in `senzingapi-M.m.P-00000.x86_64.rpm` and `senzingapi_M.m.P-00000_amd64.deb`

### Contents

1. [Preamble](#preamble)
    1. [Legend](#legend)
1. [Tips](#tips)
    1. [View a specific release of g2/python](#view-a-specific-release-of-g2python)
    1. [Compare releases](#compare-releases)
1. [Develop](#develop)
    1. [Prerequisites for development](#prerequisites-for-development)
    1. [Clone repository](#clone-repository)
    1. [Create branch](#create-branch)
    1. [Update files from submodules](#update-files-from-submodules)
    1. [Update CHANGELOG.md](#update-changelog.md)
    1. [Verify changes](#verify-changes)
    1. [Pull branch into main](#pull-branch-into-main)
    1. [Create versioned release](#crate-versioned-release)

## Preamble

At [Senzing](http://senzing.com),
we strive to create GitHub documentation in a
"[don't make me think](https://github.com/Senzing/knowledge-base/blob/main/WHATIS/dont-make-me-think.md)" style.
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

## Tips

### View a specific release of g2/python

1. Visit [github.com/Senzing/g2-python](https://github.com/Senzing/g2-python)
1. In upper-left dropdown, probably set to "main", select the dropdown and choose the "Tags" tab.
1. Choose the Senzing SDK API version of interest.
1. The new URL will look like [https://github.com/Senzing/g2-python/tree/2.8.0](https://github.com/Senzing/g2-python/tree/2.8.0),
   where `2.8.0` is the version of Senzing SDK API.
1. In addition to the `g2/python` directory being at the specified Senzing SDK API version,
   the GitHub submodule references will also be at that version.

### Compare releases

1. :pencil2: To compare the differences between Senzing versions, use a URL like the following:
   [https://github.com/Senzing/g2-python/compare/2.7.0...2.8.0](https://github.com/Senzing/g2-python/compare/2.7.0...2.8.0)
   Where:
    1. `2.7.0` can be replaced with the earliest release in the comparison.
    1. `2.8.0` can be replaced with the latest release  in the comparison.
    1. `main` can be used to replace `2.7.0` or `2.8.0` to indicate current head of main branch.

## Develop

The following instructions are used when modifying and building the Docker image.

### Prerequisites for development

:thinking: The following tasks need to be complete before proceeding.
These are "one-time tasks" which may already have been completed.

1. The following software programs need to be installed:
    1. [git](https://github.com/Senzing/knowledge-base/blob/main/HOWTO/install-git.md)

### Clone repository

For more information on environment variables,
see [Environment Variables](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md).

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=g2-python
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

1. Using the environment variables values just set, follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/main/HOWTO/clone-repository.md) to install the Git repository.

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

### Update CHANGELOG.md

1. Update `CHANGELOG.md` to reflect the new version of Senzing.

### Verify changes

1. If a new version of Senzing is installed into `/opt/senzing/g2`, then files can be compared.
   Example:

    ```console
    diff -r /opt/senzing/g2/python ${GIT_REPOSITORY_DIR}/g2/python
    ```

1. Opening, but not creating, pull request can be made for the `GIT_BRANCH` branch to determine
   if the changes seen are the expected.

### Pull branch into main

1. Follow a standard process for pulling into main branch.
   Example:
    1. Create a Pull Request for the `GIT_BRANCH` branch.
    1. Have Pull Request approved.
    1. Merge Pull request into "main" branch.

### Create versioned release

1. [Create a new versioned release](https://github.com/Senzing/g2-python/releases) that matches the `senzingapi` release.

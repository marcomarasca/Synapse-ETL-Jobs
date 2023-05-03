# Synapse-ETL-Jobs
The Synapse ETL jobs project maintains all the Python-based Glue job scripts respoonsible for moving 
Synapse Warehouse data to S3 where it can be queried by Athena. 
The scripts are deployed to S3, from where the Synapse Stack Builder can include them in a Glue
CloudFormation stack it creates.
For more information please see the [design documentation](https://sagebionetworks.jira.com/wiki/spaces/DW/pages/2732916846/Processing+Access+Records+using+AWS+High+Level+Design).

# Repository structure
This repository only contains the Python scripts which are used by Glue jobs.

### `src/`
This directory contains job scripts.

### `tests/`
This directory contains unit tests.

### `.github/`
This directory contains the scripts for our GitHub Actions.

# Local environment setup
The project uses AWS Glue libraries and Python, and requires Docker.

To install Python on MacOS use the command below, which requires Python version 3 to be installed. 
PIP (PIP is a package manager for Python packages, or modules ) is included in python version 3.4 or later.

```
brew install python
```

The following steps build and run a Docker container.

Build image

```
docker build -t aws-glue .
```

To run tests use the following command. It will execute the test files, whose names start with test.

```
docker run -v ~/.aws:/home/glue_user/.aws -v  ${GITHUB_WORKSPACE}:/home/glue_user/workspace/  -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080  aws-glue -c "python3 -m pytest"
```

# GitHub Actions

1. python-package.yml : This action sets up python, installs dependencies and checks syntax errors or undefined names. 
Also, it builds the docker image and run the tests.

2. tag_release.yml : A GitHub action to automatically bump and tag develop, on merge, with the latest 
SemVer formatted (Major.Minor.Patch) version. If no version is provided, the `Minor` field is bumped.
The desired bump can be specified in the commit message, e.g. with `#major`, `#minor`, or `#patch`.  For more information,
please see the [documentation](https://github.com/anothrNick/github-tag-action).

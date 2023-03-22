# Synapse-ETL-Jobs
Synapse ETL jobs project will be maintaining all the glue job scripts.All the data which is currently processed by 
Synapse-Warehouse-Workers and stored in RDS should be moved to Synapse-ETL-Jobs and stored in s3 which will be queried
by athena. Synapse-ETL-Jobs project is maintaining the python scripts which will be executed by glue jobs.Once the 
python script created in Synapse-ETL-Jobs project, the cloud formation of glue job and related resources should be done
in Synapse-Stack-Builder project.For more information please see the[design documentation](https://sagebionetworks.jira.com/wiki/spaces/DW/pages/2732916846/Processing+Access+Records+using+AWS+High+Level+Design)

# Repo structure
This repository only contains the python scripts which is used by glue job.

### `src/`
The `src` directory contains job scripts

### `tests/`
This directory contains unit tests

### `.github/`
This directory contains the scripts for our GitHub Actions.

# Local environment setup
The project is using aws-glue libraries and python.To run the test locally we need docker image.

To install python on Mac use below command.Python version 3 should be installed. 
PIP (PIP is a package manager for Python packages, or modules )is included in python version 3.4 or later.

```
brew install python
```

To run the test we need docker image so we can run test in it.

Build image
```
docker build -t aws-glue .
```
To run tests use below command
```
docker run -v ~/.aws:/home/glue_user/.aws -v  ${GITHUB_WORKSPACE}:/home/glue_user/workspace/  -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080  aws-glue -c "python3 -m pytest"
```

To run specific test file use --name.
```
docker run -v ~/.aws:/home/glue_user/.aws -v  ${GITHUB_WORKSPACE}:/home/glue_user/workspace/  -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name filename.py aws-glue -c "python3 -m pytest"
```

# GitHub Actions
1. python-package.yml : This action setup python,install dependencies and check syntax errors or undefined names. 
Also, it builds the docker image and run tests.
2. tag_release.yml : A GitHub action to automatically bump and tag develop, on merge, with the latest 
SemVer formatted(Major.Minor.Path) version. Minor type of bump is used when none explicitly provided eg(0.1.0).
If we want to bump specific like major,minor,path or combination then use #major in commit message for major.
please see the [documentation](https://github.com/anothrNick/github-tag-action)

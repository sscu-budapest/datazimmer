# datazimmer

[![Documentation Status](https://readthedocs.org/projects/datazimmer/badge/?version=latest)](https://datazimmer.readthedocs.io/en/latest)
[![codeclimate](https://img.shields.io/codeclimate/maintainability/sscu-budapest/datazimmer.svg)](https://codeclimate.com/github/sscu-budapest/datazimmer)
[![codecov](https://img.shields.io/codecov/c/github/sscu-budapest/datazimmer)](https://codecov.io/gh/sscu-budapest/datazimmer)
[![pypi](https://img.shields.io/pypi/v/datazimmer.svg)](https://pypi.org/project/datazimmer/)
[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.7499121.svg)](https://doi.org/10.5281/zenodo.7499121)


Some utility function to help with

- setting up data environments
- simplified dvc pipeline registry

these are used in the [project-template](https://github.com/sscu-budapest/project-template)

Make sure that `python` points to `python>=3.8` and you have `pip` and `git`

### To create a new project

- run `dz init project-name`
- create, register and document steps in a pipeline you will run in different [environments](TODO)
- build metadata to exportable and serialized format with `dz build-meta`
  - if you defined importable data from other artifacts in the config, you can import them with `load-external-data` 
  - ensure that you import envs that are served from sources you have access to
- build and run pipeline steps by running `dz run`
- validate that the data matches the [datascript](TODO) description with `dz validate`

## Test projects

TODO: document dogshow and everything else much better here


## Functions

### Tinker

> check out a table or few, with a notebook and some basic analysis to help

### Engineer Research

## Scheduling

- a project as a whole has a cron expression to determine the schedule of reruns
- additionally, aswan projects within the dz project can have different cron expressions for scheduling new runs of the aswan projects

## Lookahead

- overlapping names convention
- resolve naming confusion with colassigner, colaccessor and table feature / composite type / index base classes
- abstract composite type + subclass of entity class
  - import ACT, inherit from it and specify 
  - importing composite type is impossible now if it contains foreign key :(
- automatic filter for env creation based on foreign key metadata
- add option to infer data type of assigned feature
  - can be problematic b/c pandas int/float/nan issue
- sharing functions among projects
  - functions specific to processing certain composite / named types
  - e.g. function dealing with fitting into a limit in dogshow project 1
- create similar sets of features in a dry way
- detecting reliance of composite type given by assigner
  - can wait, as initial import is just the assigner transformed to accessor
- overlapping in entities
  - detect / signal the same type of entity
- properly assert importing

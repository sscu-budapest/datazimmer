# datazimmer

[![Documentation Status](https://readthedocs.org/projects/datazimmer/badge/?version=latest)](https://datazimmer.readthedocs.io/en/latest)
[![codeclimate](https://img.shields.io/codeclimate/maintainability/sscu-budapest/datazimmer.svg)](https://codeclimate.com/github/sscu-budapest/datazimmer)
[![codecov](https://img.shields.io/codecov/c/github/sscu-budapest/datazimmer)](https://codecov.io/gh/sscu-budapest/datazimmer)
[![pypi](https://img.shields.io/pypi/v/datazimmer.svg)](https://pypi.org/project/datazimmer/)

Some utility function to help with

- setting up data environments with invoke
- simplified dvc pipeline registry

these are used in the [artifact-template](https://github.com/sscu-budapest/project-template)

Make sure that `python` points to `python>=3.8` and you have `pip` and `git`

## Functions

### Tinker

> check out a table or few, with a notebook and some basic analysis to help

### Engineer Research


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

# sscutils

[![Documentation Status](https://readthedocs.org/projects/sscutils/badge/?version=latest)](https://sscutils.readthedocs.io/en/latest)
[![codeclimate](https://img.shields.io/codeclimate/maintainability/sscu-budapest/sscutils.svg)](https://codeclimate.com/github/sscu-budapest/sscutils)
[![codecov](https://img.shields.io/codecov/c/github/sscu-budapest/sscutils)](https://codecov.io/gh/sscu-budapest/sscutils)
[![pypi](https://img.shields.io/pypi/v/sscutils.svg)](https://pypi.org/project/sscutils/)

Some utility function to help with

- setting up data subsets with invoke
- simplified dvc pipeline registry

these are used in [dataset-template](https://github.com/sscu-budapest/dataset-template) and [research-project-template](https://github.com/sscu-budapest/project-template)

## Lookahead

- resolve naming confusion with colassigner, colaccessor and table feature / composite type / index base classes
- abstract composite type + subclass of entity class
  - import ACT, inherit from it and specify 
  - importing composite type is impossible now if it contains foreign key :(
- automatic filter for env creation based on foreign key metadata
- add option to infer data type of assigned feature
  - can be problematic b/c pandas int/float/nan issue
- metadata created dry, dynamically, but imported static, wet
- sharing functions among projects
  - functions specific to processing certain composite / named types
  - e.g. function dealing with fitting into a limit in dogshow project 1
- detecting reliance of composite type given by assigner
  - can wait, as initial import is just the assigner transformed to accessor
- overlapping in entities
  - detect / signal the same type of entity

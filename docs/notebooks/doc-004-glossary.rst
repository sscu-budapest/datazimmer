Glossary
========

Namespace
^^^^^^^^^

   Atomic unit of the knowledge system containing data and metadata

-  defines (optionally)

   -  tables
   -  composite types
   -  entity classes

-  can import other namespaces
-  a dataset or the output of a step in a project
-  has different environments

Dataset
^^^^^^^

   A set of defined tables in a namespace with metadata and different
   environments

-  represented in one git repository
-  one namespace

Project
^^^^^^^

   A pipeline built on datasets

-  represented in one git repository
-  steps of the pipeline create can namespaces

Metadata
^^^^^^^^

   Information describing the knowledge in a namespace

-  defined tables, composite types and entity classes

Artifact Metadata
^^^^^^^^^^^^^^^^^

-  imported namespaces, with prefix
-  metadata for all namespaces

Config
^^^^^^

   Parameters that can change from run to run

-  for a dataset

   -  the environments to create
   -  remotes to upload them to

-  for a project

   -  the environments of the imported namespaces to use
   -  parameters of the steps in the pipeline

Environment
^^^^^^^^^^^

   A subset or a scrambled version of a set of data tables

-  changes by branch of a project
-  many on one branch of dataset, created based on config and script
-  defined by the environments of the sources for a project step

Feature
^^^^^^^

   A named set of columns in a table

-  can be primitive feature, foreign key or composite feature

Subject of Records
^^^^^^^^^^^^^^^^^^

   Entity class that is represented in a table

Step
^^^^

   An element of the pipeline, collected in topmodules for a project and
   executed as one function with explicitly dtated outputs and
   dependencies

-  is logged in dvc

Topmodule
^^^^^^^^^

   Python module that is a direct child of the root src module

Child Module
^^^^^^^^^^^^

   Module that is nested under a topmodule

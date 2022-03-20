Glossary
========

Namespace
~~~~~~~~~

   The atomic unit of the knowledge system containing data and metadata

-  defines

   -  tables
   -  composite types
   -  entity classes
   -  code to build data based on these

-  represented by

   -  a module in a data artifact (as datascript) - nested right below
      the main (src) module
   -  a set of YAML files in ``{namespace name}/**.yaml`` as serialized
      metadata in the released sdist

      -  automatically generated from the code

   -  an exported .py file with basic datascript in
      ``{namespace name}/__init__.py`` in the released sdist

-  can import other namespaces, either to use

   -  data (even for foreign keys in tables)
   -  defined composite types / entity classes

Data Artifact
~~~~~~~~~~~~~

   A versioned set of interconnected namespaces with metadata and
   different environments

-  defines

   -  namespaces
   -  different environments where (usually) the same code runs for
      different data

-  represented by

   -  a git repository

      -  is a DVC repository
      -  based on a `template <TODO>`__
      -  has fixed form tags representing the releases and data versions

Registry
~~~~~~~~

   A repository containing data about the releases and dependencies of
   artifacts to make importing namespaces straightforward

-  represented by

   -  a git repository (either local or remote)
   -  write access needed to the repo to release to it

-  contains data about

   -  (named) artifacts

      -  URI
      -  versions
      -  environment->dvc remote mapping

-  contains sdist forms of metadata of artifacts release there

   -  to set up a special PyPI index so that installation and dependency
      resolution is outsourced

Metadata
~~~~~~~~

   Information about the data contained in artifacts

-  defines

   -  for each namespace

      -  tables
      -  composite types
      -  entity classes

-  represented

   -  in an artifact repository

      -  defined in code (datascript object)

         -  scrutable
         -  entitybase
         -  compositetypebase

      -  serialized (generated from code)

         -  YAML files

   -  in runtime

      -  converted as soon as possible to dataclasses in bedrock module

   -  some even in data output in parquet

Config
~~~~~~

-  defines

   -  name
   -  version (this is the metadata version, the data version is
      determined at release)
   -  default-environment name (the first environment in envs config by
      default)
   -  validation-environments (the default-environment by default)
   -  registry address (the SSCUB registry by default) TODO: link
   -  imported_artifacts

      -  either a list of artifact names to be imported, where other
         than name, all default values are used
      -  or a dictionary, where the key is the artifact name (in the
         registry), and values are:

         -  version (metadata version)
         -  data_namespaces - the namespaces where loading the data is
            required

   -  in ``envs`` for each environment (one empty env named complete by
      default)

      -  params for all local namespaces and global params (namespace
         params default to these if not defined)

         -  logged to DVC from here

      -  environments of imported artifacts (where data is needed)
      -  specific DVC remote

         -  where to push data generated as outputs of running the code
            from namespaces (TODO - find a proper name, e.g. namespace
            processor) - identified by the name of a remote defined in
            DVC config

      -  parent env (default-environment by default)

         -  all missing keys of parameters or imported ns

-  represented as ``zimmer.yaml`` in artifact root

Environment
~~~~~~~~~~~

   A complete run of the code in an artifact with its values for
   parameters and environments for imported data

-  defined by config
-  …

Tabular data related phrases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  feature: *A named set of columns in a table*

   -  can be a primitive feature, foreign key or composite feature

-  the subject of records: entity class that is represented in a table

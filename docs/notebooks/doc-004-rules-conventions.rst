Naming Conventions and Restrictions
===================================

-  project names:

   -  lower case letters and non-duplicated dashes (-) not at either end

-  environment and namespace names:

   -  lower case letters and non-duplicated underscores (_) not at
      either end

Metadata
~~~~~~~~

-  ``[a-b]*_table`` table names based on singular form of entity
   e.g. ``dog_table``
-  see all in dogshow standard

Rules
=====

Notes of rules that are necessary for operation. Too strict or stupid
rules need to change!

-  Only one registered function per namespace

   -  unless for separate environments, like with the helper function of
      data loaders and environment creation

-  usually, an env run of a namespace processor (is it called this?
   TODO) reads and writes to one env, the one it corresponds to, but:

   -  can only write to its env
   -  can read from a different one, but then can only read from that
      one
   -  this allows for registering a function that creates its
      environment from a base/complete set

-  TableNameFeatures should be the name of table feature classes if the
   table name is to be inferred
-  No composite features with the same prefix in the same table
-  Feature name can’t contain \_\_ (dunder)

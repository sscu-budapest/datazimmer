Rules
=====

Notes of rules that are necessary for operation. Too strict or stupid
rules need to change!

-  IndexBase class and ScruTable need to be defined in the same file for
   Foreign keys to be found
-  Don’t import ScruTable directly as a dependecy only import the module
   where it can be found
-  Only one step function per top module
-  TableNameFeatures should be the name of table feature classes if
   table name is to be inferred
-  No composite features with same prefix in same table
-  Feature name cant contain \_\_

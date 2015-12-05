EmptyHeaded Query Compiler
----------------------------

Table of Contents
-----------------

  * [Usages](#usage)
  * [Overview](#overview)
  * [Query Plans](#installing-from-source)

Usage
-----------------

You can build the jar by running `sbt pack` from the query_compiler directory. You can then run the query compiler by running:

```
./target/pack/bin/query-compiler --explain -c config/config.json -f -q query_file.datalog
```
This would run query compiler in --explain mode (i.e., no code is generated; the query plan will be output to stdout), with the specified config (which specified what relations exist on disk), with the query contained in the file query_file.datalog.

If you run without the `--explain` option,

You can get a more complete explanation of all the options by running with `--help`.

Implementation Overview
------------------------

Query Plans
----------------

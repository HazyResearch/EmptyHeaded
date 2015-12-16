#!/bin/bash
cd $EMPTYHEADED_HOME/storage_engine
make clean
cd $EMPTYHEADED_HOME/query_compiler
sbt clean
#!/bin/bash
#setup env variables
export EMPTYHEADED_HOME=`pwd`
export PYTHONPATH=$EMPTYHEADED_HOME/runtime

#compiles the query compiler
cd $EMPTYHEADED_HOME/query_compiler
sbt assembly
cd $EMPTYHEADED_HOME

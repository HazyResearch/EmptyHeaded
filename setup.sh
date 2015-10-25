#!/bin/bash
#set env variables
export EMPTYHEADED_HOME=`pwd`
export PYTHONPATH=$EMPTYHEADED_HOME/runtime
if [[ "$OSTYPE" == "linux-gnu" ]]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$EMPTYHEADED_HOME/libs
elif [[ "$OSTYPE" == "darwin"* ]]; then
	export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$EMPTYHEADED_HOME/libs
else
	echo "OS NOT SUPPORTED"
fi
#compiles the query compiler
cd $EMPTYHEADED_HOME/query_compiler
sbt start-script
cd $EMPTYHEADED_HOME
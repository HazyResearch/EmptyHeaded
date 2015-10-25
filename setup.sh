#!/bin/bash
export EMPTYHEADED_HOME=`pwd`
cd $EMPTYHEADED_HOME/storage_engine
make clean && make emptyheaded
if [[ "$OSTYPE" == "linux-gnu" ]]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$EMPTYHEADED_HOME/libs
elif [[ "$OSTYPE" == "darwin"* ]]; then
	export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$EMPTYHEADED_HOME/libs
else
	echo "OS NOT SUPPORTED"
fi
cd $EMPTYHEADED_HOME/query_compiler
sbt start-script
cd $EMPTYHEADED_HOME
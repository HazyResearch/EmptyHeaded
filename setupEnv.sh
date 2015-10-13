#!/bin/bash
export EMPTYHEADED_HOME=`pwd`
if [[ "$OSTYPE" == "linux-gnu" ]]; then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$EMPTYHEADED_HOME/libs
elif [[ "$OSTYPE" == "darwin"* ]]; then
	export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$EMPTYHEADED_HOME/libs
else
	echo "OS NOT SUPPORTED"
fi

set e
cd $EMPTYHEADED_HOME/query_compiler && sbt pack && cd -

#this is static
cd $EMPTYHEADED_HOME/cython/db && ./build.sh && mv *.so $EMPTYHEADED_HOME/python/ && cd -

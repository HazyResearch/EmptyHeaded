set -e
source env.sh
cd dependencies && ./install.sh && cd -
source compile.sh

$EMPTYHEADED_HOME/storage_engine/test/run_tests.sh

#cd $EMPTYHEADED_HOME/query_compiler && sbt test && cd -
#python test/graph/travis.py
#python test/rdf/travis.py

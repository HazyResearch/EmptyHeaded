set -e
source env.sh
cd dependencies && ./install.sh && cd -
source compile.sh

cd $EMPTYHEADED_HOME/query_compiler && sbt test && cd -
python test/graph/travis.py

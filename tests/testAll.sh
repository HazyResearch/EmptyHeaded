set -e
source setup.sh
python tests/examples/triangle_materialized.py
cd query_compiler
sbt test
cd ..

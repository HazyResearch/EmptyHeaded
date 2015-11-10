set -e
source setup.sh

#whole system tests
python tests/examples/createDB.py
python tests/examples/createPrunedDB.py
python tests/examples/triangle_materialized.py
python tests/examples/lollipop_agg.py
python tests/examples/barbell_agg.py

#query compiler tests
cd query_compiler
sbt test
cd ..

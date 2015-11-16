set -e
source setup.sh

#whole system tests
python tests/examples/createPrunedFBDB.py
python tests/examples/triangle_materialized.py
python tests/examples/4clique_agg.py
#python tests/examples/4clique.py

python tests/examples/createDuplicatedFBDB.py
python tests/examples/lollipop_agg.py
python tests/examples/barbell_agg.py

python tests/examples/createSimpleDB.py
python tests/examples/lollipop_materialized.py
python tests/examples/barbell_materialized.py

#query compiler tests
cd query_compiler
sbt test
cd ..

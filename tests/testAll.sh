set -e
source setup.sh

#whole system tests
python tests/examples/graph/createPrunedFBDB.py
python tests/examples/graph/triangle_materialized.py
python tests/examples/graph/4clique_agg.py
#python tests/examples/graph/4clique.py

python tests/examples/graph/createDuplicatedFBDB.py
python tests/examples/graph/lollipop_agg.py
python tests/examples/graph/barbell_agg.py

python tests/examples/graph/createSimpleDB.py
python tests/examples/graph/lollipop_materialized.py
python tests/examples/graph/barbell_materialized.py

python tests/examples/fft/createFFTDB.py
python tests/examples/fft/readFFT.py

#query compiler tests
cd query_compiler
sbt test
cd ..

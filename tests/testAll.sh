set -e
source setup.sh

#whole system tests
python tests/graph/createPrunedFBDB.py
python tests/graph/triangle_materialized.py
python tests/graph/4clique_agg.py
#python tests/graph/4clique.py

python tests/graph/createDuplicatedFBDB.py
python tests/graph/lollipop_agg.py
python tests/graph/barbell_agg.py

python tests/graph/createSimpleDB.py
python tests/graph/lollipop_materialized.py
python tests/graph/barbell_materialized.py

python tests/graph/pagerank.py
python tests/graph/sssp.py

python tests/fft/createFFTDB.py
python tests/fft/readFFT.py

python tests/rdf/createLUBM1.py
python tests/rdf/lubm1.py
python tests/rdf/lubm2.py
python tests/rdf/lubm4.py
python tests/rdf/lubm6.py
python tests/rdf/lubm7.py
python tests/rdf/lubm8.py
python tests/rdf/lubm12.py

#query compiler tests
cd query_compiler
sbt test
cd ..

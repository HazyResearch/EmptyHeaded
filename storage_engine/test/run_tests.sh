mkdir -p $EMPTYHEADED_HOME/storage_engine/build
cd $EMPTYHEADED_HOME/storage_engine/build
cmake .. -DNUM_THREADS=4
make

./bin/testVector
python setup.py build_ext --inplace --quiet --rpath $EMPTYHEADED_HOME/libs $1 
mv *.so $EMPTYHEADED_HOME/runtime/queries/$1.so
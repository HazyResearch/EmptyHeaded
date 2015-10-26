python setup.py build_ext --inplace --quiet --rpath $EMPTYHEADED_HOME/libs $1 
mv *.so ../queries/$1.so
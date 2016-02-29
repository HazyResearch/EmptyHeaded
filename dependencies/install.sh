tar -xvf JPype1-0.6.1.tar.gz
cd JPype1-0.6.1
#mkdir -p $EMPTYHEADED_HOME/dependencies/JPype/lib/python2.7/site-packages
#export PYTHONPATH=$PYTHONPATH:$EMPTYHEADED_HOME/dependencies/JPype/lib/python2.7/site-packages
#python setup.py install --prefix=$EMPTYHEADED_HOME/dependencies/JPype --user 
python setup.py install --user
cd -

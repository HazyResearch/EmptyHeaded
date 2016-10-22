mkdir $1
cp -rf db/* $1
export EMPTYHEADED_DB_PATH=$1

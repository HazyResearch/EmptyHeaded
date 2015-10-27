#include "Python.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Query_loadDB.hpp"

static PyObject * run_loadDB(PyObject * self, PyObject * args);
static PyObject * num_rows_loadDB(PyObject * self, PyObject * args);
static PyObject * fetch_data_loadDB(PyObject * self, PyObject * args);

static PyMethodDef EmptyHeadedQueryMethods[] = {
  { "run_loadDB", run_loadDB, METH_VARARGS, "" },
  { "num_rows_loadDB", num_rows_loadDB, METH_VARARGS, "Number of rows in result." },
  { "fetch_data_loadDB", fetch_data_loadDB, METH_VARARGS, "The table in python." },
  { NULL, NULL, 0, NULL }
};

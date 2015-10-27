#include "Python.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Query_HASHSTRING.hpp"

static PyObject * run_HASHSTRING(PyObject * self, PyObject * args);
static PyObject * num_rows_HASHSTRING(PyObject * self, PyObject * args);
static PyObject * fetch_data_HASHSTRING(PyObject * self, PyObject * args);

static PyMethodDef EmptyHeadedQueryMethods[] = {
  { "run_HASHSTRING", run_HASHSTRING, METH_VARARGS, "" },
  { "num_rows_HASHSTRING", num_rows_HASHSTRING, METH_VARARGS, "Number of rows in result." },
  { "fetch_data_HASHSTRING", fetch_data_HASHSTRING, METH_VARARGS, "The table in python." },
  { NULL, NULL, 0, NULL }
};
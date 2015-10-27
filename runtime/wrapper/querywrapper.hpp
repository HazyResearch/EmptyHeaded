#include "Python.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Query_build_Edge.hpp"

static PyObject * run_build_Edge(PyObject * self, PyObject * args);
static PyObject * num_rows_build_Edge(PyObject * self, PyObject * args);
static PyObject * fetch_data_build_Edge(PyObject * self, PyObject * args);

static PyMethodDef EmptyHeadedQueryMethods[] = {
  { "run_build_Edge", run_build_Edge, METH_VARARGS, "" },
  { "num_rows_build_Edge", num_rows_build_Edge, METH_VARARGS, "Number of rows in result." },
  { "fetch_data_build_Edge", fetch_data_build_Edge, METH_VARARGS, "The table in python." },
  { NULL, NULL, 0, NULL }
};

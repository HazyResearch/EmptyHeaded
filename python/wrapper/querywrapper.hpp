#include "Python.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Query.hpp"

static PyObject * run(PyObject * self, PyObject * args);
static PyObject * num_rows(PyObject * self, PyObject * args);
static PyObject * fetch_data(PyObject * self, PyObject * args);

static PyMethodDef EmptyHeadedQueryMethods[] = {
  { "run", run, METH_VARARGS, "" },
  { "num_rows", num_rows, METH_VARARGS, "Number of rows in result." },
  { "fetch_data", fetch_data, METH_VARARGS, "The table in python." },
  { NULL, NULL, 0, NULL }
};
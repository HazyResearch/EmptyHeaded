#include "Python.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Query.hpp"

static PyObject * run(PyObject * self, PyObject * args);

static PyMethodDef EmptyHeadedQueryMethods[] = {
  { "run", run, METH_VARARGS, "" },
  { NULL, NULL, 0, NULL }
};
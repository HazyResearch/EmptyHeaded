#include "Python.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ghd.hpp"

static PyObject * num_rows(PyObject * self, PyObject * args){
  PyObject * p;
  GHD * v;

  if (!PyArg_ParseTuple(args, "O", &p)) {
    return NULL;
  }

  v = (GHD*)PyCObject_AsVoidPtr(p);  
  PyObject * key_1_o = PyLong_FromLong(v->num_rows);

  Py_INCREF(key_1_o);
  return key_1_o;
}

static PyMethodDef RelationMethodsCPP[] = {
  { "num_rows", num_rows, METH_VARARGS, "Number of rows in result." },
  { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC initrelation(void)
{
  Py_InitModule("relation", RelationMethodsCPP);
}
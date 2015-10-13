#include "Python.h"
#include "query.hpp"

static void free_my_struct(void * p)
{
  printf("Let my_struct run free!\n");
  GHD * v = (GHD *) p;
  free(v);
}

static PyObject * cobj_out(PyObject * self, PyObject * args)
{
  PyObject * p;
  Query<long> * v;

  if (!PyArg_ParseTuple(args, "O", &p)) {
    return NULL;
  }
  
  v = (Query<long>*) PyCObject_AsVoidPtr(p);
  printf("struct is %ld \n", v->result);
  
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject * cobj_in(PyObject * self, PyObject * args)
{
  if (!PyArg_ParseTuple(args, "")) {
    return NULL;
  }
  Query<long> * q = new Query<long>();

  return PyCObject_FromVoidPtr(q, free_my_struct);
}

static PyMethodDef EmptyHeadedMethods[] = {
  { "cobj_in", cobj_in, METH_VARARGS, "" },
  { "cobj_out", cobj_out, METH_VARARGS, "" },
  { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC initemptyheaded(void)
{
  Py_InitModule("emptyheaded", EmptyHeadedMethods);
}
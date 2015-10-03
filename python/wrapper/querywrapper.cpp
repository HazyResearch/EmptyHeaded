#include "querywrapper.hpp"

//this is what is code generated

static void free_my_struct(void * p)
{
  free((Query*)p);
}

static PyObject * run(PyObject * self, PyObject * args)
{
  if (!PyArg_ParseTuple(args, "")) {
    return NULL;
  }
  Query* q = new Query();
  q->run();

  return PyCObject_FromVoidPtr(q, free_my_struct);
}

PyMODINIT_FUNC initquery1(void)
{
  Py_InitModule("query1", EmptyHeadedQueryMethods);
}
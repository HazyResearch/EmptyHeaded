def getCode(name):
	return """
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

static PyObject * num_rows(PyObject * self, PyObject * args){
  PyObject * p;

  if (!PyArg_ParseTuple(args, "O", &p)) {
    return NULL;
  }

  Query* v = (Query*)PyCObject_AsVoidPtr(p); 
  PyObject * key_1_o = PyLong_FromLong(v->num_rows);

  Py_INCREF(key_1_o);
  return key_1_o;
}

static PyObject * fetch_data(PyObject * self, PyObject * args){
  PyObject * p;

  if (!PyArg_ParseTuple(args, "O", &p)) {
    return NULL;
  }

  Query* v = (Query*)PyCObject_AsVoidPtr(p);
  (void) v; 

  PyObject * key_1_o = PyLong_FromLong(2);

  Py_INCREF(key_1_o);
  return key_1_o;
}

PyMODINIT_FUNC init%(name)s(void)
{
  Py_InitModule("%(name)s", EmptyHeadedQueryMethods);
}
"""% locals()
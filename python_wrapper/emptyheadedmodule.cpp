#include "Python.h"
#include "emptyheaded.hpp"

typedef struct {
  int a;
  int b;
  char c[50];
} my_struct;

static void free_my_struct(void * p)
{
  printf("Let my_struct run free!\n");
  my_struct * v = (my_struct *) p;
  free(v);
}

static PyObject * cobj_out(PyObject * self, PyObject * args)
{
  PyObject * p;
  my_struct * v;

  if (!PyArg_ParseTuple(args, "O", &p)) {
    return NULL;
  }
  
  v = (my_struct*) PyCObject_AsVoidPtr(p);
  printf("struct is %d, %d, %s\n", v->a, v->b, v->c);
  
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject * cobj_in(PyObject * self, PyObject * args)
{
  if (!PyArg_ParseTuple(args, "")) {
    return NULL;
  }
  my_struct * x = (my_struct*) malloc(sizeof(my_struct));

  x->a = 15;
  x->b = 25;
  sprintf(x->c, "this is my string");

  return PyCObject_FromVoidPtr(x, free_my_struct);
}


static PyObject * complex_return_type2(PyObject * self, PyObject * args)
{
  int key_1 = 5;
  int key_2 = 6;
  char * key_3 = (char *)"this is my key";

  char * value_1 = (char *)"this is value 1";
  char * value_2 = (char *)"this is another value";
  char * value_3 = (char *)"this is my final value";

  if (!PyArg_ParseTuple(args, "")) {
    return NULL;
  }

  return Py_BuildValue("[iissss]", key_1, key_2, key_3,
		       value_1, value_2, value_3);
}


static PyObject * complex_return_type(PyObject * self, PyObject * args)
{
  int key_1 = 5;
  int key_2 = 6;
  char * key_3 = (char *)"this is my key";

  char * value_1 = (char *)"this is value 1";
  char * value_2 = (char *)"this is another value";
  char * value_3 = (char *)"this is my final value";

  if (!PyArg_ParseTuple(args, "")) {
    return NULL;
  }

  PyObject * my_dict = PyDict_New();

  PyObject * key_1_o = PyInt_FromLong(key_1);
  PyObject * key_2_o = PyInt_FromLong(key_2);
  PyObject * key_3_o = PyString_FromString(key_3);

  PyObject * value_1_o = PyString_FromString(value_1);
  PyObject * value_2_o = PyString_FromString(value_2);
  PyObject * value_3_o = PyString_FromString(value_3);

  if (PyDict_SetItem(my_dict, key_1_o, value_1_o) == -1) {
    return NULL;
  }
  if (PyDict_SetItem(my_dict, key_2_o, value_2_o) == -1) {
    return NULL;
  }
  if (PyDict_SetItem(my_dict, key_3_o, value_3_o) == -1) {
    return NULL;
  }

  return my_dict;
}

static PyObject * query_wrapper(PyObject * self, PyObject * args)
{
  char * input = NULL;
  char * result;
  PyObject * ret;

  if (!PyArg_ParseTuple(args, "|s", &input)) {
    return NULL;
  }

  if (input == NULL) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  result = query(input);

  ret = PyString_FromString(result);
  free(result);

  return ret;
}

static PyMethodDef EmptyHeadedMethods[] = {
  { "query", query_wrapper, METH_VARARGS, "Say hello" },
  { "complex", complex_return_type, METH_VARARGS, "Return a complex data type" },
  { "complex2", complex_return_type2, METH_VARARGS, "Return a complex data type" },
  { "cobj_in", cobj_in, METH_VARARGS, "" },
  { "cobj_out", cobj_out, METH_VARARGS, "" },
  { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC initemptyheaded(void)
{
  Py_InitModule("emptyheaded", EmptyHeadedMethods);
}
def getCode(name):
	return """
#include "querywrapper.hpp"
#include <iostream>

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

  Query* q = (Query*)PyCObject_AsVoidPtr(p);
  Trie<void*,ParMMapBuffer>* result = (Trie<void*,ParMMapBuffer>*) q->result;
	result->foreach([&](std::vector<uint32_t>* tuple,void* value){
	  for(size_t i =0; i < tuple->size(); i++){
	    std::cout << tuple->at(i) << " ";
	  }
	  std::cout << std::endl;
	});
  PyObject * key_1_o = PyLong_FromLong(2);

  Py_INCREF(key_1_o);
  return key_1_o;
}

PyMODINIT_FUNC init%(name)s(void)
{
  Py_InitModule("%(name)s", EmptyHeadedQueryMethods);
}
"""% locals()
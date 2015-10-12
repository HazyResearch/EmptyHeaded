def getCode(name,mem,types,annotationType):
	t_len = len(types)
	code = """
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

  Query* q = (Query*)PyCObject_AsVoidPtr(p); 
  Trie<%(annotationType)s,%(mem)s>* result = (Trie<%(annotationType)s,%(mem)s>*) q->result;
  PyObject * key_1_o = PyLong_FromLong(result->num_rows);

  Py_INCREF(key_1_o);
  return key_1_o;
}

static PyObject * fetch_data(PyObject * self, PyObject * args){
  PyObject * p;

  if (!PyArg_ParseTuple(args, "O", &p)) {
    return NULL;
  }

  Query* q = (Query*)PyCObject_AsVoidPtr(p);
  Trie<%(annotationType)s,%(mem)s>* result = (Trie<%(annotationType)s,%(mem)s>*) q->result;
  std::vector<void*>* encodings = (std::vector<void*>*) q->encodings;
  (void) encodings;
	result->foreach([&](std::vector<uint32_t>* tuple,%(annotationType)s value){
	  assert(tuple->size() == %(t_len)s);"""% locals()
	i = 0
	for t in types:
		code += """
		Encoding<%(t)s>* Encoding_%(i)s = (Encoding<%(t)s>*)encodings->at(%(i)s);
		std::cout << Encoding_%(i)s->key_to_value.at(tuple->at(%(i)s)) << " ";"""% locals()
		i += 1 
	code+="""std::cout << std::endl;
	});

  PyObject * key_1_o = PyLong_FromLong(2);

  Py_INCREF(key_1_o);
  return key_1_o;
}

PyMODINIT_FUNC init%(name)s(void)
{
  Py_InitModule("%(name)s", EmptyHeadedQueryMethods);
}"""% locals()
	return code
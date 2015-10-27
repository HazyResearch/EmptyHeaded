def getCode(hashstring,mem,types,annotationType):
	t_len = len(types)
	code = """
#include "querywrapper.hpp"
#include <iostream>

//this is what is code generated

static void free_my_struct_%(hashstring)s(void * p)
{
  free((Query_%(hashstring)s*)p);
}

static PyObject * run_%(hashstring)s(PyObject * self, PyObject * args)
{
  if (!PyArg_ParseTuple(args, "")) {
    return NULL;
  }

  Query_%(hashstring)s* q = new Query_%(hashstring)s();
  q->run_%(hashstring)s();

  return PyCObject_FromVoidPtr(q, free_my_struct_%(hashstring)s);
}

static PyObject * num_rows_%(hashstring)s(PyObject * self, PyObject * args){
  PyObject * p;

  if (!PyArg_ParseTuple(args, "O", &p)) {
    return NULL;
  }

  Query_%(hashstring)s* q = (Query_%(hashstring)s*)PyCObject_AsVoidPtr(p); 
  Trie<%(annotationType)s,%(mem)s>* result = (Trie<%(annotationType)s,%(mem)s>*) q->result_%(hashstring)s;
  PyObject * key_1_o = PyLong_FromLong(result->num_rows);

  Py_INCREF(key_1_o);
  return key_1_o;
}

static PyObject * fetch_data_%(hashstring)s(PyObject * self, PyObject * args){
  PyObject * p;

  if (!PyArg_ParseTuple(args, "O", &p)) {
    return NULL;
  }

  Query_%(hashstring)s* q = (Query_%(hashstring)s*)PyCObject_AsVoidPtr(p);
  Trie<%(annotationType)s,%(mem)s>* result = (Trie<%(annotationType)s,%(mem)s>*) q->result_%(hashstring)s;
  std::vector<void*> encodings = result->encodings;
  PyObject *retTable = PyList_New(0);
  (void) encodings;
	result->foreach([&](std::vector<uint32_t>* tuple,%(annotationType)s value){
	  assert(tuple->size() == %(t_len)s);
  	  PyObject *retRow = PyTuple_New(tuple->size());"""% locals()
  	i = 0
	for t in types:
		if t == "long":
			code+="""PyObject * rowelem_%(i)s = PyLong_FromLong(((Encoding<%(t)s>*)encodings.at(%(i)s))->key_to_value.at(tuple->at(%(i)s)));
			PyTuple_SetItem(retRow,%(i)s,rowelem_%(i)s);"""% locals()
		elif t == "int":
			print "NOT IMPLEMENTED"
		elif t == "string":
			print "NOT IMPLEMENTED"
		elif t == "float":
			print "NOT IMPLEMENTED"
		i += 1
	code += """if(PyList_Append(retTable,retRow) == -1){
   	std::cout << "ERROR INSERTING ROW";}"""% locals()
	code += """});
  return retTable;
}

PyMODINIT_FUNC initQuery_%(hashstring)s(void)
{
  Py_InitModule("Query_%(hashstring)s", EmptyHeadedQueryMethods);
}"""% locals()
	return code
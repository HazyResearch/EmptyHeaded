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
  PyObject *retTable = PyList_New(0);
  (void) encodings;
	result->foreach([&](std::vector<uint32_t>* tuple,%(annotationType)s value){
	  assert(tuple->size() == %(t_len)s);
  	  PyObject *retRow = PyTuple_New(tuple->size());"""% locals()
  	i = 0
	for t in types:
		if t == "long":
			code+="""PyObject * rowelem_%(i)s = PyLong_FromLong(((Encoding<%(t)s>*)encodings->at(%(i)s))->key_to_value.at(tuple->at(%(i)s)));
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
  std::cout << PyList_Size(retTable) << std::endl;

  return retTable;
}

PyMODINIT_FUNC init%(name)s(void)
{
  Py_InitModule("%(name)s", EmptyHeadedQueryMethods);
}"""% locals()
	return code
from cpython.cobject cimport PyCObject_AsVoidPtr
import cython
cimport cython
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from cpython.cobject cimport PyCObject_AsVoidPtr

cdef extern from "Query_#HASHSTRING#.hpp":
  # Imports definitions from a c header file
  # Corresponding source file (cfunc.c) must be added to
  # the extension definition in setup.py for proper compiling & linking
  void Query_#HASHSTRING#(unordered_map[string,void*]* _Triemap)

def c_query(tm):
  _Triemap = \
    <unordered_map[string,void*]*>PyCObject_AsVoidPtr(tm)
  Query_#HASHSTRING#(_Triemap)
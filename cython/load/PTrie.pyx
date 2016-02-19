import cython
cimport cython
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
import numpy as np
cimport numpy as np
cimport cpython
from cpython.ref cimport PyObject
import imp
import os
from cpython.cobject cimport PyCObject_AsVoidPtr
cimport PTrie

cdef extern from "load.hpp":
  # Imports definitions from a c header file
  # Corresponding source file (cfunc.c) must be added to
  # the extension definition in setup.py for proper compiling & linking
  void* load(unordered_map[string,void*]* _Triemap)

cdef class PTrie:
  cdef:
    #Cython does not like storing vector[void*] in a unordered_map.
    Trie[void*,ParMemoryBuffer]* _TriePointer

  def __cinit__(PTrie self):
    self._TriePointer = NULL

  def __init__(PTrie self,tm):
    _Triemap = \
      <unordered_map[string,void*]*>PyCObject_AsVoidPtr(tm)
    print "LOAD1: " + str(_Triemap.size())
    self._TriePointer = <Trie[void*,ParMemoryBuffer]*>load(_Triemap)
    print "LOAD2: " + str(_Triemap.size())

  cdef int _check_alive(PTrie self) except -1:
    # Beacuse of the context manager protocol, the C++ object
    # might die before Trie self is reclaimed.
    # We therefore need a small utility to check for the
    # availability of self._Triemap
    if self._TriePointer == NULL:
      raise RuntimeError("Wrapped C++ object is deleted")
    else:
      return 0

  # The context manager protocol allows us to precisely
  # control the liftetime of the wrapped C++ object. del
  # is called deterministically and independently of 
  # the Python garbage collection.

  def __enter__(PTrie self):
    self._check_alive()
    return self

  def __exit__(PTrie self, exc_tp, exc_val, exc_tb):
    self._TriePointer = NULL # inform __dealloc__
    return False # propagate exceptions

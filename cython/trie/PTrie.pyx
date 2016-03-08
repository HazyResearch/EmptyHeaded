# distutils: language = c++

import cython
cimport cython
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
import numpy as np
import pandas as pd
cimport numpy as np
cimport cpython
from cpython.ref cimport PyObject
import imp
import os
from libc.stdint cimport uint32_t, uint64_t, int32_t, int64_t
from cpython.cobject cimport PyCObject_AsVoidPtr
cimport #PTrie#

ctypedef Trie[#ANNOTYPE#,ParMemoryBuffer]* trietype

cdef extern from "TrieWrapper.hpp":
  # Imports definitions from a c header file
  # Corresponding source file (cfunc.c) must be added to
  # the extension definition in setup.py for proper compiling & linking
  void* get(unordered_map[string,void*]* _Triemap)
  void load(unordered_map[string,void*]* _Triemap)
  vector[void*] getDF(trietype _Trie)


def c_load(tm):
    # Exposes a c function to python
    _Triemap = \
      <unordered_map[string,void*]*>PyCObject_AsVoidPtr(tm)
    load(_Triemap)

cdef class #PTrie#:
  cdef:
    #Cython does not like storing vector[void*] in a unordered_map.
    trietype _TriePointer

  def __cinit__(#PTrie# self):
    self._TriePointer = NULL

  def __init__(#PTrie# self):
    self._TriePointer = NULL

  def get(#PTrie# self,tm):
    _Triemap = \
      <unordered_map[string,void*]*>PyCObject_AsVoidPtr(tm)
    self._TriePointer = <trietype>get(_Triemap)

  def getDF(#PTrie# self):
    cdef vector[void*] data
    data = getDF(self._TriePointer)
    df = convert(data,self.num_rows)
    return df

  property annotated:
    # Here we use a property to expose the public member
    # x of Trie to Python
    def __get__(#PTrie# self):
      self._check_alive()
      return self._TriePointer.annotated

  property num_rows:
    # Here we use a property to expose the public member
    # x of Trie to Python
    def __get__(#PTrie# self):
      self._check_alive()
      return self._TriePointer.num_rows

  property num_columns:
    # Here we use a property to expose the public member
    # x of Trie to Python
    def __get__(#PTrie# self):
      self._check_alive()
      return self._TriePointer.num_columns

  cdef int _check_alive(#PTrie# self) except -1:
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

  def __enter__(#PTrie# self):
    self._check_alive()
    return self

  def __exit__(#PTrie# self, exc_tp, exc_val, exc_tb):
    self._TriePointer = NULL # inform __dealloc__
    return False # propagate exceptions


np.import_array()

#Class should just store a map from (name -> pair[vector[void*],size]) 
@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline uint32c2np(void* ptr, length):
  #method 1 dataframe to c++ just return input data & a size
  ##maybe called 
  cdef size_t n
  n = length
  a = <uint32_t *>ptr

  #method 2 take a pointer to memory and return an nd array.
  cdef np.npy_intp shape[1]
  shape[0] = <np.npy_intp> n
  # Create a 1D array, of length 'size'
  ndarray = np.PyArray_SimpleNewFromData(1, shape,
          np.NPY_UINT32, a)
  return ndarray

@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline uint64c2np(void* ptr, length):
  #method 1 dataframe to c++ just return input data & a size
  ##maybe called 
  cdef size_t n
  n = length
  a = <uint64_t *>ptr

  #method 2 take a pointer to memory and return an nd array.
  cdef np.npy_intp shape[1]
  shape[0] = <np.npy_intp> n
  # Create a 1D array, of length 'size'
  ndarray = np.PyArray_SimpleNewFromData(1, shape,
          np.NPY_UINT64, a)
  return ndarray

@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline int32c2np(void* ptr, length):
  #method 1 dataframe to c++ just return input data & a size
  ##maybe called 
  cdef size_t n
  n = length
  a = <int32_t *>ptr

  #method 2 take a pointer to memory and return an nd array.
  cdef np.npy_intp shape[1]
  shape[0] = <np.npy_intp> n
  # Create a 1D array, of length 'size'
  ndarray = np.PyArray_SimpleNewFromData(1, shape,
          np.NPY_INT32, a)
  return ndarray

@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline int64c2np(void* ptr, length):
  #method 1 dataframe to c++ just return input data & a size
  ##maybe called 
  cdef size_t n
  n = length
  a = <int64_t *>ptr

  #method 2 take a pointer to memory and return an nd array.
  cdef np.npy_intp shape[1]
  shape[0] = <np.npy_intp> n
  # Create a 1D array, of length 'size'
  ndarray = np.PyArray_SimpleNewFromData(1, shape,
          np.NPY_INT64, a)
  return ndarray

@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline floatc2np(void* ptr, length):
  #method 1 dataframe to c++ just return input data & a size
  ##maybe called 
  cdef size_t n
  n = length
  a = <float *>ptr

  #method 2 take a pointer to memory and return an nd array.
  cdef np.npy_intp shape[1]
  shape[0] = <np.npy_intp> n
  # Create a 1D array, of length 'size'
  ndarray = np.PyArray_SimpleNewFromData(1, shape,
          np.NPY_FLOAT32, a)
  return ndarray

@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline doublec2np(void* ptr, length):
  #method 1 dataframe to c++ just return input data & a size
  ##maybe called 
  cdef size_t n
  n = length
  a = <float *>ptr

  #method 2 take a pointer to memory and return an nd array.
  cdef np.npy_intp shape[1]
  shape[0] = <np.npy_intp> n
  # Create a 1D array, of length 'size'
  ndarray = np.PyArray_SimpleNewFromData(1, shape,
          np.NPY_DOUBLE, a)
  return ndarray
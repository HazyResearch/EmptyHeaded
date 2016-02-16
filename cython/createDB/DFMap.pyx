import cython
cimport cython
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
import numpy as np
cimport numpy as np
cimport cpython

from libc.stdint cimport uint64_t, uint32_t, int64_t, int32_t

###############################################
##FIXME: Delete this block
##Needed for PyArray_SimpleNewFromData
np.import_array()

#Class should just store a map from (name -> pair[vector[void*],size]) 

@cython.boundscheck(False)
@cython.wraparound(False)
def ndarray2vector(np.ndarray[int, ndim=1, mode='c'] input not None):
  #method 1 dataframe to c++ just return input data & a size
  ##maybe called 
  cdef int n
  n = input.shape[0]
  a = <int *>input.data

  #method 2 take a pointer to memory and return an nd array.
  cdef np.npy_intp shape[1]
  shape[0] = <np.npy_intp> n
  # Create a 1D array, of length 'size'
  ndarray = np.PyArray_SimpleNewFromData(1, shape,
          np.NPY_INT, a)
  print ndarray
  #print ndarray 
  return None
###############################################################################
###TO DO FILL ME OUT FOR ALL TYPES THAT WE WANT.
#Pulls the pointer to the data and returns it as a void* for each type.
cdef void* p2cint(np.ndarray[int32_t, ndim=1, mode='c'] input):
  cdef int32_t* r
  r = <int32_t*> &input[0]
  return <void*> r
cdef void* p2cuint(np.ndarray[uint32_t, ndim=1, mode='c'] input):
  cdef uint32_t* r
  r = <uint32_t*> &input[0]
  return <void*> r
cdef void* p2clong(np.ndarray[int64_t, ndim=1, mode='c'] input):
  cdef int64_t* r
  r = <int64_t*> &input[0]
  return <void*> r
cdef void* p2culong(np.ndarray[uint64_t, ndim=1, mode='c'] input):
  cdef uint64_t* r
  r = <uint64_t*> &input[0]
  return <void*> r
cdef void* p2cfloat(np.ndarray[float, ndim=1, mode='c'] input):
  cdef float* r
  r = <float*> &input[0]
  return <void*> r
cdef void* p2cdouble(np.ndarray[double, ndim=1, mode='c'] input):
  cdef double* r
  r = <double*> &input[0]
  return <void*> r
###############################################################################
ctypedef vector[void*] myvector
ctypedef pair[size_t,myvector] mypair

cdef extern from "loadAndEncode.hpp":
    void loadAndEncode(unordered_map[string,mypair]* map)

cdef pair[size_t,myvector] get_pair(size_t a, myvector v):
  cdef pair[size_t,myvector] p
  p.first = a
  p.second = v
  return p

cdef pair[string,mypair] get_final_pair(string a, mypair b):
  cdef pair[string,mypair] p
  p.first = a
  p.second = b
  return p

cdef myvector get_vector():
  cdef myvector v
  return v

cdef class #DFMap#:
  """ 
  Maintains the map from (relation names -> (len,data)
  """
  cdef:
    #Cython does not like storing vector[void*] in a unordered_map.
    unordered_map[string,mypair]* _dfmap

  def __cinit__(#DFMap# self):
  # Initialize the "this pointer" to NULL so __dealloc__
  # knows if there is something to deallocate. Do not 
  # call new TestClass() here.
    self._dfmap = NULL

  def __init__(#DFMap# self, relations):
    # Constructing the C++ object might raise std::bad_alloc
    # which is automatically converted to a Python MemoryError
    # by Cython. We therefore need to call "new TestClass()" in
    # __init__ instead of __cinit__.

    self._dfmap = new unordered_map[string,mypair]()
    for rel in relations:
      if rel.df:
        df = rel.dataframe
        v = get_vector()
        length = 0
        for i in range(0,df.shape[1]):
          column = df.iloc[:,i].as_matrix()
          if length != 0 and column.shape[0] != length:
            raise Exception("All columns must be the same length in the dataframe.")
          length = column.shape[0] 

          #transfers the dataframe to C land
          if str(column.dtype.name) == "int32":
            v.push_back(p2cint(column))
          elif str(column.dtype.name) == "uint32":
            v.push_back(p2cuint(column))
          elif str(column.dtype.name) == "int64":
            v.push_back(p2clong(column))
          elif str(column.dtype.name) == "uint64":
            v.push_back(p2culong(column))
          elif str(column.dtype.name) == "float32":
            v.push_back(p2cfloat(column))
          elif str(column.dtype.name) == "float64":
            v.push_back(p2cdouble(column))
          else:
            raise Exception("DataFrame type error. Type " + str(column.dtype.name) + " not accepted.")

        p = get_pair(length,v)
        self._dfmap.insert(get_final_pair(rel.name,p))
    loadAndEncode(self._dfmap)

  def __dealloc__(#DFMap# self):
    # Only call del if the C++ object is alive, 
    # or we will get a segfault.
    if self._dfmap != NULL:
      del self._dfmap

  cdef int _check_alive(#DFMap# self) except -1:
    # Beacuse of the context manager protocol, the C++ object
    # might die before DFMap self is reclaimed.
    # We therefore need a small utility to check for the
    # availability of self._dfmap
    if self._dfmap == NULL:
      raise RuntimeError("Wrapped C++ object is deleted")
    else:
      return 0    


  # The context manager protocol allows us to precisely
  # control the liftetime of the wrapped C++ object. del
  # is called deterministically and independently of 
  # the Python garbage collection.

  def __enter__(#DFMap# self):
    self._check_alive()
    return self

  def __exit__(#DFMap# self, exc_tp, exc_val, exc_tb):
    if self._dfmap != NULL:
      del self._dfmap
    self._dfmap = NULL # inform __dealloc__
    return False # propagate exceptions

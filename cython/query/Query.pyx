# distutils: language = c++

from cpython.cobject cimport PyCObject_AsVoidPtr
import cython
cimport cython
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from cpython.cobject cimport PyCObject_AsVoidPtr
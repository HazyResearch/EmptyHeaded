
# Using a .pxd file gives us a separate namespace for
# the C++ declarations. Using a .pxd file also allows
# us to reuse the declaration in multiple .pyx modules.
from libcpp cimport bool
from libcpp.string cimport string
from libc.stdint cimport uint32_t, uint64_t, int32_t, int64_t

cdef extern from "Trie.hpp":
  cdef cppclass Trie[A,M]:
    bool annotated
    size_t num_rows
    size_t num_columns

cdef extern from "utils/ParMemoryBuffer.hpp":
  cdef cppclass ParMemoryBuffer:
    string path
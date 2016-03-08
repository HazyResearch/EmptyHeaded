project(emptyheaded)

add_library(emptyheaded SHARED 
  utils/thread_pool.cpp
  utils/parallel.cpp
	utils/MemoryBuffer.cpp
	utils/MMapBuffer.cpp
	utils/ParMemoryBuffer.cpp
	utils/ParMMapBuffer.cpp
	Encoding.cpp
	Trie.cpp
	TrieBuilder.cpp
	TrieIterator.cpp
)

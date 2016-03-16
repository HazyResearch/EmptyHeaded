#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"
#include "load.hpp"

int main()
{
 thread_pool::initializeThreadPool();

  //std::string ehhome = std::string(getenv ("EMPTYHEADED_HOME"));

  auto tup = load_vector_and_matrix(
    "vector.tsv",
    "/dfs/scratch0/caberger/systems/matrix_benchmarking/data/simple.tsv");
  Trie<float,ParMemoryBuffer> *V = tup.first;
  Trie<float,ParMemoryBuffer> *M = tup.second;

  M->print();
  V->print();

  //result->print();
  return 0;
}

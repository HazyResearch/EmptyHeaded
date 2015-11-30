#include <vector>
#include <stdint.h>
#include <tuple>
#include "Trie.hpp"
#include "Encoding.hpp"

#include "complex.hpp"

typedef std::complex<double> C;

struct application{
  public:
    virtual void run_0() = 0;
};

struct ParMMapBuffer;
struct ParMemoryBuffer;

//template types are the types of the attributes, followed by the type of the annotation
struct Query_0 : public application {
  void* result_0;

	Query_0(){}
	void run_0();
  void getIndices(
      int b, int p, int level, std::vector<uint32_t> row,
      std::vector<std::vector<uint32_t>>& results);
  Trie<C, ParMemoryBuffer> getInput(
      int b, int m, int n
  );
  Trie<C, ParMemoryBuffer> getFactorTrie(
      int b, int m, int i, int j
  );
};

application* init_app(){
  return new Query_0();
}
int main () {
  application* q = init_app();
  q->run_0();
}


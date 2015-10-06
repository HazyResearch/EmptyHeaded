#include "utils/utils.hpp"
#include "intermediate/intermediate.hpp"
#include "Encoding.hpp"
#include "Trie.hpp"
#include "trie/TrieBlock.hpp"

template<class T>
class application{
  public:
    virtual void run(std::string p) = 0;
};
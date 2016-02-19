#ifndef _LOAD_H_
#define _LOAD_H_

#include <iostream>
#include <vector>
#include <unordered_map>

#include "Trie.hpp"
#include "Encoding.hpp"

typedef std::unordered_map<std::string,void*> mymap;

/*
Loads relations from disk.
Input: Map from Name -> (void*)Trie*
Output: (void*)Trie*
*/
void* load(mymap* map){
  std::cout << "HERE LOADING AND INSERTING" << std::endl;
  Trie<void*,ParMemoryBuffer>* mytrie = Trie<void*,ParMemoryBuffer>::load("/Users/caberger/Documents/Research/code/EmptyHeaded/python/db/relations/graph/graph_0_1");
  ///std::cout << map->at("graph").size() << std::endl;
  map->insert(std::pair<std::string,void*>("graph",mytrie));
  return mytrie;
}

#endif
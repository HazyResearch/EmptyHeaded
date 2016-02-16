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
  ///std::cout << map->at("graph").size() << std::endl;
  map->insert(std::pair<std::string,void*>("graph",NULL));
  return NULL;
}

#endif
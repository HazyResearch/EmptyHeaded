#include <iostream>
#include <vector>
#include <unordered_map>

typedef std::unordered_map<std::string,void*> mymap;

void Query(mymap* map){
  std::cout << "QUERYING" << std::endl;
  map->insert(std::pair<std::string,void*>("query",NULL));
}
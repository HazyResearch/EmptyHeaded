#include "run.hpp"

int main()
{
  std::unordered_map<std::string, void *>* input = new std::unordered_map<std::string, void *>();
  //build(NULL);
  run(input);
  return 0;
}

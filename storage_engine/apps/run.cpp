#include "run.hpp"

int main()
{
  std::unordered_map<std::string, void *>* input = new std::unordered_map<std::string, void *>();
  run(input);
  return 0;
}

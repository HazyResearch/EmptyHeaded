#include "Query.hpp"
#include "GHD.hpp"

Query::Query(){
  num_rows = 0;
}

void Query::run(){
  //create some GHD and code gen here.
  //codegen should be nothing but calls into GHD methods
  GHD* a = new GHD();
  result = a->run();
  num_rows = result->num_rows;
}
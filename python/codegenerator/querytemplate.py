def getCode(includes,run):
	return """
#include "../codegen/Query.hpp"
#include <iostream>

%(includes)s

Query::Query(){
  num_rows = 0;
  thread_pool::initializeThreadPool();
}

void Query::run(){
  %(run)s
}
"""% locals()
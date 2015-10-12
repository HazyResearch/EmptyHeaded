def getCode(includes,run):
	return """
#include "../codegen/Query.hpp"
#include <iostream>

%(includes)s

Query::Query(){
  thread_pool::initializeThreadPool();
}

void Query::run(){
  %(run)s
  thread_pool::deleteThreadPool();
}
"""% locals()
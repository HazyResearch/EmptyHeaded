def getCode(includes,run,hashstring):
	return """
#include "Query_%(hashstring)s.hpp"
#include <iostream>

%(includes)s

void Query_%(hashstring)s::run_%(hashstring)s(){
  thread_pool::initializeThreadPool();
  %(run)s
  thread_pool::deleteThreadPool();
}
"""% locals()
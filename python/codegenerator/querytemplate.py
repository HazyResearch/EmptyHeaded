def getCode(run):
	return """
#include "Query.hpp"
#include "GHD.hpp"
#include <iostream>

template<class T, class R, class A>
Query<T,R,A>::Query(){
  num_rows = 0;
}

template<class T, class R, class A>
void Query<T,R,A>::run(){
  //create some GHD and code gen here.
  //codegen should be nothing but calls into GHD methods
  %(run)s
}

template<class T, class R, class A>
std::tuple<std::vector<T>,std::vector<R>,std::vector<A>>  
Query<T,R,A>::fetch_result(){

  std::vector<T> v1;
  std::vector<R> v2;
  std::vector<A> v3;

  v1.push_back(64);
  v2.push_back(64);
  

  return std::make_tuple(v1,v2,v3);
}

template struct Query<long,long,long>;
"""% locals()
#ifndef _QUERY14_H_
#define _QUERY14_H_

#include <vector>
#include <stdint.h>
#include <tuple>
#include "Trie.hpp"
#include "Encoding.hpp"

struct application{
  public:
    virtual void run14() = 0;
};

struct ParMMapBuffer;
struct ParMemoryBuffer;

//template types are the types of the attributes, followed by the type of the annotation
struct Query14 : public application {
  void* result14;

	Query14(){}
	void run14();
};

application* init_app(){
  return new Query14(); 
}
int main () {
  application* q = init_app();
  q->run14();
}
#ifdef GOOGLE_TEST
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif

#endif
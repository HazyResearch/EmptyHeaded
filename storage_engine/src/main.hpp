#include "emptyheaded.hpp"
#include <getopt.h>

class application{
  public:
    virtual void run() = 0;
};

application* init_app();

#ifdef GOOGLE_TEST
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif
#ifdef EXECUTABLE
int main () {
  thread_pool::initializeThreadPool();
  application* q = init_app();
  q->run();
}
#endif
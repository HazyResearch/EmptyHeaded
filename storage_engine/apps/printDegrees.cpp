#define EXECUTABLE
#include "main.hpp"

struct createGraphDBEdgelist: public application {
  ////////////////////emitInitCreateDB////////////////////
  // init ColumnStores
  void run(){

    std::string db_path = "/Users/caberger/Documents/Research/code/EmptyHeaded/examples/graph/data/facebook/db";


    Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
    {
      auto start_time = timer::start_clock();
      Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
          db_path+"/relations/Edge/Edge_0_1");
      timer::stop_clock("LOADING Trie Edge_0_1", start_time);
    }

    auto e_loading_node = timer::start_clock();
    Encoding<long> *Encoding_node = Encoding<long>::from_binary(
        db_path+"/encodings/node/");


    long prev = -1;
    long count  = 0;
    Trie_Edge_0_1->foreach([&](std::vector<uint32_t>* tuple,void* value){
        long srcnode = Encoding_node->key_to_value.at(tuple->at(0));
        if(prev != srcnode){
          if(prev != -1){
            std::cout << "NODE: " << prev << " COUNT: " << count << std::endl;
          }
          prev = srcnode;
          count = 0;
        }
        count++;
    });
    delete Trie_Edge_0_1;
  }
};

application* init_app(){
  return new createGraphDBEdgelist(); 
}

#define EXECUTABLE
#include "main.hpp"

struct createGraphDBEdgelist: public application {
  ////////////////////emitInitCreateDB////////////////////
  // init ColumnStores
  void run(){

    std::string db_path = "/dfs/scratch0/caberger/datasets/cid-patents/db_python";


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
    long max_node = -1;
    long max_count = 0;
    Trie_Edge_0_1->foreach([&](std::vector<uint32_t>* tuple,void* value){
        long srcnode = Encoding_node->key_to_value.at(tuple->at(0));
        if(prev != srcnode){
          if(prev != -1 && count == 10){
            std::cout << "NODE: " << prev << " COUNT: " << count << std::endl;
          }
          prev = srcnode;
          if(count > max_count){
            max_node = prev;
            max_count = count;
          }
          count = 0;
        }
        count++;
    });
    std::cout <<  "MAX NODE: " << max_node << " COUNT: " << max_count << std::endl;
    delete Trie_Edge_0_1;
  }
};

application* init_app(){
  return new createGraphDBEdgelist(); 
}

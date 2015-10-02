#define GENERATED
#include "main.hpp"
extern "C" void *run(std::string path) {
  std::cout << "RUNNING: " << path << std::endl;
  std::ofstream myfile;
  myfile.open (path + "/edgelist/vector_degree.txt");

  std::ofstream myfile2;
  myfile2.open (path + "/edgelist/vector_inverse_degree.txt");
  ////////////////////emitAllocators////////////////////
  allocator::memory<uint8_t> *output_buffer =
      new allocator::memory<uint8_t>(10000);
  allocator::memory<uint8_t> *tmp_buffer =
      new allocator::memory<uint8_t>(10000);
  (void)tmp_buffer;
  ////////////////////emitLoadBinaryEncoding////////////////////
  auto belt_node = debug::start_clock();
  Encoding<long> *Encoding_node = Encoding<long>::from_binary(
      path+"/db/encodings/node/");
  (void)Encoding_node;
  debug::stop_clock("LOADING ENCODINGS node", belt_node);

  ////////////////////emitLoadBinaryRelation////////////////////
  auto btlt_R_0_1 = debug::start_clock();
  Trie<hybrid, void *> *Trie_R_0_1 = Trie<hybrid, void *>::from_binary(
      path+"/db/relations/R/R_0_1/", false);
  debug::stop_clock("LOADING RELATION R_0_1", btlt_R_0_1);

  {
    auto query_time = debug::start_clock();
    ////////////////////NPRR BAG bag_R_ab////////////////////
    Trie<hybrid, float> *Trie_bag_R_ab =
        new (output_buffer->get_next(0, sizeof(Trie<hybrid, float>)))
            Trie<hybrid, float>(1, true);
    {
      auto start_time = debug::start_clock();
      const TrieBlock<hybrid, float> *TrieBlock_R_0_1_0 =
          (TrieBlock<hybrid, float> *)Trie_R_0_1->head;
      Set<hybrid> a = TrieBlock_R_0_1_0->set;
      // emitNewTrieBlock
      float invACard = (float)(1.0/a.cardinality);
      std::cout << "A CARD: " << invACard << std::endl;
      a.foreach_index([&](uint32_t a_i, uint32_t a_d) {
         (void) a_i;
        const TrieBlock<hybrid, float> *TrieBlock_R_0_1_1_a_b =
            (TrieBlock<hybrid, float> *)TrieBlock_R_0_1_0->get_block(a_d);
        Set<hybrid> b = TrieBlock_R_0_1_1_a_b->set;

        
        long nodeID = Encoding_node->key_to_value.at(a_d);
        myfile << nodeID << "\t" << (float)(1.0/a.cardinality) << "\n";
        myfile2 << nodeID << "\t" << (float)(1.0/b.cardinality) << "\n";

      });
      debug::stop_clock("Bag bag_R_ab", start_time);
    }

    myfile.close();
    myfile2.close();
    // emitRewriteOutputTrie
    debug::stop_clock("QUERY TIME", query_time);
  }

  return NULL;
}
#ifndef GOOGLE_TEST
int main(int argc, char *argv[]) {
  assert(argc == 2);
  thread_pool::initializeThreadPool();
  run(argv[1]);
}
#endif

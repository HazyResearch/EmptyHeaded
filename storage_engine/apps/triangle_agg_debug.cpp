#define EXECUTABLE
#include "main.hpp"

typedef ParMemoryBuffer mem;

struct triangleAgg: public application {
  ////////////////////emitInitCreateDB////////////////////
  // init ColumnStores
  void run(){
    Trie<void *, mem> *Trie_R_0_1 = NULL;
    {
      auto start_time = timer::start_clock();
      // buildTrie
      Trie_R_0_1 = Trie<void *,mem>::load( 
          "/Users/caberger/Documents/Research/data/databases/higgs/db_pruned/relations/R/R_0_1");
      timer::stop_clock("LOADING TRIE R_0_1", start_time);
    }
    /*
    Encoding<long> *Encoding_node = NULL;
    {
      auto start_time = timer::start_clock();
      Encoding_node =
          Encoding<long>::from_binary("/dfs/scratch0/caberger/datasets/higgs/db_python/encodings/node/");
      timer::stop_clock("LOADING ENCODINGS node", start_time);
    }
    */

    auto query_time = timer::start_clock();
    Trie<long,mem> *Trie_Triangle_ = new Trie<long,mem>("here",3);
    {
      ////////////////////NPRR BAG bag_R_abc////////////////////
      {
        auto start_time = timer::start_clock();
       
        const TrieBlock<hybrid,mem>* head = Trie_R_0_1->getHead();  
        const Set<hybrid> a = head->set;
        par::reducer<long> annotation(0,
                                      [](size_t a, size_t b) { return a + b; });

        ParMemoryBuffer* tmp_buffers = new ParMemoryBuffer(100);

        a.par_foreach([&](size_t tid, uint32_t a_d) {
          MemoryBuffer* const b_buffer = tmp_buffers->elements.at(tid);

          const TrieBlock<hybrid,mem>* s1 =  head->get_next_block(a_d,Trie_R_0_1->memoryBuffers);
          const Set<hybrid> s1_s =  s1->set;

          const size_t alloc_size =
            std::max(a.number_of_bytes,
                     s1_s.number_of_bytes);
          Set<hybrid> b((uint8_t*)b_buffer->get_next(alloc_size));
          b_buffer->roll_back(alloc_size); 
          b = *ops::set_intersect(
                  (Set<hybrid> *)&b, (const Set<hybrid> *)&s1_s,
                  (const Set<hybrid> *)&a);
          
          long annotation_b = (long)0;
          b.foreach ([&](uint32_t b_d) {
            const TrieBlock<hybrid,mem>* s2 =  head->get_next_block(b_d,Trie_R_0_1->memoryBuffers);

            size_t count = ops::set_intersect(
            (const Set<hybrid> *)&s1_s,
            (const Set<hybrid> *)&s2->set);

            // emitAnnotationInitialization
            long annotation_c = count;
            annotation_b += (annotation_c * 1 * 1);
          });
          annotation.update(tid, (annotation_b * 1));
        });
        std::cout << "RESULT: " << annotation.evaluate(0) << std::endl;
        timer::stop_clock("Bag bag_R_abc", start_time);
      }
    }
    timer::stop_clock("QUERY TIME", query_time);
    //std::cout << "QUERY RESULT: " << Trie_Triangle_->annotation << std::endl;
  }
};

application* init_app(){
  return new triangleAgg(); 
}

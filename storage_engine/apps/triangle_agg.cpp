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
      // loadTrie
      /*
      Trie_R_0_1 = Trie<void *, mem>::load(
          "/Users/caberger/Documents/Research/data/databases/simple/db/relations/"
          "R/R_0_1");
      */
      Trie_R_0_1 = Trie<void *, mem>::load(
          "/Users/caberger/Documents/Research/data/databases/higgs/db_pruned/relations/"
          "R/R_0_1");
      timer::stop_clock("LOADING TRIE R_0_1", start_time);
    }
    Encoding<long> *Encoding_node = NULL;
    {
      auto start_time = timer::start_clock();
      Encoding_node =
          Encoding<long>::from_binary("/Users/caberger/Documents/Research/data/"
                                      "databases/simple/db/encodings/node/");
      timer::stop_clock("LOADING ENCODINGS node", start_time);
    }

    auto query_time = timer::start_clock();
    Trie<long,mem> *Trie_Triangle_;
    {
      ////////////////////NPRR BAG bag_R_abc////////////////////
      /*
      Trie<long,mem> *Trie_bag_R_abc =
          new (output_buffer->get_next(0, sizeof(Trie<long>)))
              Trie<long>(0, true);
      */
      {
        auto start_time = timer::start_clock();
      
        ParMemoryBuffer *a_buffer =
            new ParMemoryBuffer(10000);
        ParMemoryBuffer *b_buffer =
            new ParMemoryBuffer(10000);
        ParMemoryBuffer *c_buffer =
            new ParMemoryBuffer(10000);
          
        const TrieBlock<hybrid, mem> *TrieBlock_R_0_1_0 =
            (TrieBlock<hybrid, mem> *)Trie_R_0_1->getHead();
        Set<hybrid> a = TrieBlock_R_0_1_0->set;
        // emitAnnotationInitialization
        // emitAggregateReducer
        par::reducer<long> annotation(0,
                                      [](size_t a, size_t b) { return a + b; });
        a.par_foreach([&](size_t tid, uint32_t a_d) {
          (void)tid;
          const TrieBlock<hybrid, mem> *TrieBlock_R_0_1_1_a_b =
              (TrieBlock<hybrid, mem> *)TrieBlock_R_0_1_0->get_next_block(a_d,Trie_R_0_1->memoryBuffers);
          const size_t alloc_size_b =
              std::max(TrieBlock_R_0_1_1_a_b->set.number_of_bytes,
                       TrieBlock_R_0_1_0->set.number_of_bytes);
          Set<hybrid> b(b_buffer->get_next(tid, alloc_size_b));
          b = *ops::set_intersect(
                  &b, (const Set<hybrid> *)&TrieBlock_R_0_1_0->set,
                  (const Set<hybrid> *)&TrieBlock_R_0_1_1_a_b->set);
          // emitAnnotationInitialization
          long annotation_b = (long)0;
          b.foreach ([&](uint32_t b_d) {
            const TrieBlock<hybrid, mem> *TrieBlock_R_0_1_1_b_c =
                (TrieBlock<hybrid, mem> *)TrieBlock_R_0_1_0->get_next_block(b_d,Trie_R_0_1->memoryBuffers);
            const TrieBlock<hybrid, mem> *TrieBlock_R_0_1_1_a_c =
                (TrieBlock<hybrid, mem> *)TrieBlock_R_0_1_0->get_next_block(a_d,Trie_R_0_1->memoryBuffers);
            const size_t alloc_size_c =
                std::max(TrieBlock_R_0_1_1_a_c->set.number_of_bytes,
                         TrieBlock_R_0_1_1_b_c->set.number_of_bytes);
            Set<hybrid> c(c_buffer->get_next(tid, alloc_size_c));
            c = *ops::set_intersect(
                    &c, (const Set<hybrid> *)&TrieBlock_R_0_1_1_b_c->set,
                    (const Set<hybrid> *)&TrieBlock_R_0_1_1_a_c->set);
            // emitAnnotationInitialization
            long annotation_c = (long)0;
            // emitAnnotationComputation
            annotation_c += (c.cardinality * 1 * 1);
            c_buffer->roll_back(tid, alloc_size_c);
            annotation_b += (annotation_c * 1 * 1);
          });
          b_buffer->roll_back(tid, alloc_size_b);
          annotation.update(tid, (annotation_b * 1));
        });
        std::cout << annotation.evaluate(0) << std::endl;
        //a_buffer->free();
        //b_buffer->free();
        //c_buffer->free();
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

#include "main.hpp"

template<class T>
struct triangleAgg: public application<T> {
  ////////////////////emitInitCreateDB////////////////////
  // init ColumnStores
  void run(std::string path){
    ////////////////////emitAllocators////////////////////
    allocator<uint8_t> *output_buffer =
        new allocator<uint8_t>(10000);
    allocator<uint8_t> *tmp_buffer =
        new allocator<uint8_t>(10000);
    (void)tmp_buffer;
    ////////////////////emitLoadBinaryEncoding////////////////////
    auto belt_node = debug::start_clock();
    Encoding<long> *Encoding_node = Encoding<long>::from_binary(
        "/Users/caberger/Documents/Research/code/databases/higgs/db/encodings/node/");
    (void)Encoding_node;
    debug::stop_clock("LOADING ENCODINGS node", belt_node);

    ////////////////////emitLoadBinaryRelation////////////////////
    auto btlt_R_0_1 = debug::start_clock();
    Trie<void *> *Trie_R_0_1 = Trie<void *>::from_binary(
        "/Users/caberger/Documents/Research/code/databases/higgs/db/relations/R/R_0_1/",
        false);
    debug::stop_clock("LOADING RELATION R_0_1", btlt_R_0_1);

    auto query_time = debug::start_clock();
    Trie<long> *Trie_Triangle_;
    {
      ////////////////////NPRR BAG bag_R_abc////////////////////
      Trie<long> *Trie_bag_R_abc =
          new (output_buffer->get_next(0, sizeof(Trie<long>)))
              Trie<long>(0, true);
      {
        auto start_time = debug::start_clock();
        allocator<uint8_t> *a_buffer =
            new allocator<uint8_t>(10000);
        allocator<uint8_t> *b_buffer =
            new allocator<uint8_t>(10000);
        allocator<uint8_t> *c_buffer =
            new allocator<uint8_t>(10000);
        const TrieBlock<hybrid, long> *TrieBlock_R_0_1_0 =
            (TrieBlock<hybrid, long> *)Trie_R_0_1->head;
        Set<hybrid> a = TrieBlock_R_0_1_0->set;
        // emitAnnotationInitialization
        // emitAggregateReducer
        par::reducer<long> annotation(0,
                                      [](size_t a, size_t b) { return a + b; });
        a.par_foreach([&](size_t tid, uint32_t a_d) {
          (void)tid;
          const TrieBlock<hybrid, long> *TrieBlock_R_0_1_1_a_b =
              (TrieBlock<hybrid, long> *)TrieBlock_R_0_1_0->get_block(a_d);
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
            const TrieBlock<hybrid, long> *TrieBlock_R_0_1_1_b_c =
                (TrieBlock<hybrid, long> *)TrieBlock_R_0_1_0->get_block(b_d);
            const TrieBlock<hybrid, long> *TrieBlock_R_0_1_1_a_c =
                (TrieBlock<hybrid, long> *)TrieBlock_R_0_1_0->get_block(a_d);
            const size_t alloc_size_c =
                std::max(TrieBlock_R_0_1_1_a_c->set.number_of_bytes,
                         TrieBlock_R_0_1_1_b_c->set.number_of_bytes);
            Set<hybrid> c(output_buffer->get_next(tid, alloc_size_c));
            c = *ops::set_intersect(
                    &c, (const Set<hybrid> *)&TrieBlock_R_0_1_1_b_c->set,
                    (const Set<hybrid> *)&TrieBlock_R_0_1_1_a_c->set);
            // emitAnnotationInitialization
            long annotation_c = (long)0;
            // emitAnnotationComputation
            annotation_c += (c.cardinality * 1 * 1);
            output_buffer->roll_back(tid, alloc_size_c);
            annotation_b += (annotation_c * 1 * 1);
          });
          b_buffer->roll_back(tid, alloc_size_b);
          annotation.update(tid, (annotation_b * 1));
        });
        Trie_bag_R_abc->annotation = annotation.evaluate(0);
        a_buffer->free();
        b_buffer->free();
        c_buffer->free();
        debug::stop_clock("Bag bag_R_abc", start_time);
      }
      // emitRewriteOutputTrie
      Trie_Triangle_ = Trie_bag_R_abc;
    }
    debug::stop_clock("QUERY TIME", query_time);
    std::cout << "QUERY RESULT: " << Trie_Triangle_->annotation << std::endl;
  }
};

template<class T>
application<T>* init_app(){
  return new triangleAgg<T>(); 
}

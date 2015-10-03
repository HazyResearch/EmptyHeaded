#include "GHD.hpp"
#include "emptyheaded.hpp"

GHD::GHD(){
	thread_pool::initializeThreadPool();
}

std::pair<size_t,void*> GHD::run(){
  ////////////////////emitAllocators////////////////////
  allocator<uint8_t> *output_buffer =
      new allocator<uint8_t>(10000);
  allocator<uint8_t> *tmp_buffer =
      new allocator<uint8_t>(10000);
  (void)tmp_buffer;

    ////////////////////emitInitCreateDB////////////////////
  // init ColumnStores
  ColumnStore<long, long> *R = new ColumnStore<long, long>();
  std::vector<void *> *annotation_R = new std::vector<void *>();
  // init encodings
  SortableEncodingMap<long> *node_encodingMap = new SortableEncodingMap<long>();
  Encoding<long> *Encoding_node = new Encoding<long>();
  {
    auto start_time = debug::start_clock();
    tsv_reader f_reader(
        "/Users/caberger/Documents/Research/data/simple.tsv");
    char *next = f_reader.tsv_get_first();
    while (next != NULL) {
      node_encodingMap->update(R->append_from_string<0>(next));
      next = f_reader.tsv_get_next();
      node_encodingMap->update(R->append_from_string<1>(next));
      next = f_reader.tsv_get_next();
      R->num_rows++;
    }
    debug::stop_clock("READING ColumnStore R", start_time);
  }

  {
    auto start_time = debug::start_clock();
    Encoding_node->build(node_encodingMap->get_sorted());
    delete node_encodingMap;
    debug::stop_clock("BUILDING ENCODINGS", start_time);
  }

  ////////////////////emitEncodeColumnStore////////////////////
  EncodedColumnStore<void *> *Encoded_R =
      new EncodedColumnStore<void *>(annotation_R);
  {
    auto start_time = debug::start_clock();
    // encodeColumnStore
    Encoded_R->add_column(Encoding_node->encode_column(&R->get<0>()),
                          Encoding_node->num_distinct);
    Encoded_R->add_column(Encoding_node->encode_column(&R->get<1>()),
                          Encoding_node->num_distinct);
    debug::stop_clock("ENCODING R", start_time);
  }

  ////////////////////emitBuildTrie////////////////////

  Trie<void *> *Trie_R_0_1 = NULL;
  {
    auto start_time = debug::start_clock();
    // buildTrie
    Trie_R_0_1 = new Trie<void *>( &Encoded_R->max_set_size,
        &Encoded_R->data, &Encoded_R->annotation);
    debug::stop_clock("BUILDING TRIE R_0_1", start_time);
  }

  Trie<long> *Trie_Triangle_;
  {
    auto query_time = debug::start_clock();
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
    debug::stop_clock("QUERY TIME", query_time);
  }

  size_t num_rows = Trie_Triangle_->annotation;
  void* result = Trie_Triangle_;

  return std::make_pair(num_rows,result);
}
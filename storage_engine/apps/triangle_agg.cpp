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
        //"/afs/cs.stanford.edu/u/caberger/db/relations/R/R_0_1"
        //"/dfs/scratch0/caberger/datasets/g_plus/db_python/relations/R/R_0_1"
        //"/Users/caberger/Documents/Research/data/databases/simple/db/relations/R/R_0_1"
        "/Users/caberger/Documents/Research/data/databases/higgs/db_pruned/relations/R/R_0_1"
        );
        //"/dfs/scratch0/caberger/datasets/g_plus/db_python/relations/R/R_0_1");
      timer::stop_clock("LOADING TRIE R_0_1", start_time);
    }

    auto query_time = timer::start_clock();
    Trie<long,mem> *Trie_Triangle_ = new Trie<long,mem>("here",3,true);
    {
      ////////////////////NPRR BAG bag_R_abc////////////////////
      {
        auto start_time = timer::start_clock();
       
        const ParTrieIterator<void*,mem> Iterators_R_a_b(Trie_R_0_1);
        const ParTrieIterator<void*,mem> Iterators_R_b_c(Trie_R_0_1);
        const ParTrieIterator<void*,mem> Iterators_R_a_c(Trie_R_0_1);

        ParTrieBuilder<long,mem> Builders_Triangle(Trie_Triangle_);

        Builders_Triangle.build_aggregated_set(
          Iterators_R_a_b.head);

        par::reducer<long> annotation(0,
          [](size_t a, size_t b) { return a + b; });

        Builders_Triangle.par_foreach_aggregate([&](const size_t tid, const uint32_t a_d) {
          
          TrieBuilder<long,mem>* const Builder_Triangle = Builders_Triangle.builders.at(tid);

          TrieIterator<void*,mem>* const Iterator_R_a_b = Iterators_R_a_b.iterators.at(tid);
          TrieIterator<void*,mem>* const Iterator_R_b_c = Iterators_R_b_c.iterators.at(tid);
          TrieIterator<void*,mem>* const Iterator_R_a_c = Iterators_R_a_c.iterators.at(tid);
        
          Iterator_R_a_b->get_next_block(0,a_d);
          Iterator_R_a_c->get_next_block(0,a_d);

          Builder_Triangle->build_aggregated_set(
            Iterator_R_b_c->get_block(0),
            Iterator_R_a_b->get_block(1)
          );
                  
          long annotation_b = (long)0;
          Builder_Triangle->foreach_aggregate([&](const uint32_t b_d) {
            //std::cout << b_d << std::endl;
            Iterator_R_b_c->get_next_block(0,b_d);

            size_t count = Builder_Triangle->count_set(
              Iterator_R_b_c->get_block(1),
              Iterator_R_a_b->get_block(1)
            );
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

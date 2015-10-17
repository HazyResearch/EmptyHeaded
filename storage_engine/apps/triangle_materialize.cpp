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
        "/Users/caberger/Documents/Research/data/databases/simple/db/relations/R/R_0_1"
        //"/Users/caberger/Documents/Research/data/databases/simple/db/relations/R/R_0_1"
          //"/Users/caberger/Documents/Research/data/databases/simple/db/relations/R/R_0_1"
          //"/dfs/scratch0/caberger/datasets/higgs/db_python/relations/R/R_0_1"
          );
      timer::stop_clock("LOADING TRIE R_0_1", start_time);
    }

    auto query_time = timer::start_clock();
    Trie<void*,mem> *Trie_Triangle_ = new Trie<void*,mem>("here",3,false);
    {
      ////////////////////NPRR BAG bag_R_abc////////////////////
      {
        auto start_time = timer::start_clock();
       
        std::cout << "ITERATORS A" << std::endl;

        const ParTrieIterator<void*,mem> Iterators_R_a_b(Trie_R_0_1);
        const ParTrieIterator<void*,mem> Iterators_R_b_c(Trie_R_0_1);
        const ParTrieIterator<void*,mem> Iterators_R_a_c(Trie_R_0_1);

        std::cout << "ITERATORS" << std::endl;

        ParTrieBuilder<void*,mem> Builders_Triangle(Trie_Triangle_);

        std::cout << "BUILDER" << std::endl;

        const Set<hybrid>* a = Builders_Triangle.build_set(Iterators_R_a_b.head);
        a = Builders_Triangle.allocate_next();

        std::cout << "ALLOCATE NEXT: " << std::endl;

        par::reducer<long> num_rows(0,
          [](size_t a, size_t b) { return a + b; });
        a->par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
          TrieBuilder<void*,mem>* const Builder_Triangle = Builders_Triangle.builders.at(tid);

          TrieIterator<void*,mem>* const Iterator_R_a_b = Iterators_R_a_b.iterators.at(tid);
          TrieIterator<void*,mem>* const Iterator_R_b_c = Iterators_R_b_c.iterators.at(tid);
          TrieIterator<void*,mem>* const Iterator_R_a_c = Iterators_R_a_c.iterators.at(tid);
        
          Iterator_R_a_b->get_next_block(a_d);
          Iterator_R_a_c->get_next_block(a_d);

          const Set<hybrid>* b = Builder_Triangle->build_set(
            tid,
            1,
            Iterator_R_b_c->get_block(0),
            Iterator_R_a_b->get_block(1)
          );

          Builder_Triangle->allocate_next(tid,1);
          b->foreach_index([&](uint32_t b_i, uint32_t b_d) {
            std::cout << "B_D: " << b_d << std::endl;
            Iterator_R_b_c->get_next_block(b_d);
            const Set<hybrid>* c = Builder_Triangle->build_set(
              tid,
              2,
              Iterator_R_b_c->get_block(1),
              Iterator_R_a_b->get_block(1)
            );
            
            num_rows.update(tid, c->cardinality);
            Builder_Triangle->set_level(b_i,b_d,2);
            // emitAnnotationInitialization
          });
          Builder_Triangle->set_level(a_i,a_d,1); //for head we don't give an index
        });
        std::cout << "RESULT: " << num_rows.evaluate(0) << std::endl;
        timer::stop_clock("Bag bag_R_abc", start_time);
      
        Trie_Triangle_->num_rows = num_rows.evaluate(0);
        Trie_Triangle_->foreach([&](std::vector<uint32_t>* tuple,void* value){
          for(size_t i =0; i < tuple->size(); i++){
            std::cout << tuple->at(i) << " ";
          }
          std::cout << std::endl;
        });

      }
    }
    timer::stop_clock("QUERY TIME", query_time);
    //std::cout << "QUERY RESULT: " << Trie_Triangle_->annotation << std::endl;
  }
};

application* init_app(){
  return new triangleAgg(); 
}

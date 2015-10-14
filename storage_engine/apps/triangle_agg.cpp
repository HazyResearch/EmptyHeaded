#define EXECUTABLE
#include "main.hpp"

typedef ParMMapBuffer mem;

struct triangleAgg: public application {
  ////////////////////emitInitCreateDB////////////////////
  // init ColumnStores
  void run(){
    Trie<void *, mem> *Trie_R_0_1 = NULL;
    {
      auto start_time = timer::start_clock();
      // loadTrie
      Trie_R_0_1 = Trie<void *, mem>::load(
          "/Users/caberger/Documents/Research/data/databases/higgs/db_pruned/relations/"
          "R/R_0_1");
      timer::stop_clock("LOADING TRIE R_0_1", start_time);
    }
        mem *yu = new mem("output",100);
    std::cout << "HERE" << std::endl;
    Encoding<long> *Encoding_node = NULL;
    {
      auto start_time = timer::start_clock();
      Encoding_node =
          Encoding<long>::from_binary("/Users/caberger/Documents/Research/data/"
                                      "databases/higgs/db_pruned/encodings/node/");
      timer::stop_clock("LOADING ENCODINGS node", start_time);
    }

    auto query_time = timer::start_clock();
    Trie<long,mem> *Trie_Triangle_ = new Trie<long,mem>("/Users/caberger/Documents/Research/data/databases/higgs/db_pruned/relations/",3);
    std::cout << "TRIE TRIANGLE" << std::endl;
    {
      ////////////////////NPRR BAG bag_R_abc////////////////////
      /*
      Trie<long,mem> *Trie_bag_R_abc =
          new (output_buffer->get_next(0, sizeof(Trie<long>)))
              Trie<long>(0, true);
      */
      {
        /*
        Trie_R_0_1->foreach([&](std::vector<uint32_t>* tuple,void* value){
          for(size_t i =0; i < tuple->size(); i++){
            std::cout << tuple->at(i) << " ";
          }
          std::cout << std::endl;
        });
        */

        auto start_time = timer::start_clock();
       
        TrieBuilder<long,mem> Builder_Triangle(Trie_Triangle_);
        ParTrieIterator<void*,mem> Iterators_R_a_b(Trie_R_0_1);
        ParTrieIterator<void*,mem> Iterators_R_b_c(Trie_R_0_1);
        ParTrieIterator<void*,mem> Iterators_R_a_c(Trie_R_0_1);

        Set<hybrid> a = Trie_R_0_1->getHead()->set;
        // emitAnnotationInitialization
        // emitAggregateReducer
        par::reducer<long> annotation(0,
                                      [](size_t a, size_t b) { return a + b; });
        a.par_foreach([&](size_t tid, uint32_t a_d) {
          TrieIterator<void*,mem>* Iterator_R_a_b = Iterators_R_a_b.iterators.at(tid);
          TrieIterator<void*,mem>* Iterator_R_b_c = Iterators_R_b_c.iterators.at(tid);
          TrieIterator<void*,mem>* Iterator_R_a_c = Iterators_R_a_c.iterators.at(tid);
        
          Iterator_R_a_b->get_next_block(a_d);
          Iterator_R_a_c->get_next_block(a_d);

          Set<hybrid> b = Builder_Triangle.build_aggregated_set(
            tid,
            1,
            Iterator_R_b_c->get_block(0),
            Iterator_R_a_b->get_block(1)
            );
          
          long annotation_b = (long)0;
          b.foreach ([&](uint32_t b_d) {
            Iterator_R_b_c->get_next_block(b_d);
            size_t count = Builder_Triangle.count_set(
              Iterator_R_b_c->get_block(1),
              Iterator_R_a_c->get_block(1)
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

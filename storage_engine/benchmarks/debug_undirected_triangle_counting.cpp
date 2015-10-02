#include "main.hpp"

template<class T>
struct undirected_triangle_counting: public application<T> {
  uint64_t result = 0;
  void run(std::string path){
    //create the relation (currently a column wise table)
    Relation<uint64_t,uint64_t> *R_ab = new Relation<uint64_t,uint64_t>();

//////////////////////////////////////////////////////////////////////
    //File IO (for a tsv, csv should be roughly the same)
    auto rt = debug::start_clock();
    //tsv_reader f_reader("simple.txt");
    tsv_reader f_reader(path);
    char *next = f_reader.tsv_get_first();
    R_ab->num_rows = 0;
    while(next != NULL){
      //have to code generate number of attributes here
      //maybe can accomplish with variadic templates? Might be hard.
      R_ab->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab->num_rows++;
    }
    debug::stop_clock("Reading File",rt);
//////////////////////////////////////////////////////////////////////
    //Encoding
    auto et = debug::start_clock();
    //encode A
    std::vector<Column<uint64_t>> *a_attributes = new std::vector<Column<uint64_t>>();
    a_attributes->push_back(R_ab->get<0>());
    a_attributes->push_back(R_ab->get<1>());
    Encoding<uint64_t> *a_encoding = new Encoding<uint64_t>(a_attributes);
    debug::stop_clock("Encoding",et);
//////////////////////////////////////////////////////////////////////
    //Trie building

    //after all encodings are done, set up encoded relation (what is passed into the Trie)
    //You can switch the ordering here to be what you want it to be in the Trie.
    //A mapping will need to be kept in the query compiler so it is known what
    //encoding each level in the Trie should map to.
    
    auto bt = debug::start_clock();
    std::vector<Column<uint32_t>> *ER_ab = new std::vector<Column<uint32_t>>();
    std::vector<size_t> *ranges_ab = new std::vector<size_t>();

    ER_ab->push_back(a_encoding->encoded.at(0)); //perform filter, selection
    ranges_ab->push_back(a_encoding->num_distinct);
    ER_ab->push_back(a_encoding->encoded.at(1));
    ranges_ab->push_back(a_encoding->num_distinct);

    //add some sort of lambda to do selections 
    Trie<T> *TR_ab = Trie<T>::build(ER_ab,ranges_ab,[&](size_t index){
      return ER_ab->at(0).at(index) > ER_ab->at(1).at(index);
    });
    
    debug::stop_clock("Build",bt);

//////////////////////////////////////////////////////////////////////
    //Prints the relation    
    //R(a,b) join T(b,c) join S(a,c)

    //rpcm.init_counter_states();

    //allocate memory
    allocator::memory<uint8_t> B_buffer(R_ab->num_rows*sizeof(uint64_t));
    allocator::memory<uint8_t> C_buffer(R_ab->num_rows*sizeof(uint64_t));
    par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){
      return a + b;
    });

    auto qt = debug::start_clock();

    const TrieBlock<T> H = *TR_ab->head;
    const Set<T> A = H.set;
    size_t tid = 0;
    uint32_t a_i = 856;

      Set<T> B(B_buffer.get_memory(tid)); //initialize the memory
      Set<T> C(C_buffer.get_memory(tid));

      const Set<T> op1 = H.get_block(a_i)->set;
      std::cout << "a_i: " << a_i << " " << op1.cardinality << std::endl;
      op1.foreach([&](uint32_t data){
        std::cout << data << std::endl;
      });

      B = ops::set_intersect(&B,&op1,&A); //intersect the B
      B.foreach([&](uint32_t b_i){ //Peel off B attributes
        if(b_i == 671){
          std::cout << "A: " << a_i << " A_card: " << A.cardinality << " B: " << b_i << " B_card: " << B.cardinality << std::endl;
          const TrieBlock<T>* l2 = H.get_block(b_i);
          assert(l2 != NULL);
          std::cout << "l2: " << l2->set.cardinality << " " << l2->set.number_of_bytes << std::endl;
          l2->set.foreach([&](uint32_t data){
            std::cout << "data: " << data << std::endl;
          });
          
          std::cout << "op1: " << op1.cardinality << " " << op1.number_of_bytes << std::endl;
          op1.foreach([&](uint32_t data){
            std::cout << data << std::endl;
          });
          
          ops::set_intersect(&C,
            &op1,
            &l2->set);
          size_t count = C.cardinality;
          std::cout << count << " " << C.number_of_bytes << std::endl;
          C.foreach([&](uint32_t data){
            std::cout << "output: " << data << std::endl;
          });
          num_triangles.update(tid,count);
        }
      });

    result = num_triangles.evaluate(0);
    
    debug::stop_clock("Query",qt);

    std::cout << result << std::endl;
    /*
    rpcm.end_counter_states();
    rpcm.print_state();
    */

    //////////////////////////////////////////////////////////////////////
  }
};

template<class T>
application<T>* init_app(){
  return new undirected_triangle_counting<T>(); 
}
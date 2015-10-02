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
    ER_ab->push_back(a_encoding->encoded.at(0)); //perform filter, selection
    ER_ab->push_back(a_encoding->encoded.at(1));

    //add some sort of lambda to do selections 
    Trie<T> *TR_ab = Trie<T>::build(ER_ab,[&](size_t index){
      return ER_ab->at(0).at(index) > ER_ab->at(1).at(index);
    });
    
    debug::stop_clock("Build",bt);

//////////////////////////////////////////////////////////////////////
    //Prints the relation    
    //R(a,b) join T(b,c) join S(a,c)

    //rpcm.init_counter_states();
    //allocate memory
    allocator::memory<uint8_t> B_buffer(R_ab->num_rows*sizeof(uint64_t));
    par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){
      return a + b;
    });

    auto qt = debug::start_clock();

    const TrieBlock<T,size_t> H = *TR_ab->head;
    const Set<T> A = H.set;
    A.par_foreach([&](size_t tid, uint32_t a_i){
      Set<T> B(B_buffer.get_memory(tid)); //initialize the memory

      const Set<T> op1 = H.get_block(a_i)->set;
      B = ops::set_intersect(&B,&op1,&A); //intersect the B
      //std::cout << "A: " << a_i << " card: " << B.cardinality << std::endl; 
      B.foreach([&](uint32_t b_i){ //Peel off B attributes
        //std::cout << "A: " << a_i << " B: " << b_i << std::endl; 
        const TrieBlock<T,size_t>* l2 = H.get_block(b_i);
        const size_t count = ops::set_intersect(
          &l2->set,
          &op1);
        //std::cout << count << std::endl;
        num_triangles.update(tid,count);
      });
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
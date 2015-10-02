#include "main.hpp"

template<class T>
struct undirected_triangle_listing: public application<T> {
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

    //allocate memory
    allocator::memory<uint8_t> output_buffer(R_ab->num_rows * sizeof(uint64_t) * sizeof(TrieBlock<T,size_t>));


    const TrieBlock<T,size_t> H = *TR_ab->head;
    const Set<T> A = H.set;
    TrieBlock<T,size_t>* a_block = new(output_buffer.get_next(0, sizeof(TrieBlock<T,size_t>))) TrieBlock<T,size_t>(H);
    a_block->init_pointers(0,&output_buffer,A.cardinality,a_encoding->num_distinct,false); 

    par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){
      return a + b;
    });

    double t_get_block = 0.0;
    double t_intersection = 0.0;
    auto qt = debug::start_clock();

    A.par_foreach_index([&](size_t tid, uint32_t a_i,uint32_t a_d){
      const Set<T> matching_b = H.get_block(a_i,a_d)->set;

      //build output B block
      TrieBlock<T,size_t>* b_block = new(output_buffer.get_next(tid, sizeof(TrieBlock<T,size_t>))) TrieBlock<T,size_t>(true);
      const size_t alloc_size = sizeof(uint64_t)*a_encoding->num_distinct*2;

      Set<T> B(output_buffer.get_next(tid, alloc_size));

      b_block->set = ops::set_intersect(&B, &matching_b, &A); //intersect the B

      output_buffer.roll_back(tid, alloc_size - b_block->set.number_of_bytes);
      /*
      b_block->init_pointers(tid, &output_buffer, b_block->set.cardinality,a_encoding->num_distinct,true); //find out the range of level 1

      //Set a block pointer to new b block
      a_block->set_block(a_d,a_d,b_block); 

      //Next attribute to peel off
      b_block->set.foreach_index([&](uint32_t b_i, uint32_t b_d){ // Peel off B attributes
        it = debug::start_clock();
        const TrieBlock<T,size_t>* matching_c = H.get_block(b_d);
        t_get_block += debug::stop_clock(it);

        // Placement new!!
        TrieBlock<T,size_t>* c_block = new(output_buffer.get_next(tid, sizeof(TrieBlock<T,size_t>))) TrieBlock<T,size_t>(true);
        c_block->set = Set<T>(output_buffer.get_next(tid, alloc_size));

        it = debug::start_clock();
        const size_t count = ops::set_intersect(&c_block->set, &matching_c->set, &matching_b)->cardinality;
        t_intersection += debug::stop_clock(it);

        num_triangles.update(tid,count);

        output_buffer.roll_back(tid, alloc_size - c_block->set.number_of_bytes);
        b_block->set_block(b_i,b_d,c_block);

        //assert(b_block->get_block(b_d)->set.cardinality == c_block->set.cardinality);        
      });
      */
    });

    std::cout << num_triangles.evaluate(0) << std::endl;
    debug::stop_clock("Query",qt);

    std::cout << "GET BLOCK TIME: " << t_get_block << std::endl;
    std::cout << "INTERSECTION TIME: " << t_intersection << std::endl;

    unsigned long size = 0;
    TrieBlockIterator<T,size_t> a_blockI(a_block);
    a_block->set.foreach([&](uint32_t a_d) {
        TrieBlockIterator<T,size_t> b_blockI = a_blockI.get_block(a_d);
        TrieBlock<T,size_t> *b_block = b_blockI.trie_block;
        Set<T> b_set = b_block->set;
        if (b_block) {
          b_set.foreach([&](uint32_t b_d) {
              TrieBlockIterator<T,size_t> c_blockI = b_blockI.get_block(b_d);
              TrieBlock<T,size_t> *c_block = c_blockI.trie_block;
              Set<T> c_set = c_block->set;
              if (c_block) {
                //std::cout << "A: " << a_d << " B: " << b_d << " Count: " << c_block->set.cardinality << std::endl;
                size += c_set.cardinality;
              }
          });
        }
      });

    result = size;
    std::cout << size << std::endl;
   //////////////////////////////////////////////////////////////////////
  }
};

template<class T>
application<T>* init_app(){
  return new undirected_triangle_listing<T>(); 
}
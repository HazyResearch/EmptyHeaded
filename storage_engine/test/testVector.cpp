#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "Trie.hpp"
#include "Vector.hpp"
#include "VectorOps.hpp"

const size_t vector_length = 10;
const size_t multiplication_factor = 32;
const float init_value = 0.25f;
const size_t offset = 128;

//Add tests for trie build.

//Add tests for vector intersections.
/*
template<class T>
void test_foreach(const Vector<T,float,ParMemoryBuffer> & v){
  float reduced_anno = 0;
  v.foreach([&](const uint32_t index, const uint32_t data, const float& anno){
    (void) index; (void) data;
    reduced_anno += anno;
  });
  REQUIRE((reduced_anno) == (2.75f));
}

template<class T>
void test_parforeach(const Vector<T,float,ParMemoryBuffer> & v){
  par::reducer<float> reduced_anno(0,[&](const float a, const float b){return a + b;});
  v.parforeach_index([&](const size_t tid, const uint32_t index, const uint32_t data){
    reduced_anno.update(tid,v.get(index,data));
  });
  REQUIRE((reduced_anno.evaluate(0)) == (2.75f));
}

template<class T>
void test_get(const Vector<T,float,ParMemoryBuffer> & v){
  for(size_t i = 0; i < vector_length; i++){
    REQUIRE((v.get(offset+i*multiplication_factor)) == (init_value));
  }
}

template<class T>
void test_set(Vector<T,float,ParMemoryBuffer> & v){
  v.set(1,offset+multiplication_factor,0.5f);
  REQUIRE((v.get(offset+multiplication_factor)) == (0.5f));
}

TEST_CASE( "Test basic DenseVector functionality.", "[DenseVector]" ) {
  thread_pool::initializeThreadPool();

  /////////////////////////////////////////////////////////////////////////////
  // Build a Vector
  /////////////////////////////////////////////////////////////////////////////
  ParMemoryBuffer *p = new ParMemoryBuffer(2);
  std::vector<uint32_t> v1;
  std::vector<float> a;
  for(size_t i = 0; i < vector_length; i++){
    v1.push_back(offset+i*multiplication_factor);
    a.push_back(init_value);
  }
  Vector<DenseVector,float,ParMemoryBuffer> v =
    Vector<DenseVector,float,ParMemoryBuffer>::from_array(
      0,
      p,
      v1.data(),
      a.data(),
      v1.size());

  /////////////////////////////////////////////////////////////////////////////
  // Test Get
  /////////////////////////////////////////////////////////////////////////////
  test_get<DenseVector>(v);

  /////////////////////////////////////////////////////////////////////////////
  // Test Set
  /////////////////////////////////////////////////////////////////////////////
  test_set<DenseVector>(v);

  /////////////////////////////////////////////////////////////////////////////
  // Test Foreach 
  /////////////////////////////////////////////////////////////////////////////
  test_foreach<DenseVector>(v);

  /////////////////////////////////////////////////////////////////////////////
  // Test Par Foreach
  /////////////////////////////////////////////////////////////////////////////
  test_parforeach<DenseVector>(v);

  thread_pool::deleteThreadPool();
}

TEST_CASE( "Test basic SparseVector functionality.", "[SparseVector]" ) {
  thread_pool::initializeThreadPool();

  /////////////////////////////////////////////////////////////////////////////
  // Build a Vector
  /////////////////////////////////////////////////////////////////////////////
  ParMemoryBuffer *p = new ParMemoryBuffer(2);
  std::vector<uint32_t> v1;
  std::vector<float> a;
  for(size_t i = 0; i < vector_length; i++){
    v1.push_back(offset+i*multiplication_factor);
    a.push_back(init_value);
  }
  Vector<SparseVector,float,ParMemoryBuffer> v =
    Vector<SparseVector,float,ParMemoryBuffer>::from_array(
      0,
      p,
      v1.data(),
      a.data(),
      v1.size());

  /////////////////////////////////////////////////////////////////////////////
  // Test Get
  /////////////////////////////////////////////////////////////////////////////
  test_get<SparseVector>(v);

  /////////////////////////////////////////////////////////////////////////////
  // Test Set
  /////////////////////////////////////////////////////////////////////////////
  test_set<SparseVector>(v);

  /////////////////////////////////////////////////////////////////////////////
  // Test Foreach 
  /////////////////////////////////////////////////////////////////////////////
  test_foreach<SparseVector>(v);

  /////////////////////////////////////////////////////////////////////////////
  // Test Par Foreach
  /////////////////////////////////////////////////////////////////////////////
  test_parforeach<SparseVector>(v);

  thread_pool::deleteThreadPool();
}
*/

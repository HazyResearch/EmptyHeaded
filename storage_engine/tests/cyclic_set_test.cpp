#include "utils/utils.hpp"
#include "trie/set/Set.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <new>
#include <utility>
#include <vector>

namespace {

class TestSet {
 public:
  explicit TestSet(const std::vector<uint32_t>& input)
      : values(input), storage((sizeof(Set<hybrid>) + 8192 + 7) / 8, 0) {
    set = new (storage.data()) Set<hybrid>();
    set->from_array(set->get_data(), values.data(), values.size());
  }

  std::vector<uint32_t> values;
  std::vector<uint64_t> storage;
  Set<hybrid>* set;
};

void verify(const std::vector<uint32_t>& values, type::layout expected_type) {
  TestSet test_set(values);
  assert(test_set.set->type == expected_type);

  std::vector<std::vector<uint32_t> > by_thread(NUM_THREADS);
  test_set.set->par_foreach(
      par::schedule::cyclic_encoded_key,
      [&](size_t tid, uint32_t key) {
        assert(tid < NUM_THREADS);
        assert(key % NUM_THREADS == tid);
        by_thread[tid].push_back(key);
      });

  std::vector<uint32_t> observed;
  for(size_t tid = 0; tid < NUM_THREADS; tid++) {
    assert(std::is_sorted(by_thread[tid].begin(), by_thread[tid].end()));
    observed.insert(
        observed.end(), by_thread[tid].begin(), by_thread[tid].end());
  }
  std::sort(observed.begin(), observed.end());
  assert(observed == values);

  typedef std::pair<uint32_t, uint32_t> IndexedKey;
  std::vector<std::vector<IndexedKey> > indexed_by_thread(NUM_THREADS);
  test_set.set->par_foreach_index(
      par::schedule::cyclic_encoded_key,
      [&](size_t tid, uint32_t index, uint32_t key) {
        assert(tid < NUM_THREADS);
        assert(key % NUM_THREADS == tid);
        indexed_by_thread[tid].push_back(IndexedKey(index, key));
      });

  std::vector<IndexedKey> indexed;
  for(size_t tid = 0; tid < NUM_THREADS; tid++) {
    indexed.insert(indexed.end(),
                   indexed_by_thread[tid].begin(),
                   indexed_by_thread[tid].end());
  }
  std::sort(indexed.begin(), indexed.end());
  assert(indexed.size() == values.size());
  for(size_t i = 0; i < indexed.size(); i++) {
    assert(indexed[i].first == i);
    assert(indexed[i].second == values[i]);
  }
}

}  // namespace

int main() {
#if NUM_THREADS_IN > 1
  thread_pool::initializeThreadPool();
#endif

  // Empty, singleton, and sparse/gapped UINTEGER layouts.
  verify(std::vector<uint32_t>(), type::UINTEGER);
  verify(std::vector<uint32_t>{7}, type::UINTEGER);
  verify(std::vector<uint32_t>{3, 1024, 4099, 9001}, type::UINTEGER);

  // Dense RANGE_BITSET layouts: cardinality below T, a non-word-aligned
  // offset spanning several words, and the uint32_t upper boundary.
  verify(std::vector<uint32_t>{130, 131, 132}, type::RANGE_BITSET);
  verify(std::vector<uint32_t>{130, 131, 133, 134, 139, 145, 159,
                               160, 161, 190, 191, 192, 195},
         type::RANGE_BITSET);
  verify(std::vector<uint32_t>{UINT32_MAX - 7, UINT32_MAX - 6,
                               UINT32_MAX - 4, UINT32_MAX - 1, UINT32_MAX},
         type::RANGE_BITSET);

  std::cout << "cyclic_set_test passed with threads=" << NUM_THREADS << '\n';
  return 0;
}


#include "fft.hpp"
#include "utils/thread_pool.hpp"
#include "utils/parallel.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/timer.hpp"
#include "utils/ParMemoryBuffer.hpp"
#include "Encoding.hpp"
#include "intermediate/EncodedColumnStore.hpp"
#include "trie/TrieBlock.hpp"

#include <math.h>
#include <cmath>

#include "ffthelper.hpp"

void Query_0::run_0() {
  thread_pool::initializeThreadPool();
  const int base = 2;
  const int pow = 2;
  const uint32_t n = 4;

  auto fft_timer = timer::start_clock();

  Trie<C, ParMemoryBuffer> bag_i = getInput(base, pow, n);

  // Build factors
  std::vector<std::vector<Trie<C, ParMemoryBuffer>>> trie_factors(pow);
  for (int j = 0; j < pow; j++) {
    for (int k = 0; k < pow - j; k++) {
      auto t = getFactorTrie(base, pow, j, k);
      trie_factors[j].push_back(t);
    }
  }

  Trie<C, ParMemoryBuffer> bag_0(
      "/Users/egan/Documents/Projects/EmptyHeaded/examples/fft/"
      "db/relations/bag_0",
      pow, true);
  {
    ParTrieBuilder<C, ParMemoryBuffer> builders_0(&bag_0, pow + 1);

    const ParTrieIterator<C, ParMemoryBuffer> iterators_bag_i(&bag_i);

    const ParTrieIterator<C, ParMemoryBuffer> iterators_0_1(
        &trie_factors[0][1]);

    builders_0.build_set(iterators_0_1.head);
    builders_0.allocate_next();
    builders_0.par_foreach_builder([&](const size_t tid, const uint32_t x0_i,
                                       const uint32_t x0_d) {

      TrieBuilder<C, ParMemoryBuffer> &builder_0 = *builders_0.builders.at(tid);

      TrieIterator<C, ParMemoryBuffer> &iterator_bag_i =
          *iterators_bag_i.iterators.at(tid);

      TrieIterator<C, ParMemoryBuffer> &iterator_0_1 =
          *iterators_0_1.iterators.at(tid);

      iterator_0_1.get_next_block(0, x0_d);
      builder_0.build_set(tid, iterator_bag_i.get_block(0));
      builder_0.allocate_annotation(tid);
      builder_0.foreach_builder([&](const uint32_t y0_i, const uint32_t y0_d) {
        iterator_bag_i.get_next_block(0, y0_i, y0_d);
        C annot_sum(0.0);

        builder_0.build_aggregated_set(iterator_0_1.get_block(1),
                                       iterator_bag_i.get_block(1));
        builder_0.foreach_aggregate([&](const uint32_t y1_d) {
          annot_sum += iterator_bag_i.get_annotation(1, y1_d) *
                       iterator_0_1.get_annotation(1, y1_d);
        });
        builder_0.set_annotation(annot_sum, y0_i, y0_d);
      });
      builder_0.set_level(x0_i, x0_d);
    });
  }
  Trie<C, ParMemoryBuffer> bag_1(
      "/Users/egan/Documents/Projects/EmptyHeaded/examples/fft/"
      "db/relations/bag_1",
      pow, true);
  {
    ParTrieBuilder<C, ParMemoryBuffer> builders_1(&bag_1, pow + 1);

    const ParTrieIterator<C, ParMemoryBuffer> iterators_bag_0(&bag_0);

    const ParTrieIterator<C, ParMemoryBuffer> iterators_0_0(
        &trie_factors[0][0]);

    const ParTrieIterator<C, ParMemoryBuffer> iterators_1_0(
        &trie_factors[1][0]);

    builders_1.build_set(iterators_1_0.head);
    builders_1.allocate_next();
    builders_1.par_foreach_builder([&](const size_t tid, const uint32_t x1_i,
                                       const uint32_t x1_d) {

      TrieBuilder<C, ParMemoryBuffer> &builder_1 = *builders_1.builders.at(tid);

      TrieIterator<C, ParMemoryBuffer> &iterator_bag_0 =
          *iterators_bag_0.iterators.at(tid);

      TrieIterator<C, ParMemoryBuffer> &iterator_0_0 =
          *iterators_0_0.iterators.at(tid);

      TrieIterator<C, ParMemoryBuffer> &iterator_1_0 =
          *iterators_1_0.iterators.at(tid);

      iterator_1_0.get_next_block(0, x1_d);
      builder_1.build_set(tid, iterator_bag_0.get_block(0));
      builder_1.allocate_annotation(tid);
      builder_1.foreach_builder([&](const uint32_t x0_i, const uint32_t x0_d) {
        iterator_0_0.get_next_block(0, x0_d);
        iterator_bag_0.get_next_block(0, x0_i, x0_d);
        C annot_sum(0.0);

        builder_1.build_aggregated_set(iterator_0_0.get_block(1),
                                       iterator_bag_0.get_block(1));
        builder_1.foreach_aggregate([&](const uint32_t y0_d) {
          annot_sum += iterator_bag_0.get_annotation(1, y0_d) *
                       iterator_0_0.get_annotation(1, y0_d) *
                       iterator_1_0.get_annotation(1, y0_d);
        });
        builder_1.set_annotation(annot_sum, x0_i, x0_d);
      });
      builder_1.set_level(x1_i, x1_d);
    });
    bag_1.printN(5);
  }
  timer::stop_clock("FFT TIME", fft_timer);

  thread_pool::deleteThreadPool();
}

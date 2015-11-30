headers = """
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
"""


def prologue(b, m, n):
    return """
void Query_0::run_0() {{
  thread_pool::initializeThreadPool();
  const int base = {b};
  const int pow = {m};
  const uint32_t n = {n};
""".format(
        b=b,
        m=m,
        n=n,
    )


epilogue = """
  thread_pool::deleteThreadPool();
}
"""

setup = """
  Trie<C, ParMemoryBuffer> bag_i = getInput(base, pow, n);

  // Build factors
  std::vector<std::vector<Trie<C, ParMemoryBuffer>>> trie_factors (pow);
  for (int j = 0; j < pow; j++) {
    for (int k = 0; k < pow - j; k++) {
      auto t = getFactorTrie(base, pow, j, k);
      trie_factors[j].push_back(t);
    }
  }
"""


def declare_bag(num):
    return """
    Trie<C, ParMemoryBuffer> bag_{num}(
          "/Users/egan/Documents/Projects/EmptyHeaded/examples/fft/"
              "db/relations/bag_{num}",
          pow,
          true);
    """.format(
        num=num,
    )


def declare_p_builder(num):
    return """
    ParTrieBuilder<C, ParMemoryBuffer> builders_{num}(&bag_{num}, pow + 1);
    """.format(
        num=num,
    )


def declare_p_bag_iterator(num):
    return """
    const ParTrieIterator<C, ParMemoryBuffer> iterators_bag_{num}(&bag_{num});
    """.format(
        num=num
    )


def declare_p_factor_iterator(xi, yi):
    return """
    const ParTrieIterator<C, ParMemoryBuffer> iterators_{xi}_{yi}(&trie_factors[{xi}][{yi}]);
    """.format(
        xi=xi,
        yi=yi,
    )


def build_base(bag_num, m):
    return """
    builders_{num}.build_set(iterators_{num}_{yi}.head);
    builders_{num}.allocate_next();
    builders_{num}.par_foreach_builder([&](const size_t tid, const uint32_t x{num}_i,
                                        const uint32_t x{num}_d) {{
    """.format(
        num=bag_num,
        yi=m - bag_num - 1,
    )


def declare_builder(num):
    return """
    TrieBuilder<C, ParMemoryBuffer> &builder_{num} = *builders_{num}.builders.at(tid);
    """.format(
        num=num,
    )


def declare_bag_iterator(num):
    return """
    TrieIterator<C, ParMemoryBuffer> &iterator_bag_{num} = *iterators_bag_{num}.iterators.at(tid);
    """.format(
        num=num
    )


def declare_factor_iterator(xi, yi):
    return """
    TrieIterator<C, ParMemoryBuffer> &iterator_{xi}_{yi} = *iterators_{xi}_{yi}.iterators.at(tid);
    """.format(
        xi=xi,
        yi=yi,
    )


def factor_next_block(xi, yi):
    return """iterator_{xi}_{yi}.get_next_block(0, x{xi}_d);
    """.format(
        xi=xi,
        yi=yi,
    ).strip()


def bag_next_block(bag_num, level, var):
    return """
    iterator_bag_{num}.get_next_block({level}, {var}_i, {var}_d);
    """.format(
        num=bag_num,
        level=level,
        var=var,
    ).strip()


def bag_build_set(bag_num, prev_bag_num, level):
    return """
    builder_{bag_num}.build_set(tid, iterator_bag_{prev_bag_num}.get_block({level}));
    """.format(
        bag_num=bag_num,
        prev_bag_num=prev_bag_num,
        level=level,
    ).strip()


def bag_allocate_next(bag_num):
    return """
    builder_{bag_num}.allocate_next(tid);
    """.format(
        bag_num=bag_num,
    ).strip()


def bag_allocate_annot(bag_num):
    return """
    builder_{bag_num}.allocate_annotation(tid);
    """.format(
        bag_num=bag_num
    ).strip()


def bag_foreach(bag_num, var):
    return """
    builder_{b}.foreach_builder([&](const uint32_t {var}_i, const uint32_t {var}_d) {{
    """.format(
        b=bag_num,
        var=var,
    ).strip()


def bag_set_level(bag_num, var):
    return """
    builder_{b}.set_level({var}_i, {var}_d);
    """.format(
        b=bag_num,
        var=var,
    ).strip()


def bag_set_annot(bag_num, var):
    return """
    builder_{b}.set_annotation(annot_sum, {var}_i, {var}_d);
    """.format(
        b=bag_num,
        var=var,
    ).strip()


declare_annot = 'C annot_sum(0.0);'


def factor_get_annot_expr(ix, agg_index):
    return "iterator_{ix}_{agg}.get_annotation(1, y{agg}_d)".format(
        ix=ix,
        agg=agg_index,
    )


def loop_aggregation(bag_num, prev_bag_num, agg_index, m):
    # build_aggregated_set still doesn't work for 1 set

    # code_block = """
    # builder_{b}.build_aggregated_set(iterator_bag_{pb}.get_block({lvl}));
    # builder_{b}.foreach_aggregate([&](const uint32_t y{agg}_d) {{
    #     annot_sum += iterator_bag_{pb}.get_annotation({lvl}, y{agg}_d) *
    # """.format(
    code_block = """
    builder_{b}.build_aggregated_set(iterator_0_{agg}.get_block(1), iterator_bag_{pb}.get_block({lvl}));
    builder_{b}.foreach_aggregate([&](const uint32_t y{agg}_d) {{
        annot_sum += iterator_bag_{pb}.get_annotation({lvl}, y{agg}_d) *
    """.format(
        b=bag_num,
        pb=prev_bag_num,
        agg=agg_index,
        lvl=m - 1,
    )

    for ix in range(m - agg_index):
        code_block += factor_get_annot_expr(ix, agg_index)
        if ix < m - agg_index - 1:
            code_block += "*"
        else:
            code_block += ";"

    code_block += "});"
    return code_block


def bag_print(bag_num):
    return "bag_{bag_num}.printN(5);".format(bag_num=bag_num)


start_timer = """
auto fft_timer = timer::start_clock();
"""

end_timer = """
timer::stop_clock("FFT TIME", fft_timer);
"""

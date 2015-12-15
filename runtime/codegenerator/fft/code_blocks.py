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


def build_base(bag_num):
    return """
    builders_{num}.build_set(iterators_{num}_0.head);
    builders_{num}.allocate_next();
    builders_{num}.par_foreach_builder([&](const size_t tid, const uint32_t x{num}_i,
                                        const uint32_t x{num}_d) {{
    """.format(
        num=bag_num,
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
    return """iterator_{xi}_{yi}.get_next_block(0, x{xi}_i, x{xi}_d);
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



def factor_get_annot_expr(ix, iy):
    return "iterator_{ix}_{iy}.get_annotation(1, y{iy}_d)".format(
        ix=ix,
        iy=iy,
    ).strip()


def declare_annot(i):
    if i == 0:
        return """
        C annot_{i}(1.0);
        """.format(i=i).strip()
    else:
        return """
        C annot_{i}(annot_{pi});
        """.format(
            i=i,
            pi=i-1,
        ).strip()


def declare_annots(num):
    return """
    C annot[{num}];
    C annot_f;
    """.format(
        num=num
    ).strip()

def mult_factor_annot(ix, iy):
    factor = factor_get_annot_expr(ix, iy)
    if iy == 0:
        return """
        annot[{i}] = {factor};
        """.format(
            i=iy,
            factor=factor
        ).strip()
    else:
        return """
        annot[{iy}] = annot[{pi}] * {factor};
        """.format(
            iy=iy,
            pi=iy-1,
            factor=factor,
        ).strip()


zero_sum_annot = "annot_f = 0.0;"


def mult_annot_bag(iy, prev_bag, m):
    return """
    annot_f += annot[{i}] * iterator_bag_{pb}.get_annotation({lvl}, y{i}_d);
    """.format(
        i=iy,
        pb=prev_bag,
        lvl=m-1,
    ).strip()


def bag_build_agg_set(bag_num, prev_bag_num, level):
    return """
    builder_{b}.build_aggregated_set(iterator_{b}_0.get_block(1), iterator_bag_{prev_bag_num}.get_block({level}));
    """.format(
        b=bag_num,
        prev_bag_num=prev_bag_num,
        level=level,
    ).strip()


def bag_agg_foreach(bag_num, var):
    return """
    builder_{b}.foreach_aggregate([&](const uint32_t {var}_d) {{
    """.format(
        b=bag_num,
        var=var,
    ).strip()


def bag_set_annot(bag_num, var):
    return """
    builder_{b}.set_annotation(annot_f, {var}_i, {var}_d);
    """.format(
        b=bag_num,
        var=var,
    ).strip()


def bag_print(bag_num):
    return "bag_{bag_num}.printN(5);".format(bag_num=bag_num)


def comment_var(var):
    return """
    // {var}
    """.format(
        var=var
    )


def start_timer(name):
    return """
    auto {name}_timer = timer::start_clock();
    """.format(
        name=name,
    ).strip()


def end_timer(name):
    return """
    timer::stop_clock("{name} TIME", {name}_timer);
    """.format(
        name=name,
    ).strip()

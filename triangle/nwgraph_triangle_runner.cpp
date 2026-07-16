#include "nwgraph/adjacency.hpp"
#include "nwgraph/algorithms/triangle_count.hpp"
#include "nwgraph/edge_list.hpp"

#include <omp.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

#ifdef NWGRAPH_TRIANGLE_U32
using VertexId = std::uint32_t;
using Index = std::uint32_t;
constexpr int kGraphIdBits = 32;
#else
using VertexId = std::uint64_t;
using Index = std::uint64_t;
constexpr int kGraphIdBits = 64;
#endif

using EdgeList = nw::graph::index_edge_list<
    VertexId, nw::graph::unipartite_graph_base,
    nw::graph::directedness::directed>;
using Graph = nw::graph::index_adjacency<0, Index, VertexId>;

double seconds_since(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
      .count();
}

// Run a literal cyclic parallelization of NWGraph's supplied sequential loop
// on OpenMP's reusable worker team. The team is initialized explicitly before
// graph loading, so the measured boundary matches EmptyHeaded's generated BAG
// timer without relying on in-process query warmups.
// Unlike NWGraph's stock parallel overload, this intentionally intersects the
// complete source adjacency rather than starting at the current-edge suffix.
std::uint64_t triangle_count_cyclic_full(const Graph& graph,
                                         std::size_t threads) {
  const auto first = graph.begin();
  const auto last = graph.end();
  std::uint64_t triangles = 0;
#pragma omp parallel num_threads(threads) reduction(+ : triangles)
  {
    const auto tid = static_cast<std::size_t>(omp_get_thread_num());
    for (auto source = first + tid; source < last; source += threads) {
      for (auto edge = (*source).begin(), end = (*source).end(); edge != end;
           ++edge) {
        triangles += nw::graph::intersection_size(
            *source, first[nw::graph::target(graph, *edge)]);
      }
    }
  }
  return triangles;
}

// Establish the OpenMP team before the query timer starts.  This makes a
// one-query-per-process trial comparable to EmptyHeaded, whose worker pool is
// also initialized outside its generated BAG timer.
void initialize_openmp_team(std::size_t threads) {
  omp_set_dynamic(0);
  std::size_t participants = 0;
  std::size_t thread_id_sum = 0;
#pragma omp parallel num_threads(threads) reduction(+ : participants, thread_id_sum)
  {
    ++participants;
    thread_id_sum += static_cast<std::size_t>(omp_get_thread_num());
  }
  const auto expected_sum = threads * (threads - 1) / 2;
  if (participants != threads || thread_id_sum != expected_sum) {
    throw std::runtime_error("OpenMP did not create the requested worker team");
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 7 && argc != 8) {
      std::cerr << "usage: " << argv[0]
                << " EDGES.tsv VERTICES THREADS WARMUPS TRIALS EXPECTED"
                   " [async|cyclic-full|sequential]\n";
      return 2;
    }
    const auto vertices = std::stoull(argv[2]);
    const auto threads = std::stoull(argv[3]);
    const auto warmups = std::stoull(argv[4]);
    const auto trials = std::stoull(argv[5]);
    const auto expected = std::stoull(argv[6]);
    const std::string mode = argc == 8 ? argv[7] : "async";
    if (mode != "async" && mode != "cyclic-full" && mode != "sequential") {
      throw std::runtime_error(
          "mode must be async, cyclic-full, or sequential");
    }
    if (threads == 0 || trials == 0) {
      throw std::runtime_error("threads and trials must be positive");
    }

    // EmptyHeaded creates its worker pool before loading the persisted trie.
    // Do the same here so team initialization is untimed and does not disturb
    // the freshly prepared adjacency immediately before its first query.
    if (mode == "cyclic-full") initialize_openmp_team(threads);

    auto load_start = std::chrono::steady_clock::now();
    std::ifstream input(argv[1]);
    if (!input) throw std::runtime_error("could not open edge TSV");
    EdgeList edges(vertices);
    std::uint64_t source = 0;
    std::uint64_t target = 0;
    std::size_t rows = 0;
    while (input >> source >> target) {
      if (!(source < target && target < vertices)) {
        throw std::runtime_error("input is not strictly upper-oriented");
      }
      edges.push_back(source, target);
      ++rows;
    }
    if (!input.eof()) throw std::runtime_error("malformed edge TSV");
    edges.close_for_push_back();
    const auto load_seconds = seconds_since(load_start);

    auto prepare_start = std::chrono::steady_clock::now();
    Graph graph(edges, true, std::execution::seq);
    const auto prepare_seconds = seconds_since(prepare_start);

    auto run_once = [&]() {
      auto start = std::chrono::steady_clock::now();
      const auto count = mode == "sequential"
                             ? nw::graph::triangle_count(graph)
                         : mode == "cyclic-full"
                             ? triangle_count_cyclic_full(graph, threads)
                             : nw::graph::triangle_count(graph, threads);
      return std::pair<double, std::uint64_t>{seconds_since(start), count};
    };

    for (std::size_t trial = 0; trial < warmups; ++trial) {
      const auto [elapsed, count] = run_once();
      if (count != expected) throw std::runtime_error("warmup count mismatch");
      std::cout << "NWGRAPH_TRIANGLE_WARMUP index=" << trial
                << " seconds=" << std::setprecision(12) << elapsed
                << " count=" << count << '\n';
    }

    std::vector<double> samples;
    samples.reserve(trials);
    for (std::size_t trial = 0; trial < trials; ++trial) {
      const auto [elapsed, count] = run_once();
      if (count != expected) throw std::runtime_error("trial count mismatch");
      samples.push_back(elapsed);
      std::cout << "NWGRAPH_TRIANGLE_TRIAL index=" << trial
                << " seconds=" << std::setprecision(12) << elapsed
                << " count=" << count << '\n';
    }
    auto sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    const auto median = sorted[sorted.size() / 2];
    std::cout << "NWGRAPH_TRIANGLE_METRICS {\"schema_version\":1"
              << ",\"graph_id_bits\":" << kGraphIdBits
              << ",\"threads\":" << threads
              << ",\"mode\":\"" << mode << "\""
              << ",\"vertices\":" << vertices
              << ",\"edges\":" << rows
              << ",\"count\":" << expected
              << ",\"load_seconds\":" << std::setprecision(12)
              << load_seconds
              << ",\"prepare_seconds\":" << prepare_seconds
              << ",\"execute_min_seconds\":" << sorted.front()
              << ",\"execute_median_seconds\":" << median
              << ",\"execute_max_seconds\":" << sorted.back()
              << "}\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "nwgraph_triangle_runner: " << error.what() << '\n';
    return 1;
  }
}

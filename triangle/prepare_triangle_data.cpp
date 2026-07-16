#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

constexpr char kMagic[8] = {'H', 'O', 'P', 'Q', 'C', 'S', 'R', '\0'};

struct DataHeader {
  char magic[8];
  std::uint32_t index_bits;
  std::uint32_t version;
  std::uint64_t vertices;
  std::uint64_t forward_edges;
  std::uint64_t reverse_edges;
  std::uint64_t source_edge_bytes;
  std::uint64_t source_vertex_bytes;
};

template <class Index>
struct AdjacencyEntry {
  Index neighbor;
  std::int32_t timestamp;
};

using Edge = std::pair<std::uint64_t, std::uint64_t>;

template <class T>
void read_exact(std::ifstream& input, T* data, std::size_t count,
                const char* what) {
  const auto bytes = static_cast<std::streamsize>(sizeof(T) * count);
  if (!input.read(reinterpret_cast<char*>(data), bytes)) {
    throw std::runtime_error(std::string("short read while reading ") + what);
  }
}

template <class Index>
std::vector<Edge> read_projected_edges(std::ifstream& input,
                                       const DataHeader& header) {
  std::vector<Index> row(static_cast<std::size_t>(header.vertices + 1));
  std::vector<AdjacencyEntry<Index>> adjacency(
      static_cast<std::size_t>(header.forward_edges));
  read_exact(input, row.data(), row.size(), "forward row offsets");
  read_exact(input, adjacency.data(), adjacency.size(), "forward adjacency");

  if (row.empty() || row.front() != 0 ||
      static_cast<std::uint64_t>(row.back()) != header.forward_edges) {
    throw std::runtime_error("invalid forward row offsets");
  }

  std::vector<Edge> edges;
  edges.reserve(adjacency.size());
  for (std::uint64_t source = 0; source < header.vertices; ++source) {
    const auto begin = static_cast<std::uint64_t>(row[source]);
    const auto end = static_cast<std::uint64_t>(row[source + 1]);
    if (begin > end || end > header.forward_edges) {
      throw std::runtime_error("non-monotone forward row offsets");
    }
    for (std::uint64_t position = begin; position < end; ++position) {
      const auto target =
          static_cast<std::uint64_t>(adjacency[position].neighbor);
      if (target >= header.vertices) {
        throw std::runtime_error("forward neighbor is out of range");
      }
      if (source == target) continue;
      edges.emplace_back(std::min(source, target), std::max(source, target));
    }
  }
  std::sort(edges.begin(), edges.end());
  edges.erase(std::unique(edges.begin(), edges.end()), edges.end());
  return edges;
}

std::uint64_t triangle_count(std::uint64_t vertices,
                             const std::vector<Edge>& edges) {
  std::vector<std::vector<std::uint64_t>> outgoing(
      static_cast<std::size_t>(vertices));
  for (const auto& [source, target] : edges) outgoing[source].push_back(target);

  std::uint64_t triangles = 0;
  for (std::uint64_t source = 0; source < vertices; ++source) {
    const auto& left = outgoing[source];
    for (const auto middle : left) {
      const auto& right = outgoing[middle];
      auto i = left.begin();
      auto j = right.begin();
      while (i != left.end() && j != right.end()) {
        if (*i < *j) {
          ++i;
        } else if (*j < *i) {
          ++j;
        } else {
          ++triangles;
          ++i;
          ++j;
        }
      }
    }
  }
  return triangles;
}

void write_outputs(const std::filesystem::path& output,
                   std::uint64_t vertices, const std::vector<Edge>& edges,
                   std::uint64_t raw_edges, std::uint64_t triangles) {
  std::filesystem::create_directories(output);

  std::ofstream tsv(output / "edges.tsv");
  std::ofstream matrix(output / "graph.mtx");
  std::ofstream manifest(output / "manifest.json");
  if (!tsv || !matrix || !manifest) {
    throw std::runtime_error("could not create output files");
  }

  matrix << "%%MatrixMarket matrix coordinate pattern symmetric\n"
         << "% stable ascending-degree relabel; one upper edge per pair\n"
         << vertices << ' ' << vertices << ' ' << edges.size() << '\n';
  for (const auto& [source, target] : edges) {
    tsv << source << '\t' << target << '\n';
    matrix << (source + 1) << ' ' << (target + 1) << '\n';
  }
  manifest << "{\n"
           << "  \"schema_version\": 1,\n"
           << "  \"vertices\": " << vertices << ",\n"
           << "  \"raw_directed_events\": " << raw_edges << ",\n"
           << "  \"undirected_simple_edges\": " << edges.size() << ",\n"
           << "  \"triangles\": " << triangles << ",\n"
           << "  \"id_type\": \"uint64\",\n"
           << "  \"relabel\": \"stable-ascending-undirected-degree\",\n"
           << "  \"orientation\": \"lower-new-id-to-higher-new-id\"\n"
           << "}\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 3) {
      std::cerr << "usage: " << argv[0] << " INPUT.csr64 OUTPUT_DIRECTORY\n";
      return 2;
    }

    std::ifstream input(argv[1], std::ios::binary);
    if (!input) throw std::runtime_error("could not open input");

    DataHeader header{};
    read_exact(input, &header, 1, "header");
    if (std::memcmp(header.magic, kMagic, 7) != 0 || header.version != 1 ||
        (header.index_bits != 32 && header.index_bits != 64)) {
      throw std::runtime_error("unsupported HOPQCSR header");
    }

    auto edges = header.index_bits == 32
                     ? read_projected_edges<std::uint32_t>(input, header)
                     : read_projected_edges<std::uint64_t>(input, header);

    std::vector<std::uint64_t> degree(
        static_cast<std::size_t>(header.vertices), 0);
    for (const auto& [source, target] : edges) {
      ++degree[source];
      ++degree[target];
    }
    std::vector<std::uint64_t> order(
        static_cast<std::size_t>(header.vertices));
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](auto lhs, auto rhs) {
      if (degree[lhs] != degree[rhs]) return degree[lhs] < degree[rhs];
      return lhs < rhs;
    });
    std::vector<std::uint64_t> new_id(order.size());
    for (std::uint64_t id = 0; id < header.vertices; ++id) {
      new_id[order[id]] = id;
    }

    for (auto& [source, target] : edges) {
      source = new_id[source];
      target = new_id[target];
      if (target < source) std::swap(source, target);
    }
    std::sort(edges.begin(), edges.end());

    const auto triangles = triangle_count(header.vertices, edges);
    write_outputs(argv[2], header.vertices, edges, header.forward_edges,
                  triangles);
    std::cout << "TRIANGLE_DATA vertices=" << header.vertices
              << " raw_directed_events=" << header.forward_edges
              << " undirected_simple_edges=" << edges.size()
              << " triangles=" << triangles << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "prepare_triangle_data: " << error.what() << '\n';
    return 1;
  }
}

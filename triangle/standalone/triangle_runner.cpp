#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>

#include "run_0.hpp"

int main(int argc, char **argv) {
  const std::uint64_t expected =
      argc == 2 ? std::strtoull(argv[1], NULL, 10)
                : static_cast<std::uint64_t>(EXPECTED_TRIANGLE_COUNT);

  mymap output_tries;
  const auto start = std::chrono::steady_clock::now();
  const long result = run_0(&output_tries);
  const auto stop = std::chrono::steady_clock::now();
  const double wall_seconds =
      std::chrono::duration<double>(stop - start).count();

  std::cout << "EH_RESULT triangles=" << result << " expected=" << expected
            << " wall_seconds=" << std::fixed << std::setprecision(9)
            << wall_seconds << std::endl;
  if (result < 0 || static_cast<std::uint64_t>(result) != expected) {
    std::cerr << "EH_VALIDATION_FAILURE expected=" << expected
              << " actual=" << result << std::endl;
    return 2;
  }
  std::cout << "EH_VALIDATION_OK" << std::endl;
  return 0;
}

#include "utils/io.hpp"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <limits>

int main() {
  const std::uint64_t above_u32 = (std::uint64_t{1} << 40) + 17;
  assert(utils::from_string<std::uint64_t>("1099511627793") == above_u32);
  assert(utils::from_string<std::uint64_t>("18446744073709551615") ==
         std::numeric_limits<std::uint64_t>::max());
  assert(utils::from_string<std::int64_t>("-1099511627793") ==
         -static_cast<std::int64_t>(above_u32));

  std::cout << "u64_io_test passed\n";
  return 0;
}

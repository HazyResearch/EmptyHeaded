#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
CXX=${CXX:-c++}

build_and_run() {
  local threads=$1
  local output="${TMPDIR:-/tmp}/emptyheaded-cyclic-set-test-t${threads}"
  "$CXX" \
    -std=c++11 -O2 -Wall -Wextra -mavx2 -mpopcnt -pthread \
    -DNUM_THREADS_IN="$threads" \
    -I"$ROOT/include" \
    "$ROOT/tests/cyclic_set_test.cpp" \
    "$ROOT/src/utils/parallel.cpp" \
    "$ROOT/src/utils/thread_pool.cpp" \
    -o "$output"
  "$output"
}

build_and_run 1
build_and_run 16

u64_output="${TMPDIR:-/tmp}/emptyheaded-u64-io-test"
"$CXX" \
  -std=c++11 -O2 -Wall -Wextra \
  -I"$ROOT/include" \
  "$ROOT/tests/u64_io_test.cpp" \
  -o "$u64_output"
"$u64_output"

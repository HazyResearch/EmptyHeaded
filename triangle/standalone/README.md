# Native EmptyHeaded triangle runner

This target compiles a generated EmptyHeaded plan and the historical
storage-engine sources directly into one optimized native executable. It is
the linkage used for the recorded `dv3` measurements. It deliberately avoids
the obsolete prebuilt `libemptyheaded`, TBB 4, and jemalloc in the historical
Docker image.

Docker is needed only to run EmptyHeaded's historical compiler and database
builder. Build and run this target on the native benchmark host. See the
complete command sequence in [`../RUNBOOK.md`](../RUNBOOK.md).

## Timed boundary

`Time[BAG TriangleCount TIME]` is the query-kernel metric. It starts after
worker-pool creation, trie loading, and encoding loading and stops before
result printing. The runner's `wall_seconds` includes setup performed inside
`run_0`; it is retained only as an end-to-end diagnostic.

`run_trials.sh RUNNER 2 7` starts two warmup processes and then seven measured
processes. Each process runs and validates exactly one query.

## Persisted layout and runtime workers

The final comparison uses a one-persisted-partition database (`gNN-t1`) for
both the one-worker and 16-worker executables. The persisted layout and runtime
worker count need not match: `Trie::getHead()` uses the number of buffers stored
in the loaded trie, while `NUM_THREADS_IN` controls the runtime worker pool.
The 16-worker-over-T1 combination was correctness-validated in every warmup
and measured run at scales 16, 18, and 20.

For the parallel comparison, configure with
`-DEMPTYHEADED_CYCLIC_OUTER=ON`. This selects the generic
`cyclic_encoded_key` outer traversal: dense encoded key `x` is owned by worker
`x % T`. It partitions source-row work; it does not repartition the persisted
database. EmptyHeaded's historical adaptive scheduler remains the default.

## ID widths

The `Edge` schema and input labels use external `uint64_t` values. EmptyHeaded
dictionary-encodes them to dense `uint32_t` hot join keys. The original label
magnitude is therefore not truncated, but a database is limited to at most
`UINT32_MAX` distinct encoded values. The benchmark itself uses labels below
`2^20`; arbitrary high-magnitude U64 labels were not benchmarked.

## Build examples using the same T1 database

One worker:

```sh
cmake -S triangle/standalone -B build/eh-g16-t1 \
  -DCMAKE_BUILD_TYPE=Release \
  -DEMPTYHEADED_GENERATED_DIR="$PWD/triangle/generated/g16-t1" \
  -DEMPTYHEADED_DATABASE_ROOT="$PWD/triangle/generated/g16-t1/database" \
  -DEMPTYHEADED_NUM_THREADS=1 \
  -DEXPECTED_TRIANGLE_COUNT=435586 \
  -DEMPTYHEADED_TBB_INCLUDE_DIR=/path/to/oneapi-tbb/include \
  -DEMPTYHEADED_TBB_LIBRARY_DIR=/path/to/oneapi-tbb/lib
cmake --build build/eh-g16-t1 --parallel 16
```

Sixteen cyclic workers over that same persisted database:

```sh
cmake -S triangle/standalone -B build/eh-g16-t1db-t16 \
  -DCMAKE_BUILD_TYPE=Release \
  -DEMPTYHEADED_GENERATED_DIR="$PWD/triangle/generated/g16-t1" \
  -DEMPTYHEADED_DATABASE_ROOT="$PWD/triangle/generated/g16-t1/database" \
  -DEMPTYHEADED_NUM_THREADS=16 \
  -DEMPTYHEADED_CYCLIC_OUTER=ON \
  -DEXPECTED_TRIANGLE_COUNT=435586 \
  -DEMPTYHEADED_TBB_INCLUDE_DIR=/path/to/oneapi-tbb/include \
  -DEMPTYHEADED_TBB_LIBRARY_DIR=/path/to/oneapi-tbb/lib
cmake --build build/eh-g16-t1db-t16 --parallel 16
```

Run two process warmups and seven measured processes:

```sh
triangle/standalone/run_trials.sh \
  build/eh-g16-t1db-t16/eh_triangle_runner 2 7
```

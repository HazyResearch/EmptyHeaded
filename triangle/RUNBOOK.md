# Reproducing the EmptyHeaded triangle experiment

This runbook reproduces the data, generated EmptyHeaded plan, native binaries,
and the primary measurements in [`results/dv3`](results/dv3). Docker is used
only for EmptyHeaded's historical Scala/Python/Cython build environment. The
generated C++ is compiled and timed natively on `dv3`; no timed query runs in a
container.

The query is:

```text
TriangleCount(;count) :-
    Edge(i,j), Edge(j,k), Edge(i,k),
    count:long <- [COUNT(*)].
```

`Edge` contains each undirected simple edge once, oriented from the lower to
the higher relabeled ID. A match therefore has `i < j < k` and counts one
undirected triangle.

## Recorded provenance

The final experiment used:

| Component | Recorded version |
|---|---|
| EmptyHeaded base | `f1fdb409225940b64d536e001d3831313532e537` |
| HOP/Graph500 generator | `8e086b1d64ce74a5ca58aa0f2e393416f084458e` |
| NWGraph | `4932de9500cf5edef04be7862dc98a2fd9755fca` |
| EmptyHeaded Docker image | `craberger/emptyheaded:latest` |
| Immutable Docker image ID | `sha256:cfae046d778839e569aaedab0164a3e727df98ad9e4bb96f55bd8017c7746575` |
| Docker image platform | Linux `amd64`, created 2017-03-15 |
| Native host | `dv3`, Ubuntu Linux 6.8, x86-64 |
| Native compiler | GCC/G++ 13.3.0 |
| Native CMake | 3.28.3 |
| oneTBB headers | 2023.0.0 |

The `latest` tag is mutable, so use the immutable image ID when exact
reproduction matters. The EmptyHeaded revision in this branch includes the
generic terminal-count optimization, cyclic outer scheduler, 64-bit external
schema support, and persisted-partition loading fix described in
[`REPORT.md`](REPORT.md).

The recorded native directory was:

```sh
export EXP=/home/lums/eh-triangle-20260715
export EH="$EXP/src"
export HOP=/path/to/hop-query
export NW=/path/to/NWGraph
mkdir -p "$EXP/bin" "$EXP/build" "$EXP/data" "$EXP/raw" "$EXP/results"
```

Using the same `EXP` path is necessary only to reproduce the recorded binary
hashes because the portable generated header is compiled with the database
root as a string literal. It is not required for equivalent results.

Before building, verify the three source revisions:

```sh
git -C "$EH" rev-parse HEAD
git -C "$HOP" rev-parse HEAD
git -C "$NW" rev-parse HEAD
```

The EmptyHeaded line may show a descendant of the base revision after the
experiment changes are committed; the base named above must still be an
ancestor.

## 1. Generate and prepare the graphs

Build the HOP stochastic-Kronecker generator and the deterministic triangle
preparation tool natively:

```sh
cmake -S "$HOP" -B "$HOP/build" -DCMAKE_BUILD_TYPE=Release
cmake --build "$HOP/build" --target skg_gen --parallel 16

/usr/bin/g++-13 -O3 -DNDEBUG -march=native -mtune=native -std=c++17 \
  "$EH/triangle/prepare_triangle_data.cpp" \
  -o "$EXP/bin/prepare_triangle_data"
```

Generate scales 16, 18, and 20. The generator's fixed seed is 41. Parameters
`A=0.45`, `B=C=0.20`, and `D=0.15` give the recorded Graph500-style
stochastic-Kronecker distribution. `Vattr=2` and `Eattr=1` reproduce the HOP
input format; triangle preparation intentionally projects those properties
away.

```sh
for SCALE in 16 18 20; do
  "$HOP/build/skg_gen" "$SCALE" 16 0.45 0.20 2 1 \
    "$EXP/raw/g${SCALE}.csr64" --threads 16 --width 64
  "$EXP/bin/prepare_triangle_data" \
    "$EXP/raw/g${SCALE}.csr64" "$EXP/data/g${SCALE}"
done
```

Preparation performs four deterministic transformations:

1. Project every directed event `(u,v)` to the undirected pair
   `(min(u,v),max(u,v))`.
2. Remove self-loops and duplicate pairs.
3. Relabel vertices by ascending undirected degree, breaking ties by old ID.
4. Orient each relabeled edge once from its lower to its higher endpoint.

Validate the manifests and exact `edges.tsv` inputs:

| Scale | Vertices | Raw events | Simple edges | Triangles | `edges.tsv` SHA-256 |
|---:|---:|---:|---:|---:|---|
| 16 | 65,536 | 1,048,576 | 1,042,561 | 435,586 | `e5e448f8363b88020881de12f192e61a0a44ed8144e6ce2b98ed80f02b3e3a5e` |
| 18 | 262,144 | 4,194,304 | 4,185,064 | 845,171 | `4f4290779606543f98b16495d387ea12df11d2c7280e7b3995b0b9a503d2e328` |
| 20 | 1,048,576 | 16,777,216 | 16,763,097 | 1,583,446 | `494062435d8852ede738dbdd9bbfe87edec65fe0116c1e0074b4a204cc8d4ccc` |

```sh
sha256sum "$EXP"/data/g*/edges.tsv
for SCALE in 16 18 20; do
  cat "$EXP/data/g${SCALE}/manifest.json"
done
```

Both engines consume these same upper-oriented TSV relations. NWGraph stores
row offsets and neighbor IDs as `uint64_t`. EmptyHeaded accepts external
`uint64_t` labels and dictionary-encodes them to dense `uint32_t` hot keys.
Consequently, EmptyHeaded permits U64 label values but the database may contain
at most `UINT32_MAX` distinct encoded values. These benchmark labels are below
`2^20`; arbitrary high-magnitude U64 labels were not part of this measurement.

## 2. Build the compiler and one-partition databases in Docker

Perform this stage on a machine that can run the recorded Linux `amd64` image.
Set paths on that machine:

```sh
export EH_HOST=/absolute/path/to/this/EmptyHeaded/checkout
export DATA_HOST=/absolute/path/to/eh-triangle-data
export EH_IMAGE=sha256:cfae046d778839e569aaedab0164a3e727df98ad9e4bb96f55bd8017c7746575
```

`DATA_HOST` must contain `g16`, `g18`, and `g20` directories produced by step
1. If the immutable image ID is not already present, first run `docker pull
craberger/emptyheaded:latest`, verify that the pulled image has the ID above,
and then use the ID. Package the Scala compiler and build the historical
storage library with one persisted partition:

```sh
docker run --rm \
  -e EMPTYHEADED_HOME=/work \
  -v "$EH_HOST:/work" \
  -w /work \
  "$EH_IMAGE" \
  bash -lc '
    cd /work/query_compiler
    sbt pack
    mkdir -p /work/storage_engine/build-t1
    cd /work/storage_engine/build-t1
    cmake -DNUM_THREADS=1 \
      -DCMAKE_C_COMPILER=gcc-5 \
      -DCMAKE_CXX_COMPILER=g++-5 ..
    make -j16
  '
```

For each scale, generate the schema and query, compile the generated database
builder, and execute that builder. The relation filename in the generator is
`/input/edges.tsv`, hence the per-scale read-only mount:

```sh
for SCALE in 16 18 20; do
  docker run --rm \
    -e EMPTYHEADED_HOME=/work \
    -e EMPTYHEADED_STORAGE_LIBRARY_DIR=/work/storage_engine/build-t1/lib \
    -v "$EH_HOST:/work" \
    -v "$DATA_HOST/g${SCALE}:/input:ro" \
    -w /work \
    "$EH_IMAGE" \
    bash -lc "
      python triangle/generate_emptyheaded_triangle.py g${SCALE} 1
      cd triangle/generated/g${SCALE}-t1/database/libs/createDB
      ./build.sh
      python -c 'import DFMap_triangle; DFMap_triangle.DFMap_triangle([])'
    "
done
```

Each output is `triangle/generated/gNN-t1/`. It contains:

- `database/relations` and `database/encodings`: the one-persisted-partition
  physical database;
- `run_0.generated.hpp`: the exact compiler output with its original absolute
  paths;
- `run_0.hpp`: the path-portable generated plan used by the native runner; and
- `triangle_count.dl`: the input query.

The portable optimized headers for all recorded scales have SHA-256:

```text
adf12e499d637d1a45210684aced4ba28725a3ae0f23e9f01bb8c8c8fd466f0b
```

### Regenerate only the query

After changing the Scala query generator, preserve the expensive database and
replace only the generated query wrapper/header:

```sh
for SCALE in 16 18 20; do
  docker run --rm \
    -e EMPTYHEADED_HOME=/work \
    -v "$EH_HOST:/work" \
    -w /work \
    "$EH_IMAGE" \
    python triangle/generate_emptyheaded_triangle.py \
      "g${SCALE}" 1 --query-only
done
```

`--query-only` requires the existing `gNN-t1/database`, removes only
`database/libs/triangle_query`, and leaves persisted relations and encodings
untouched.

## 3. Transfer artifacts and build natively on `dv3`

Transfer the source tree, including the ignored generated database payloads,
to `$EH` on `dv3`. For example, from the Docker host:

```sh
rsync -a --checksum "$EH_HOST/" \
  dv3:/home/lums/eh-triangle-20260715/src/
```

The recorded builds used oneTBB 2023.0.0 headers at
`$EXP/deps/tbb/include` and `libtbb.so.12` at:

```text
/opt/intelFPGA_pro/24.3/qprogrammer/quartus/linux64
```

Use the installed oneTBB include/library paths on another machine, but keep
them ABI-compatible.

### EmptyHeaded native builds

The final comparison deliberately uses the same one-persisted-partition
database for both runtime modes. `Trie::getHead()` reads the number of
persisted buffers from the trie, rather than assuming it equals
`NUM_THREADS_IN`. Thus a 16-worker runtime may read a one-partition database;
the runtime partitions outer-domain work, not the stored graph. This was
correctness-validated in every warmup and measured run at scales 16, 18, and
20.

Build the scale-16 sequential executable:

```sh
cmake -S "$EH/triangle/standalone" \
  -B "$EXP/build/eh-countset-g16-t1" \
  -DCMAKE_BUILD_TYPE=Release \
  -DEMPTYHEADED_ROOT="$EH" \
  -DEMPTYHEADED_GENERATED_DIR="$EH/triangle/generated/g16-t1" \
  -DEMPTYHEADED_DATABASE_ROOT="$EH/triangle/generated/g16-t1/database" \
  -DEMPTYHEADED_NUM_THREADS=1 \
  -DEMPTYHEADED_CYCLIC_OUTER=OFF \
  -DEXPECTED_TRIANGLE_COUNT=435586 \
  -DEMPTYHEADED_TBB_INCLUDE_DIR="$EXP/deps/tbb/include" \
  -DEMPTYHEADED_TBB_LIBRARY_DIR=/opt/intelFPGA_pro/24.3/qprogrammer/quartus/linux64
cmake --build "$EXP/build/eh-countset-g16-t1" --parallel 16
```

Repeat for scale 18 by replacing `g16` with `g18` and the expected count with
`845171`; name the build directory `eh-countset-g18-t1`.

Build the 16-worker cyclic scale-16 executable against that same `g16-t1`
database:

```sh
cmake -S "$EH/triangle/standalone" \
  -B "$EXP/build/eh-countset-g16-t1db-t16-cyclic" \
  -DCMAKE_BUILD_TYPE=Release \
  -DEMPTYHEADED_ROOT="$EH" \
  -DEMPTYHEADED_GENERATED_DIR="$EH/triangle/generated/g16-t1" \
  -DEMPTYHEADED_DATABASE_ROOT="$EH/triangle/generated/g16-t1/database" \
  -DEMPTYHEADED_NUM_THREADS=16 \
  -DEMPTYHEADED_CYCLIC_OUTER=ON \
  -DEXPECTED_TRIANGLE_COUNT=435586 \
  -DEMPTYHEADED_TBB_INCLUDE_DIR="$EXP/deps/tbb/include" \
  -DEMPTYHEADED_TBB_LIBRARY_DIR=/opt/intelFPGA_pro/24.3/qprogrammer/quartus/linux64
cmake --build "$EXP/build/eh-countset-g16-t1db-t16-cyclic" --parallel 16
```

Repeat that command for scales 18 and 20, using build directories
`eh-countset-g18-t1db-t16-cyclic` and
`eh-countset-g20-t1db-t16-cyclic` and expected counts `845171` and `1583446`.

The CMake target compiles the generated header and historical storage sources
directly into one native executable with `-O3 -DNDEBUG -mavx2 -march=native
-mtune=native -fno-strict-aliasing -fwrapv`. It does not link the obsolete
container-built EmptyHeaded library.

Recorded binary hashes, which also depend on the exact absolute paths and
toolchain, were:

```text
0d6d7019f31043b3b11ae4e31aec3a6da75e174239a6eb762b2f74a78a28e3a4  eh-countset-g16-t1/eh_triangle_runner
9bb6b5ed6071f2c402fc97b58095be2970546d5d80ca7b5b577827a5a2fcf141  eh-countset-g18-t1/eh_triangle_runner
0617b64d72caa92844056ed0705d4bf60470fa7be35c55e8c50932b7074e6932  eh-countset-g16-t1db-t16-cyclic/eh_triangle_runner
985ca08ecc4ca45649f82a7c9a65378fe4a82ac31c3e6b91b6574cc872f02b53  eh-countset-g18-t1db-t16-cyclic/eh_triangle_runner
dc462a795875ea8d411148b77b384f7401d58273363c0a8e3d9bae30e024dcad  eh-countset-g20-t1db-t16-cyclic/eh_triangle_runner
```

### NWGraph native build

Build the U64 NWGraph comparison runner outside Docker:

```sh
/usr/bin/g++-13 -O3 -DNDEBUG -march=native -mtune=native \
  -std=c++20 -fopenmp \
  -I"$NW/include" \
  -I"$EXP/deps/tbb/include" \
  "$EH/triangle/nwgraph_triangle_runner.cpp" \
  -L/opt/intelFPGA_pro/24.3/qprogrammer/quartus/linux64 \
  -Wl,-rpath,/opt/intelFPGA_pro/24.3/qprogrammer/quartus/linux64 \
  -ltbb -pthread \
  -o "$EXP/bin/nwgraph_triangle_runner_u64"
```

Do not define `NWGRAPH_TRIANGLE_U32`: the primary runner uses U64 row indices
and vertex IDs. The recorded executable SHA-256 was:

```text
fbefb3a240cceec35f88ba7379e10dd2cb853c62a0cac759187fa164e6517f01
```

## 4. Run the primary fresh-process measurements

Run each command to completion before starting the next. Do not put any run in
the background and do not run queries concurrently: each 16-worker query owns
all selected cores.

The common protocol is two whole-process warmups followed by seven measured
whole-process invocations. Each invocation performs exactly one timed query.
Both implementations initialize their worker team outside the query timer.

- EmptyHeaded's metric is `Time[BAG TriangleCount TIME]`.
- NWGraph's metric is `NWGRAPH_TRIANGLE_TRIAL ... seconds=...`.
- Both use `std::chrono::steady_clock`.
- EmptyHeaded trie/encoding loading and NWGraph TSV/adjacency construction are
  outside their respective query timers.
- EmptyHeaded's `wall_seconds`, NWGraph's `load_seconds`, and NWGraph's
  `prepare_seconds` are end-to-end diagnostics, not primary metrics.

### Sequential, CPU 0 and NUMA node 0

```sh
set -o pipefail

numactl --physcpubind=0 --membind=0 \
  "$EH/triangle/standalone/run_trials.sh" \
  "$EXP/build/eh-countset-g16-t1/eh_triangle_runner" 2 7 \
  | tee "$EXP/results/emptyheaded-countset-sequential-g16.txt"

numactl --physcpubind=0 --membind=0 \
  "$EH/triangle/standalone/run_nw_fresh_trials.sh" \
  "$EXP/bin/nwgraph_triangle_runner_u64" \
  "$EXP/data/g16/edges.tsv" 65536 1 435586 sequential 2 7 \
  | tee "$EXP/results/nwgraph-fresh-sequential-g16.txt"
```

Repeat with:

| Scale | EmptyHeaded build | Vertices | Expected count |
|---:|---|---:|---:|
| 18 | `eh-countset-g18-t1` | 262144 | 845171 |

Scale 20 was not part of the recorded sequential primary table.

### Cyclic, 16 cores on NUMA node 0

EmptyHeaded pins runtime worker `t` to allowed CPU `t`; its generated outer
loop assigns dense encoded key `x` to worker `x mod 16`. The `numactl` binding
confines the process and its memory to the recorded socket:

```sh
set -o pipefail

numactl --physcpubind=0-15 --membind=0 \
  "$EH/triangle/standalone/run_trials.sh" \
  "$EXP/build/eh-countset-g16-t1db-t16-cyclic/eh_triangle_runner" 2 7 \
  | tee "$EXP/results/emptyheaded-countset-cyclic-g16-t1db.txt"
```

NWGraph uses the same complete-list intersection as its supplied sequential
loop, with source row `x` assigned to OpenMP worker `x mod 16`. Its worker team
is explicitly established before graph loading and before the query timer.
The primary runs use libgomp's default wait policy; make sure an inherited
`OMP_WAIT_POLICY` does not silently select the passive diagnostic case:

```sh
set -o pipefail

env -u OMP_WAIT_POLICY \
  OMP_PROC_BIND=close OMP_PLACES=cores \
  numactl --physcpubind=0-15 --membind=0 \
  "$EH/triangle/standalone/run_nw_fresh_trials.sh" \
  "$EXP/bin/nwgraph_triangle_runner_u64" \
  "$EXP/data/g16/edges.tsv" 65536 16 435586 cyclic-full 2 7 \
  | tee "$EXP/results/nwgraph-fresh-defaultwait-cyclic-full-g16.txt"
```

Repeat both commands with:

| Scale | EmptyHeaded build | Vertices | Expected count |
|---:|---|---:|---:|
| 18 | `eh-countset-g18-t1db-t16-cyclic` | 262144 | 845171 |
| 20 | `eh-countset-g20-t1db-t16-cyclic` | 1048576 | 1583446 |

The graph is degree-relabeled before either engine sees it. “Cyclic” refers to
source-row scheduling, not a different stored graph or a 16-way persisted
physical partition. NWGraph schedules external relabeled source IDs;
EmptyHeaded schedules dense dictionary keys. Isolated vertices are absent from
the EmptyHeaded dictionary, so the numeric worker label can be rotated after
that omitted prefix, but the same logical rows run on the same 16 homogeneous
cores.

## 5. Run the terminal-materialization controls

The preserved
`triangle/generated/baseline-materialized/run_0.hpp` predates the generic
terminal `count_set` optimization. Ignoring formatting, its only semantic
difference from the optimized header is that the final intersection uses
`build_aggregated_set`. It therefore isolates the effect of materializing the
terminal set.

Build controls against the same per-scale one-partition databases. This helper
produces both runtime modes without changing the preserved header:

```sh
build_materialized() {
  SCALE=$1
  EXPECTED=$2
  THREADS=$3
  CYCLIC=$4
  BUILD_NAME=$5

  cmake -S "$EH/triangle/standalone" -B "$EXP/build/$BUILD_NAME" \
    -DCMAKE_BUILD_TYPE=Release \
    -DEMPTYHEADED_ROOT="$EH" \
    -DEMPTYHEADED_GENERATED_DIR="$EH/triangle/generated/baseline-materialized" \
    -DEMPTYHEADED_DATABASE_ROOT="$EH/triangle/generated/g${SCALE}-t1/database" \
    -DEMPTYHEADED_NUM_THREADS="$THREADS" \
    -DEMPTYHEADED_CYCLIC_OUTER="$CYCLIC" \
    -DEXPECTED_TRIANGLE_COUNT="$EXPECTED" \
    -DEMPTYHEADED_TBB_INCLUDE_DIR="$EXP/deps/tbb/include" \
    -DEMPTYHEADED_TBB_LIBRARY_DIR=/opt/intelFPGA_pro/24.3/qprogrammer/quartus/linux64
  cmake --build "$EXP/build/$BUILD_NAME" --parallel 16
}

build_materialized 16 435586 1 OFF eh-materialized-g16-t1
build_materialized 18 845171 1 OFF eh-materialized-g18-t1
build_materialized 16 435586 16 ON eh-materialized-g16-t1db-t16-cyclic
build_materialized 18 845171 16 ON eh-materialized-g18-t1db-t16-cyclic
build_materialized 20 1583446 16 ON eh-materialized-g20-t1db-t16-cyclic
```

Run the two sequential controls on CPU 0 and the three cyclic controls on CPUs
0--15, one command at a time. These are again fresh-process 2+7 runs:

```sh
set -o pipefail

numactl --physcpubind=0 --membind=0 \
  "$EH/triangle/standalone/run_trials.sh" \
  "$EXP/build/eh-materialized-g16-t1/eh_triangle_runner" 2 7 \
  | tee "$EXP/results/emptyheaded-sequential-g16.txt"

numactl --physcpubind=0 --membind=0 \
  "$EH/triangle/standalone/run_trials.sh" \
  "$EXP/build/eh-materialized-g18-t1/eh_triangle_runner" 2 7 \
  | tee "$EXP/results/emptyheaded-sequential-g18.txt"

numactl --physcpubind=0-15 --membind=0 \
  "$EH/triangle/standalone/run_trials.sh" \
  "$EXP/build/eh-materialized-g16-t1db-t16-cyclic/eh_triangle_runner" 2 7 \
  | tee "$EXP/results/emptyheaded-cyclic-g16-t1db.txt"

numactl --physcpubind=0-15 --membind=0 \
  "$EH/triangle/standalone/run_trials.sh" \
  "$EXP/build/eh-materialized-g18-t1db-t16-cyclic/eh_triangle_runner" 2 7 \
  | tee "$EXP/results/emptyheaded-cyclic-g18-t1db.txt"

numactl --physcpubind=0-15 --membind=0 \
  "$EH/triangle/standalone/run_trials.sh" \
  "$EXP/build/eh-materialized-g20-t1db-t16-cyclic/eh_triangle_runner" 2 7 \
  | tee "$EXP/results/emptyheaded-cyclic-g20-t1db-baseline-materialized.txt"
```

## 6. Validate and summarize

Validate the committed recorded logs and their committed summary without
replacing any files:

```sh
python3 triangle/summarize_triangle_results.py --check
```

The checker requires exactly two warmup and seven measured process markers,
validates every returned triangle count, recomputes min/median/max from the raw
query-kernel samples, and verifies that the result is byte-for-byte identical
to [`results/dv3/summary.json`](results/dv3/summary.json).

Fresh timing values will naturally differ from the recorded values, so do not
run `--check` after copying fresh logs over the committed archive. To validate
and summarize a fresh full run, use a separate clean checkout, copy all fresh
primary and materialization-control logs into its `triangle/results/dv3` under
the cataloged names, and run without `--check`:

```sh
export FRESH_CHECKOUT=/path/to/separate/EmptyHeaded/checkout
cp -f "$EXP/results/"*.txt \
  "$FRESH_CHECKOUT/triangle/results/dv3/"
cd "$FRESH_CHECKOUT"
python3 triangle/summarize_triangle_results.py \
  > "$EXP/results/summary-fresh.json"
```

Running without `--check` still validates the invocation counts and returned
triangle counts before emitting newly computed statistics. Inspect the fresh
summary and compare it with the recorded summary; replacing the committed
archive should be a separate, intentional action.

The primary, control, and diagnostic log families are cataloged in
[`results/dv3/README.md`](results/dv3/README.md).

# Generated EmptyHeaded triangle counting versus NWGraph

## Result

With one generic compiler optimization, EmptyHeaded is now in the same
performance class as the supplied NWGraph triangle loop. EmptyHeaded is faster
in both sequential experiments and in the scale-16 parallel experiment, is
within 4% at scale 18, and takes 19% more time at scale 20. The generated query
is a Generic-Join plan; it is not a hash join, a meet-in-the-middle (MiM)
plan, an `m x n` Cartesian-product search, or a triangle-specific routine.

The compiler change recognizes a terminal join level whose values are needed
only for their cardinality and emits `count_set`. It contains no test for the
triangle query, `Edge`, or any query name.

The following are the primary measurements on `dv3`. Each entry is the median
of seven independent, one-query processes after two discarded warmup
processes. Only the query kernel is timed. Brackets contain the minimum and
maximum of the seven measured processes; `EH / NW < 1` means EmptyHeaded was
faster.

| Mode | Scale | EmptyHeaded | NWGraph | EH / NW |
|---|---:|---:|---:|---:|
| sequential | 16 | 90.236 ms [89.854, 92.516] | 156.273 ms [155.766, 156.479] | 0.577 |
| sequential | 18 | 483.381 ms [480.591, 490.059] | 735.091 ms [734.181, 738.095] | 0.658 |
| cyclic, 16 workers | 16 | 12.264 ms [9.994, 15.146] | 16.042 ms [15.741, 16.548] | 0.764 |
| cyclic, 16 workers | 18 | 65.850 ms [63.774, 80.911] | 63.418 ms [63.095, 64.340] | 1.038 |
| cyclic, 16 workers | 20 | 327.136 ms [323.439, 330.367] | 275.056 ms [270.015, 279.301] | 1.189 |

Equivalently, EmptyHeaded is 1.73x and 1.52x faster sequentially at scales 16
and 18. With 16 workers it is 1.31x faster at scale 16, near parity at scale
18, and 1.19x slower at scale 20. The few-millisecond scale-16 parallel run is
useful as an absolute-latency observation, not as a basis for a strong-scaling
claim. Scale 20 is the most informative remaining parallel gap.

Every warmup and measured process returned the independently computed expected
triangle count. Machine-readable values and the raw logs are in
[`results/dv3`](results/dv3), with the primary values in
[`summary.json`](results/dv3/summary.json).

## Query and graph convention

A relational join combines rows that agree on variables with the same name.
Here `Edge(x,y)` is a two-column relation containing each undirected simple
edge once. The endpoints are ordered so that `x < y`; define the forward
neighbor set

```text
N+(x) = { y : Edge(x,y) }.
```

EmptyHeaded receives this ordinary Datalog aggregate:

```text
TriangleCount(;count) :-
    Edge(i,j), Edge(j,k), Edge(i,k),
    count:long <- [COUNT(*)].
```

A result tuple must contain all three edges `(i,j)`, `(j,k)`, and `(i,k)`.
Because the relation is upper-oriented, every result automatically satisfies
`i < j < k`, so each undirected triangle is counted exactly once. This is the
same mathematical computation as the supplied NWGraph loop: for each oriented
edge `(i,j)`, add the number of common forward neighbors,
`|N+(i) intersect N+(j)|`.

## The generated Generic-Join algorithm

Generic Join binds variables one at a time. At each variable, it intersects
the values allowed by all relation atoms whose earlier variables have already
been fixed. It does not first construct a potentially large pairwise join and
then join that intermediate with the third relation.

For variable order `i,j,k`, EmptyHeaded derives this plan:

```text
I = sources(Edge(i,k)) intersect sources(Edge(i,j))
for i in I:
    J = sources(Edge(j,k)) intersect N+(i)
    for j in J:
        count += |N+(i) intersect N+(j)|
```

The root intersection finds possible values of `i`. After fixing `i`, the
second intersection finds values of `j` that are both forward neighbors of
`i` and sources of an outgoing edge. After fixing `i` and `j`, the final
intersection finds possible `k` values. Only its size is needed by
`COUNT(*)`.

The important part of the generated C++, with declarations and multiplicative
annotation bookkeeping elided, is:

```cpp
const size_t count_i = Builders.build_aggregated_set(
    Iterators_Edge_i_k.head, Iterators_Edge_i_j.head);

Builders.par_foreach_aggregate([&](const size_t tid, const uint32_t i_d) {
  // Select both Edge tries at the chosen i.
  Iterator_Edge_i_k->get_next_block(0, i_d);
  Iterator_Edge_i_j->get_next_block(0, i_d);

  // Materialize J because its j values are enumerated by the next loop.
  const size_t count_j = Builder->build_aggregated_set(
      Iterator_Edge_j_k->get_block(0),
      Iterator_Edge_i_j->get_block(1));

  Builder->foreach_aggregate([&](const uint32_t j_d) {
    Iterator_Edge_j_k->get_next_block(0, j_d);

    // K is terminal: compute its cardinality without materializing its keys.
    const size_t count_k = Builder->count_set(
        Iterator_Edge_i_k->get_block(1),
        Iterator_Edge_j_k->get_block(1));
    annotation_j += count_k;
  });
});
```

The exact generated output is
[`generated/g16-t1/run_0.generated.hpp`](generated/g16-t1/run_0.generated.hpp).
[`generated/g16-t1/run_0.hpp`](generated/g16-t1/run_0.hpp) changes only the
database path and exposes the aggregate to the validating standalone runner.

EmptyHeaded's historical compiler calls this cyclic-GHD execution path NPRR.
For this plan, the operation is the attribute-at-a-time, multiway-intersection
scheme normally described as Generic Join. The innermost operation is a set
intersection cardinality, just as in NWGraph, although the two systems use
different set representations and runtimes.

## Generic terminal-cardinality code generation

Before this change, the generated terminal step materialized every matching
`k` in a temporary Builder set and then used the returned cardinality:

```cpp
const size_t count_k = Builder->build_aggregated_set(
    Iterator_Edge_i_k->get_block(1),
    Iterator_Edge_j_k->get_block(1));
annotation_j += count_k;
```

The preserved baseline output is
[`generated/baseline-materialized/run_0.generated.hpp`](generated/baseline-materialized/run_0.generated.hpp).
The optimized compiler instead emits:

```cpp
const size_t count_k = Builder->count_set(
    Iterator_Edge_i_k->get_block(1),
    Iterator_Edge_j_k->get_block(1));
annotation_j += count_k;
```

`count_set` runs the representation-appropriate intersection cardinality
kernel but does not write the matching keys to a temporary result set.

The code generator uses this path only when all of the following hold:

- the attribute is the terminal level of the generated plan;
- that level is not requested as materialized output;
- an aggregate is present;
- no selection predicate needs to inspect each matched value;
- there are one or two accessors, matching the available `count_set`
  overloads; and
- none of those accessors is annotated, because an annotated relation may
  require reading a value-specific annotation.

Other cases retain enumeration or materialization. In particular, the
intermediate set `J` above remains materialized because the generated code
must enumerate its `j` values, and terminal intersections with three or more
inputs remain on the existing path until there is an n-way count kernel.
This is a property of the logical/code-generation state, not a recognition of
triangle syntax. It therefore applies to other count-only joins with the same
terminal shape.

## Effect of avoiding terminal materialization

The controls below compare the old materializing output with `count_set` using
the same inputs, one-persisted-partition EmptyHeaded database, compilation
mode, binding, and fresh-process timing protocol as the primary runs.

| Mode | Scale | Materialize final set | Count only | Speedup |
|---|---:|---:|---:|---:|
| sequential | 16 | 101.555 ms | 90.236 ms | 1.125x |
| sequential | 18 | 529.936 ms | 483.381 ms | 1.096x |
| cyclic, 16 workers | 16 | 13.250 ms | 12.264 ms | 1.080x |
| cyclic, 16 workers | 18 | 77.689 ms | 65.850 ms | 1.180x |
| cyclic, 16 workers | 20 | 347.765 ms | 327.136 ms | 1.063x |

The generic change improves all five configurations, by 1.06x to 1.18x. It
removes real work but is not the whole query: the compiler still constructs
and enumerates `J`, maintains scalar-output machinery, and performs the root
self-intersection.

## Physical representations and identifier widths

NWGraph stores row offsets and vertex identifiers as `uint64_t` in its CSR.
EmptyHeaded accepts and preserves identifier values across the `uint64_t`
numeric range in its input schema and dictionary. Its hot trie keys and its
`num_distinct` field are `uint32_t`, so the current representation supports at
most `UINT32_MAX` distinct encoded values in a domain and now fails explicitly
rather than wrapping if that ceiling is exceeded. U64 labels must not be
confused with an unlimited number of distinct internal keys. These benchmark
labels are below `2^20` after relabeling: the measurements exercise the U64
typed path, but they are not an experiment with high-magnitude U64 labels.

EmptyHeaded chooses a sorted-integer or range-bitset representation for each
set. Integer intersections use the runtime's SIMD or galloping paths according
to relative cardinality; bitset intersections use word operations and
population counts. The optimized `count_set` dispatches to cardinality-only
variants of these kernels. NWGraph uses its sorted-list two-pointer
`intersection_size` on U64 CSR rows. These representation choices help explain
why the generated plan can beat the compact handwritten loop sequentially.

## How the parallel executions are alike and different

Both primary parallel programs use cyclic ownership of outer source rows:
worker `t` handles keys congruent to `t modulo 16`. Both consume the same
degree-relabeled edge relation, and neither run overlaps another timed query.
That makes their outer work-assignment rule comparable.

Historical EmptyHeaded uses adaptive outer scheduling. This branch adds the
generic, opt-in `cyclic_encoded_key` schedule selected by
`EMPTYHEADED_CYCLIC_OUTER`; adaptive scheduling remains the default. The
cyclic scheduler examines encoded keys, not query or relation names.

They do **not** execute through the same parallel runtime or physical graph
layout:

- NWGraph uses an OpenMP team and a U64 CSR. For every assigned source row
  `i`, it enumerates the full `N+(i)` and invokes the supplied two-list
  intersection for each `j`.
- EmptyHeaded uses its own pre-created worker pool, per-worker trie
  iterators/Builders/reducers, dense U32 dictionary keys, and hybrid
  integer/bitset blocks. It executes the generated `I`, `J`, and terminal
  cardinality steps.
- NWGraph assigns relabeled source ID `i` to worker `i mod 16`.
  EmptyHeaded assigns encoded source key `d` to worker `d mod 16`. Degree
  relabeling places isolated vertices first and those IDs do not occur in the
  Edge dictionary, so the two assignments differ only by a constant rotation
  of homogeneous worker labels on these inputs.

"Cyclic partitioning" here therefore means query-time row scheduling, not 16
separately persisted graph shards.

For both the one-worker and 16-worker EmptyHeaded measurements, the Edge trie
was constructed once with **one persisted partition**. The runtime fix makes
trie-head lookup use the number of buffers recorded in the database rather
than the query binary's worker count. A 16-worker query can consequently read
the identical T1 physical database and distribute its source keys at query
time. This removes physical-layout changes from the T1-versus-T16 comparison;
it does not make the parallel implementations identical. Both warmups and all
seven measured T16 processes at each of scales 16, 18, and 20 returned the
correct count while reading the corresponding T1 database.

## Apples-to-apples timing protocol

The primary protocol is deliberately one timed query per fresh process for
both systems:

1. Run two complete processes and discard their query measurements.
2. Run seven more complete processes, each executing exactly one timed query.
3. Validate the triangle count in every process and report the median, minimum,
   and maximum of those seven query measurements.

The discarded processes can establish operating-system page cache and machine
state, but there is no in-process query warmup hidden before a measured query.
Older repeated-query, same-process NWGraph logs are retained only as
diagnostics and are not mixed into the primary table.

The timed boundaries are:

- EmptyHeaded's generated `BAG TriangleCount TIME`, after worker-pool creation,
  trie loading, and encoding loading, and stopped before result printing;
- NWGraph's `run_once`, after its OpenMP team has been explicitly initialized,
  the TSV has been loaded, and the CSR has been built.

Both timers use `std::chrono::steady_clock`. Data generation, relabeling,
deduplication, database/CSR construction, code generation, compilation,
process startup, loading, printing, and validation are excluded.

Sequential processes are bound to physical CPU 0 and NUMA node 0. Parallel
processes are confined to physical CPUs 0--15 and memory on NUMA node 0.
NWGraph uses `OMP_PROC_BIND=close`, `OMP_PLACES=cores`, and the default libgomp
wait policy (`OMP_WAIT_POLICY` is not set). EmptyHeaded internally pins worker
ID `t` to CPU `t` within the same confinement. A passive-wait NWGraph run is
kept as a diagnostic, not used as a primary result.

The NWGraph parallel runner is a literal cyclic parallelization of the
supplied sequential loop and intersects the complete source adjacency. The
stock NWGraph parallel overload starts the first intersection at the current
edge's suffix. That valid upper-oriented-graph optimization is intentionally
excluded here because using it on only the parallel NWGraph side would change
both algorithm and threading at once.

## Input construction

The graph source is the HOP/Graph500 stochastic-Kronecker generator with edge
factor 16, `A=0.45`, `B=C=0.20`, seed 41, two generated vertex attributes, one
edge attribute, and 64-bit CSR output. Triangle preparation projects away the
attributes and applies these deterministic steps:

1. Map every directed event `(u,v)` to
   `(min(u,v), max(u,v))`.
2. Drop self-loops and duplicate pairs.
3. Relabel vertices by ascending undirected degree, breaking ties by old ID.
4. Store each relabeled pair once, from lower to higher new ID.

| Scale | Vertices | Raw events | Simple edges | Triangles |
|---:|---:|---:|---:|---:|
| 16 | 65,536 | 1,048,576 | 1,042,561 | 435,586 |
| 18 | 262,144 | 4,194,304 | 4,185,064 | 845,171 |
| 20 | 1,048,576 | 16,777,216 | 16,763,097 | 1,583,446 |

SHA-256 of the exact `edges.tsv` inputs:

```text
g16 e5e448f8363b88020881de12f192e61a0a44ed8144e6ce2b98ed80f02b3e3a5e
g18 4f4290779606543f98b16495d387ea12df11d2c7280e7b3995b0b9a503d2e328
g20 494062435d8852ede738dbdd9bbfe87edec65fe0116c1e0074b4a204cc8d4ccc
```

Both evaluators consume exactly these upper-oriented relations.

## What remains

The scale-20 16-worker result leaves a 19% gap. The generated code identifies
ordinary compiler/runtime work that can remove it in principle; there is no
algorithmic requirement that EmptyHeaded do more asymptotic work for this
query. The next changes should remain generic:

1. Recognize and remove the root intersection of a set with itself.
2. Fuse construction and traversal of an intermediate such as `J`, or provide
   a streaming intersection iterator, when values have a single consumer.
3. Remove `num_rows_reducer.update(tid, 1)` from each `j` iteration when the
   only requested output is one scalar aggregate.
4. Specialize away the output Trie, unused `count_i`/`count_j` values,
   intermediate multiplications by one, and annotation scaffolding for scalar
   count-only results.
5. Add a true n-way cardinality kernel so terminal intersections with more
   than two accessors can receive the same optimization.
6. Reduce worker dispatch/reducer overhead and measure per-worker intersection
   work before changing the generic cyclic schedule.

These are plan-shape and runtime optimizations, not a request to recognize
triangles. The current result already rules out the earlier concern that
EmptyHeaded is using MiM or enumerating an `m x n` Cartesian product: the
emitted algorithm is the expected sequence of constrained set intersections.

## Validation and provenance

- Host: `dv3`, Ubuntu Linux 6.8, x86-64.
- CPU: two AMD EPYC 74F3 sockets; runs use physical cores 0--15 on NUMA node 0.
- Compiler: GCC/G++ 13.3.0; CMake 3.28.3.
- EmptyHeaded base: `f1fdb409225940b64d536e001d3831313532e537`.
- NWGraph: `4932de9500cf5edef04be7862dc98a2fd9755fca`.
- HOP generator: `8e086b1d64ce74a5ca58aa0f2e393416f084458e`.
- Native flags: Release `-O3 -DNDEBUG`, `-march=native -mtune=native`;
  EmptyHeaded also uses `-mavx2 -fno-strict-aliasing -fwrapv`.
- The focused generator tests cover the positive cardinality-only case and
  materialized, selected, annotated, and three-input negative cases. The full
  Scala suite passes 65/65 tests.
- The generated standalone C++ compiles, the T1/T16 cyclic scheduler tests
  pass, and all measured counts match the independent reference counts above.
- Portable generated headers for every scale and runtime configuration are
  byte-identical, SHA-256
  `adf12e499d637d1a45210684aced4ba28725a3ae0f23e9f01bb8c8c8fd466f0b`;
  the optimized raw generated files across those configurations differ only
  in embedded database paths. The separately preserved materializing baseline
  is intentionally different.

Complete generation, native-build, pinning, and execution commands are in
[`RUNBOOK.md`](RUNBOOK.md).

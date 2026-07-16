# `dv3` triangle results

This directory contains raw query logs, data manifests, and the validated
machine-readable summary for the EmptyHeaded-versus-NWGraph triangle
experiment. The primary comparison is in [`summary.json`](summary.json); the
exact reproduction commands and timing boundary are in
[`../../RUNBOOK.md`](../../RUNBOOK.md).

All primary rows use two whole-process warmups followed by seven measured
whole-process invocations. Each invocation runs exactly one query. Every
result count is checked. Neither engine's data loading or physical
representation construction is included in the primary query-kernel metric.

## Primary logs

These are the only logs used for the main EmptyHeaded-versus-NWGraph rows in
`summary.json`:

| Mode | Scale | EmptyHeaded | NWGraph |
|---|---:|---|---|
| sequential | 16 | `emptyheaded-countset-sequential-g16.txt` | `nwgraph-fresh-sequential-g16.txt` |
| sequential | 18 | `emptyheaded-countset-sequential-g18.txt` | `nwgraph-fresh-sequential-g18.txt` |
| cyclic, 16 workers | 16 | `emptyheaded-countset-cyclic-g16-t1db.txt` | `nwgraph-fresh-defaultwait-cyclic-full-g16.txt` |
| cyclic, 16 workers | 18 | `emptyheaded-countset-cyclic-g18-t1db.txt` | `nwgraph-fresh-defaultwait-cyclic-full-g18.txt` |
| cyclic, 16 workers | 20 | `emptyheaded-countset-cyclic-g20-t1db.txt` | `nwgraph-fresh-defaultwait-cyclic-full-g20.txt` |

`countset` identifies the generic compiler optimization that counts the final
terminal intersection without materializing it. `t1db` identifies the common
one-persisted-partition EmptyHeaded database used by both runtime modes.
`fresh` identifies one query per new process. `defaultwait` states that no
`OMP_WAIT_POLICY` override was active; the OpenMP team was initialized before
loading and before the timed query.

The EmptyHeaded metric parsed from each invocation is:

```text
Time[BAG TriangleCount TIME]: ... s
```

The NWGraph metric is:

```text
NWGRAPH_TRIANGLE_TRIAL index=0 seconds=... count=...
```

EmptyHeaded's trie and encoding load lines and `wall_seconds` are diagnostic.
NWGraph's `load_seconds` and `prepare_seconds` are likewise diagnostic.

## Terminal-materialization controls

The `terminal_count_controls` section of `summary.json` compares the optimized
`countset` logs above with otherwise corresponding materializing generated
plans:

| Mode | Scale | Materializing control |
|---|---:|---|
| sequential | 16 | `emptyheaded-sequential-g16.txt` |
| sequential | 18 | `emptyheaded-sequential-g18.txt` |
| cyclic, 16 workers | 16 | `emptyheaded-cyclic-g16-t1db.txt` |
| cyclic, 16 workers | 18 | `emptyheaded-cyclic-g18-t1db.txt` |
| cyclic, 16 workers | 20 | `emptyheaded-cyclic-g20-t1db-baseline-materialized.txt` |

These controls isolate the final `build_aggregated_set` to `count_set`
code-generation change. They are not additional NWGraph comparison rows.

## Retained diagnostics

The following logs are intentionally retained but are not inputs to the
primary table:

- `nwgraph-fresh-cyclic-full-g16.txt` is the fresh-process
  `OMP_WAIT_POLICY=PASSIVE` diagnostic. The primary default-wait result is
  `nwgraph-fresh-defaultwait-cyclic-full-g16.txt`.
- `nwgraph-sequential-g16.txt`, `nwgraph-sequential-g18.txt`, and
  `nwgraph-cyclic-full-g{16,18,20}.txt` contain two warmups and seven queries
  in one process. They show NWGraph's in-process steady state, not the common
  fresh-process lifecycle.
- `emptyheaded-cyclic-g{16,18,20}.txt` are earlier materializing-plan runs over
  16-persisted-partition databases. The final primary comparison instead uses
  one persisted partition in both EmptyHeaded runtime modes.

The per-scale `gNN-manifest.json` files record graph cardinalities,
orientation, and relabeling. They are copies of the manifests emitted during
deterministic data preparation.

## Validation

From the repository root, validate all primary and materialization-control raw
logs and the committed summary:

```sh
python3 triangle/summarize_triangle_results.py --check
```

The checker requires exactly two warmup markers, seven measured markers, and
the correct count in every invocation before recomputing the statistics.
`--check` is specifically for the committed archive: newly measured times will
naturally differ. For a fresh full run, keep the committed directory intact,
put all fresh primary and materialization-control logs under the cataloged
names in a separate checkout, and run the summarizer without `--check` to emit
a new summary.

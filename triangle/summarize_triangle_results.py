#!/usr/bin/env python3
"""Validate and summarize the committed dv3 triangle query logs."""

import argparse
import json
import re
import statistics
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results" / "dv3"

GRAPHS = {
    16: {"vertices": 65536, "edges": 1042561, "triangles": 435586},
    18: {"vertices": 262144, "edges": 4185064, "triangles": 845171},
    20: {"vertices": 1048576, "edges": 16763097, "triangles": 1583446},
}

PRIMARY = [
    (16, "sequential", "emptyheaded-countset-sequential-g16.txt",
     "nwgraph-fresh-sequential-g16.txt"),
    (18, "sequential", "emptyheaded-countset-sequential-g18.txt",
     "nwgraph-fresh-sequential-g18.txt"),
    (16, "cyclic-16", "emptyheaded-countset-cyclic-g16-t1db.txt",
     "nwgraph-fresh-defaultwait-cyclic-full-g16.txt"),
    (18, "cyclic-16", "emptyheaded-countset-cyclic-g18-t1db.txt",
     "nwgraph-fresh-defaultwait-cyclic-full-g18.txt"),
    (20, "cyclic-16", "emptyheaded-countset-cyclic-g20-t1db.txt",
     "nwgraph-fresh-defaultwait-cyclic-full-g20.txt"),
]

MATERIALIZATION_CONTROLS = [
    (16, "sequential", "emptyheaded-sequential-g16.txt",
     "emptyheaded-countset-sequential-g16.txt"),
    (18, "sequential", "emptyheaded-sequential-g18.txt",
     "emptyheaded-countset-sequential-g18.txt"),
    (16, "cyclic-16", "emptyheaded-cyclic-g16-t1db.txt",
     "emptyheaded-countset-cyclic-g16-t1db.txt"),
    (18, "cyclic-16", "emptyheaded-cyclic-g18-t1db.txt",
     "emptyheaded-countset-cyclic-g18-t1db.txt"),
    (20, "cyclic-16", "emptyheaded-cyclic-g20-t1db-baseline-materialized.txt",
     "emptyheaded-countset-cyclic-g20-t1db.txt"),
]

INVOCATION = re.compile(
    r"(?P<implementation>EH|NWGRAPH)_INVOCATION "
    r"phase=(?P<phase>warmup|measured) iteration=(?P<iteration>\d+)")
EH_TIME = re.compile(r"Time\[BAG TriangleCount TIME\]: ([0-9.eE+-]+) s")
EH_COUNT = re.compile(r"EH_RESULT triangles=(\d+) expected=(\d+)")
NW_TIME = re.compile(
    r"NWGRAPH_TRIANGLE_TRIAL index=\d+ seconds=([0-9.eE+-]+) count=(\d+)")
EH_VALIDATION = re.compile(r"^EH_VALIDATION_OK$")


def parse(path, implementation, expected):
    expected_prefix = "EH" if implementation == "emptyheaded" else "NWGRAPH"
    records = []
    current = None

    def finish_invocation():
        nonlocal current
        if current is None:
            return
        required = ["seconds", "actual"]
        if implementation == "emptyheaded":
            required.append("validated")
        missing = [field for field in required if field not in current]
        if missing:
            raise ValueError(
                f"incomplete invocation in {path}: missing {', '.join(missing)}")
        records.append(current)
        current = None

    for line in path.read_text().splitlines():
        invocation = INVOCATION.search(line)
        if invocation:
            finish_invocation()
            if invocation.group("implementation") != expected_prefix:
                raise ValueError(f"unexpected implementation marker in {path}: {line}")
            current = {
                "phase": invocation.group("phase"),
                "iteration": int(invocation.group("iteration")),
            }
            continue

        if implementation == "emptyheaded":
            timing = EH_TIME.search(line)
            if timing:
                if current is None or "seconds" in current:
                    raise ValueError(f"misplaced or duplicate EH time in {path}: {line}")
                current["seconds"] = float(timing.group(1))
                continue
            count = EH_COUNT.search(line)
            if count:
                if current is None or "actual" in current:
                    raise ValueError(f"misplaced or duplicate EH result in {path}: {line}")
                actual, declared_expected = map(int, count.groups())
                if actual != expected or declared_expected != expected:
                    raise ValueError(f"incorrect result in {path}: {line}")
                current["actual"] = actual
                continue
            if EH_VALIDATION.match(line):
                if current is None or "validated" in current:
                    raise ValueError(
                        f"misplaced or duplicate EH validation in {path}: {line}")
                current["validated"] = True
        else:
            timing = NW_TIME.search(line)
            if timing:
                if current is None or "seconds" in current:
                    raise ValueError(f"misplaced or duplicate NW trial in {path}: {line}")
                seconds, actual = timing.groups()
                if int(actual) != expected:
                    raise ValueError(f"incorrect result in {path}: {line}")
                current["seconds"] = float(seconds)
                current["actual"] = int(actual)

    finish_invocation()

    warmups = [record for record in records if record["phase"] == "warmup"]
    measured = [record for record in records if record["phase"] == "measured"]
    if [record["iteration"] for record in warmups] != [1, 2]:
        raise ValueError(f"expected warmup iterations 1,2 in {path}")
    if [record["iteration"] for record in measured] != list(range(1, 8)):
        raise ValueError(f"expected measured iterations 1..7 in {path}")

    return [record["seconds"] for record in measured]


def metrics(samples):
    return {
        "min": min(samples),
        "median": statistics.median(samples),
        "max": max(samples),
    }


def build_summary():
    rows = []
    for scale, mode, eh_name, nw_name in PRIMARY:
        expected = GRAPHS[scale]["triangles"]
        eh = metrics(parse(RESULTS / eh_name, "emptyheaded", expected))
        nw = metrics(parse(RESULTS / nw_name, "nwgraph", expected))
        rows.append({
            "scale": scale,
            **GRAPHS[scale],
            "mode": mode,
            "emptyheaded": eh,
            "nwgraph": nw,
            "emptyheaded_over_nwgraph": eh["median"] / nw["median"],
        })

    controls = []
    for scale, mode, materialized_name, count_name in MATERIALIZATION_CONTROLS:
        expected = GRAPHS[scale]["triangles"]
        materialized = metrics(parse(
            RESULTS / materialized_name, "emptyheaded", expected))
        count_only = metrics(parse(
            RESULTS / count_name, "emptyheaded", expected))
        controls.append({
            "scale": scale,
            "mode": mode,
            "materialized_median": materialized["median"],
            "count_only_median": count_only["median"],
            "speedup": materialized["median"] / count_only["median"],
        })

    return {
        "schema_version": 2,
        "host": "dv3",
        "threads_parallel": 16,
        "warmup_processes": 2,
        "measured_processes": 7,
        "metric": "query_kernel_seconds",
        "emptyheaded_physical_layout": "one-persisted-partition",
        "rows": rows,
        "terminal_count_controls": controls,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check", action="store_true",
        help="fail unless summary.json is byte-for-byte canonical")
    args = parser.parse_args()
    rendered = json.dumps(build_summary(), indent=2) + "\n"
    if args.check:
        committed = (RESULTS / "summary.json").read_text()
        if committed != rendered:
            raise SystemExit(
                "summary.json differs from validated raw logs; regenerate it")
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()

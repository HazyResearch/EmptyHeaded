#!/usr/bin/env bash
set -euo pipefail

if (( $# < 6 || $# > 8 )); then
  echo "usage: run_nw_fresh_trials.sh RUNNER EDGES VERTICES THREADS EXPECTED MODE [WARMUPS] [TRIALS]" >&2
  exit 2
fi

runner=$1
edges=$2
vertices=$3
threads=$4
expected=$5
mode=$6
warmups=${7:-2}
trials=${8:-7}

run_one() {
  local phase=$1
  local iteration=$2
  echo "NWGRAPH_INVOCATION phase=${phase} iteration=${iteration}"
  "${runner}" "${edges}" "${vertices}" "${threads}" 0 1 "${expected}" "${mode}"
}

for ((i = 1; i <= warmups; ++i)); do
  run_one warmup "${i}"
done

for ((i = 1; i <= trials; ++i)); do
  run_one measured "${i}"
done

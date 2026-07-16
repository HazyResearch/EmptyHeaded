#!/usr/bin/env bash
set -euo pipefail

runner=${1:?usage: run_trials.sh RUNNER [WARMUPS] [TRIALS]}
warmups=${2:-2}
trials=${3:-7}

for ((i = 1; i <= warmups; ++i)); do
  echo "EH_INVOCATION phase=warmup iteration=${i}"
  "${runner}"
done

for ((i = 1; i <= trials; ++i)); do
  echo "EH_INVOCATION phase=measured iteration=${i}"
  "${runner}"
done

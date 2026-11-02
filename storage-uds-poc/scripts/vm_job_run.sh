#!/bin/bash
# VM job: run the benchmark matrix runner fetched from the ops bucket.
#   PHASE: smoke | full ; ITER: iterations per cell for the full phase.
set -uo pipefail
OPS="${OPS:-kalragauri-uds-poc-ops}"
gcloud storage cp --quiet "gs://$OPS/scripts/vm_run.sh" "$HOME/vm_run.sh"
PHASE="${PHASE:-smoke}" ITER="${ITER:-200}" OPS="$OPS" bash "$HOME/vm_run.sh"

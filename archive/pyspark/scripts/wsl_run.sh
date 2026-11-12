#!/usr/bin/env bash
# Run a project script in WSL with the shared Spark venv:  bash scripts/wsl_run.sh scripts/run_bronze_ais.py
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD/src"
exec ~/venvs/mct/bin/python "$@"

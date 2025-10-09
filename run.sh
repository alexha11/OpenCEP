#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
DEFAULT_DATA_FILE="$SCRIPT_DIR/data/2014-citibike-tripdata/1_January/201401-citibike-tripdata_1.csv"
DEFAULT_TARGET_STATIONS=(79 83 100)
DEFAULT_MODE="load-shedding"
DEFAULT_MAX_EVENTS=25

# If no args, announce and proceed with defaults
if [ $# -eq 0 ]; then
  echo "No arguments provided; running with defaults:"
  echo "  --mode $DEFAULT_MODE --max-events $DEFAULT_MAX_EVENTS --data-file \"$DEFAULT_DATA_FILE\" --target-stations ${DEFAULT_TARGET_STATIONS[*]}"
fi

# Append defaults if missing
HAS_DATA=false
HAS_TARGETS=false
HAS_MODE=false
HAS_MAX_EVENTS=false
for arg in "$@"; do
  if [[ "$arg" == "--data-file" || "$arg" == "-d" ]]; then HAS_DATA=true; fi
  if [[ "$arg" == "--target-stations" ]]; then HAS_TARGETS=true; fi
  if [[ "$arg" == "--mode" ]]; then HAS_MODE=true; fi
  if [[ "$arg" == "--max-events" ]]; then HAS_MAX_EVENTS=true; fi
done

ARGS=("$@")
if ! $HAS_MODE; then
  ARGS+=("--mode" "$DEFAULT_MODE")
fi
if ! $HAS_MAX_EVENTS; then
  ARGS+=("--max-events" "$DEFAULT_MAX_EVENTS")
fi
if ! $HAS_DATA; then
  ARGS+=("--data-file" "$DEFAULT_DATA_FILE")
fi
if ! $HAS_TARGETS; then
  ARGS+=("--target-stations" "${DEFAULT_TARGET_STATIONS[@]}")
fi

python3 "$SCRIPT_DIR/main.py" "${ARGS[@]}"

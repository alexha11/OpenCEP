#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Defaults
DEFAULT_DATA_FILE="$SCRIPT_DIR/data/2014-citibike-tripdata/1_January/201401-citibike-tripdata_1.csv"
DEFAULT_MODE="load-shedding"
DEFAULT_MAX_EVENTS=20

# If no args, announce and proceed with defaults
if [ $# -eq 0 ]; then
  echo "No arguments provided; running with defaults:"
  echo "  --mode $DEFAULT_MODE --max-events $DEFAULT_MAX_EVENTS --data-file \"$DEFAULT_DATA_FILE\""
  echo "  Target stations will be auto-detected from first $DEFAULT_MAX_EVENTS events"
fi

# Extract max-events value for target detection
MAX_EVENTS=$DEFAULT_MAX_EVENTS
DATA_FILE=$DEFAULT_DATA_FILE

ARGS=("$@")
i=0
while [ $i -lt ${#ARGS[@]} ]; do
  if [[ "${ARGS[$i]}" == "--max-events" && $((i+1)) -lt ${#ARGS[@]} ]]; then
    MAX_EVENTS=${ARGS[$((i+1))]}
  elif [[ "${ARGS[$i]}" == "--data-file" || "${ARGS[$i]}" == "-d" ]] && [[ $((i+1)) -lt ${#ARGS[@]} ]]; then
    DATA_FILE=${ARGS[$((i+1))]}
  fi
  ((i++))
done

# Check if we need to add defaults
HAS_DATA=false
HAS_TARGETS=false
HAS_MODE=false
HAS_MAX_EVENTS=false
for arg in "${ARGS[@]}"; do
  if [[ "$arg" == "--data-file" || "$arg" == "-d" ]]; then HAS_DATA=true; fi
  if [[ "$arg" == "--target-stations" ]]; then HAS_TARGETS=true; fi
  if [[ "$arg" == "--mode" ]]; then HAS_MODE=true; fi
  if [[ "$arg" == "--max-events" ]]; then HAS_MAX_EVENTS=true; fi
done

if ! $HAS_MODE; then
  ARGS+=("--mode" "$DEFAULT_MODE")
fi
if ! $HAS_MAX_EVENTS; then
  ARGS+=("--max-events" "$DEFAULT_MAX_EVENTS")
fi
if ! $HAS_DATA; then
  ARGS+=("--data-file" "$DEFAULT_DATA_FILE")
fi

# Auto-detect target stations if not provided
if ! $HAS_TARGETS; then
  if [ ! -f "$DATA_FILE" ]; then
    echo "Error: Data file not found: $DATA_FILE" >&2
    exit 1
  fi
  
  echo "Auto-detecting target stations from first $MAX_EVENTS events..." >&2
  AUTO_TARGETS=$(python3 "$SCRIPT_DIR/scripts/find_targets.py" "$DATA_FILE" --max-events "$MAX_EVENTS" --top 3 --mode balanced)
  
  if [ $? -ne 0 ] || [ -z "$AUTO_TARGETS" ]; then
    echo "Warning: Could not auto-detect targets, using fallback stations" >&2
    AUTO_TARGETS="526 270 519"
  fi
  
  echo "Selected target stations: $AUTO_TARGETS" >&2
  ARGS+=("--target-stations" $AUTO_TARGETS)
fi

python3 "$SCRIPT_DIR/main.py" "${ARGS[@]}"

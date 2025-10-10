#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: bash find_targets.sh PATH/TO/citibike.csv [TOP_N]"
  exit 1
fi

CSV_FILE="$1"
TOP_N="${2:-3}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/scripts/find_targets.py" "$CSV_FILE" --top "$TOP_N"

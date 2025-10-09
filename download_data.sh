#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dest_dir="${script_dir}/data"
mkdir -p "$dest_dir"
cd "$dest_dir"
rm -rf ./*

FILE="2014-citibike-tripdata.zip"
URL="https://s3.amazonaws.com/tripdata/${FILE}"

echo "Downloading $FILE ... at $URL"
curl -fL -o "$FILE" "$URL"
echo "Unzipping $FILE ..."
unzip -o "$FILE"

rm -f ./*.zip
rm -rf __MACOSX


#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

python3 - "$PGRUST_DIR" <<'PY'
import json
import subprocess
import sys

repo = sys.argv[1]
metadata = subprocess.check_output(
    ["cargo", "metadata", "--no-deps", "--format-version", "1"],
    cwd=repo,
    text=True,
)
print(json.loads(metadata)["target_directory"])
PY

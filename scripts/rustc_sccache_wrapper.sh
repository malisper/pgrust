#!/usr/bin/env bash
set -euo pipefail

if command -v sccache >/dev/null 2>&1; then
  # sccache starts a local server and may need filesystem features that ExFAT
  # external-volume TMPDIRs do not provide on macOS.
  if [[ "$(uname -s)" == "Darwin" && "${TMPDIR:-}" == /Volumes/* ]]; then
    export TMPDIR=/tmp
  fi

  exec sccache "$@"
fi

exec "$@"

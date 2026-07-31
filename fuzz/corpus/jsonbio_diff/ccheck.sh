#!/bin/bash
cd /path/to/pgrust/.wt-p1-laneaa-exec/fuzz/core/csrc/jsonpath
for f in "$@"; do
  cc -c -o /dev/null -Iinclude -I. -fno-strict-aliasing -fwrapv "$f" 2>&1 | head -40
done

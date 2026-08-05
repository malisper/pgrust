#!/bin/bash
# cov-export.sh <target> — replay the target's corpus under instrumentation
# (cargo fuzz coverage) and export an lcov to coverage/<target>.lcov using
# the nightly sysroot llvm-cov (the rf-capture recipe, proofs/coverage-rf).
set -eu
cd "$(dirname "$0")"
T="$1"
NIGHTLY=$(rustup toolchain list | grep '^nightly-2' | tail -1 | cut -d' ' -f1)
cargo +"$NIGHTLY" fuzz coverage "$T" "corpus/$T"
SYSROOT=$(rustc +"$NIGHTLY" --print sysroot)
LLVM_COV=$(find "$SYSROOT" -name llvm-cov | head -1)
TRIPLE=$(rustc +"$NIGHTLY" -vV | grep host | cut -d' ' -f2)
BIN="target/$TRIPLE/coverage/$TRIPLE/release/$T"
"$LLVM_COV" export -format=lcov -instr-profile "coverage/$T/coverage.profdata" "$BIN" > "coverage/$T.lcov"
echo "wrote coverage/$T.lcov ($(grep -c '^DA:' "coverage/$T.lcov") DA lines)"

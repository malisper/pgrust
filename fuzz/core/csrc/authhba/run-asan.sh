#!/usr/bin/env bash
# run-asan.sh: build + run the standalone libFuzzer + ASan/UBSan campaign over
# the verbatim PostgreSQL 18.3 pg_hba.conf tokenizer (authfuzz ST1, C-oracle
# half). See pg_hba_token_io.c header for provenance.
#
# This is a SELF-CONTAINED C target (no Rust link), so it sidesteps the
# Apple-clang-vs-rustc ASan runtime version mismatch that the decoder_fuzz
# mixed link hits (fuzz/core/build.rs header): Apple clang ships a matching
# libFuzzer + ASan runtime. Verified building under Apple clang 17.0.0.
#
# Usage:
#   ./run-asan.sh [max_total_time_seconds]     # default 60s smoke
#   PGRUST_AUTHFUZZ_RUNS=20000000 ./run-asan.sh   # exec-bounded campaign
#
# CI: this is the bounded 5-20M-exec ASan job referenced by the charter.
# Any crash artifact -> minimize (llvm-fuzzer -minimize_crash), then write
# docs/upstream/bug-NNN-*.txt + a docs/conformance/upstream-c-findings.md row
# (IN-REPO ONLY; Michael sends every upstream report).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$HERE/hba_token_fuzz"
CORPUS="$HERE/corpus"
CC="${CC:-clang}"
MAXT="${1:-60}"

mkdir -p "$CORPUS"

# Prefer a real libFuzzer build (coverage-guided) when the runtime archive is
# present (Linux/CI cluster, Homebrew LLVM). Apple clang CLT ships the fuzzer
# instrumentation but NOT libclang_rt.fuzzer_osx.a, so fall back to the
# standalone ASan/UBSan mutation driver (self-contained main(), still a real
# sanitizer campaign with an exec count).
echo "authfuzz: probing libFuzzer runtime..."
if "$CC" -g -O1 -fno-omit-frame-pointer -DAUTHFUZZ_LIBFUZZER \
		-fsanitize=fuzzer,address,undefined -fsanitize-address-use-after-scope \
		-o "$BIN" "$HERE/pg_hba_token_io.c" 2>/dev/null; then
	echo "authfuzz: libFuzzer OK — coverage-guided campaign"
	ARGS=(-print_final_stats=1 -rss_limit_mb=4096 -max_len=8192)
	if [ -n "${PGRUST_AUTHFUZZ_RUNS:-}" ]; then
		ARGS+=(-runs="$PGRUST_AUTHFUZZ_RUNS")
	else
		ARGS+=(-max_total_time="$MAXT")
	fi
	echo "authfuzz: running campaign ${ARGS[*]}"
	ASAN_OPTIONS=abort_on_error=1:detect_leaks=0 \
	UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
		"$BIN" "${ARGS[@]}" "$CORPUS"
else
	echo "authfuzz: libFuzzer runtime unavailable — standalone ASan/UBSan driver"
	"$CC" -g -O1 -fno-omit-frame-pointer \
		-fsanitize=address,undefined -fsanitize-address-use-after-scope \
		-o "$BIN" "$HERE/pg_hba_token_io.c"
	echo "authfuzz: running ${PGRUST_AUTHFUZZ_RUNS:-20000000} execs"
	ASAN_OPTIONS=abort_on_error=1:detect_leaks=0 \
	UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
		"$BIN" "${PGRUST_AUTHFUZZ_RUNS:-}"
fi
